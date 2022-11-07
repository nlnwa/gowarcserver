package tikvidx

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/nlnwa/gowarcserver/index"
	"github.com/nlnwa/gowarcserver/timestamp"
	"github.com/tikv/client-go/v2/txnkv"
)

func CompareClosest(ts int64) func(int64, int64) bool {
	return func(ts1 int64, ts2 int64) bool {
		return timestamp.AbsInt64(ts-ts1) < timestamp.AbsInt64(ts-ts2)
	}
}

func CompareAsc(a int64, b int64) bool {
	return a <= b
}

func CompareDesc(a int64, b int64) bool {
	return a > b
}

// iterator mimics tikv's internal iterator interface
type iterator interface {
	Next() error
	Key() []byte
	Value() []byte
	Valid() bool
	Close()
}

type iterClosest struct {
	forward  iterator
	backward iterator
	curr     iterator
	prefix   []byte
	key      []byte
	value    []byte
	valid    bool
	cmp      func(int64, int64) bool
}

func NewIterClosest(_ context.Context, snapshot *txnkv.KVSnapshot, key string, closest string) (*iterClosest, error) {
	ic := new(iterClosest)
	if t, err := time.Parse(timestamp.CDX, closest); err != nil {
		return nil, err
	} else {
		ic.cmp = CompareClosest(t.Unix())
	}

	ic.prefix = []byte(cdxPrefix + key)
	k := []byte(cdxPrefix + key + " " + closest)

	// initialize forward iterator
	forward, err := snapshot.Iter(k, []byte(cdxEOF))
	if err != nil {
		return nil, err
	}
	ic.forward = forward

	// initialize backward iterator
	backward, err := snapshot.IterReverse(k)
	if err != nil {
		return nil, err
	}
	ic.backward = backward

	return ic, ic.next()
}

func (ic *iterClosest) Next() error {
	err := ic.curr.Next()
	if err != nil {
		return err
	}
	return ic.next()
}

func (ic *iterClosest) next() error {
	var ft int64
	var bt int64

	// get forward ts
	if ic.forward.Valid() && bytes.HasPrefix(ic.forward.Key(), ic.prefix) {
		ts, _ := time.Parse(timestamp.CDX, cdxKey(ic.forward.Key()).ts())
		ft = ts.Unix()
	}
	// get backward ts
	if ic.backward.Valid() && bytes.HasPrefix(ic.backward.Key(), ic.prefix) {
		ts, _ := time.Parse(timestamp.CDX, cdxKey(ic.backward.Key()).ts())
		bt = ts.Unix()
	}

	var it iterator
	if ft != 0 && bt != 0 {
		// find closest of forward and backward
		isForward := ic.cmp(ft, bt)
		if isForward {
			it = ic.forward
		} else {
			it = ic.backward
		}
	} else if ft != 0 {
		it = ic.forward
	} else if bt != 0 {
		it = ic.backward
	} else {
		ic.valid = false
		return nil
	}

	ic.curr = it
	ic.key = it.Key()
	ic.value = it.Value()
	ic.valid = true

	return nil
}

func (ic *iterClosest) Key() []byte {
	return ic.key
}

func (ic *iterClosest) Value() []byte {
	return ic.value
}

func (ic *iterClosest) Valid() bool {
	return ic.valid
}

func (ic *iterClosest) Close() {
	ic.forward.Close()
	ic.backward.Close()
}

type maybeKV struct {
	kv    KV
	error error
}

type iterSort struct {
	iterators []iterator
	key       []byte
	value     []byte
	valid     bool
	next      <-chan maybeKV
}

func newIter(ctx context.Context, tx *txnkv.KVSnapshot, req index.SearchRequest) (iterator, error) {
	is := new(iterSort)
	var prefixes [][]byte
	var results []chan *maybeKV
	var it iterator
	var err error
	for _, key := range req.Keys() {
		k := []byte(cdxPrefix + key)
		switch req.Sort() {
		case index.SortAsc:
			it, err = tx.Iter(k, []byte(cdxEOF))
		case index.SortClosest:
			it, err = NewIterClosest(ctx, tx, key, req.Closest())
		case index.SortDesc:
			if len(req.Keys()) == 1 {
				return tx.IterReverse(k)
			}
			it, err = tx.Iter(k, []byte(cdxEOF))
		case index.SortNone:
			fallthrough
		default:
			it, err = tx.Iter(k, []byte(cdxEOF))
		}
		if err != nil {
			break
		}
		if !it.Valid() {
			continue
		}
		is.iterators = append(is.iterators, it)
		prefixes = append(prefixes, k)
		results = append(results, make(chan *maybeKV))
	}
	if err != nil {
		is.Close()
		return nil, err
	}
	if len(is.iterators) == 0 {
		return nil, nil
	}
	if len(is.iterators) == 1 {
		return is.iterators[0], nil
	}
	// initialize comparator
	var cmp func(a KV, b KV) bool

	switch req.Sort() {
	case index.SortDesc:
		cmp = func(a KV, b KV) bool {
			return CompareDesc(a.ts(), b.ts())
		}
	case index.SortClosest:
		var t time.Time
		t, err = time.Parse(timestamp.CDX, req.Closest())
		if err != nil {
			return nil, fmt.Errorf("failed to parse closest timestamp: %w", err)
		}
		cmp = func(a KV, b KV) bool {
			return CompareClosest(t.Unix())(a.ts(), b.ts())
		}
	case index.SortAsc:
		fallthrough
	case index.SortNone:
		fallthrough
	default:
		cmp = func(a KV, b KV) bool {
			return CompareAsc(a.ts(), b.ts())
		}
	}

	is.next = mergeIter(ctx.Done(), cmp, results...)

	for i, iter := range is.iterators {
		i := i
		go func(iter iterator, prefix []byte, ch chan<- *maybeKV) {
			defer close(ch)
			for iter.Valid() && bytes.HasPrefix(iter.Key(), prefix) {
				select {
				case <-ctx.Done():
					return
				case ch <- &maybeKV{kv: KV{K: iter.Key(), V: iter.Value()}}:
				}
				err := iter.Next()
				if err != nil {
					select {
					case <-ctx.Done():
					case ch <- &maybeKV{error: err}:
					}
					return
				}
			}
		}(iter, prefixes[i], results[i])
	}

	return is, is.Next()
}

// Next updates the next key, value and validity.
func (is *iterSort) Next() error {
	mkv, ok := <-is.next
	if !ok {
		is.valid = false
		return nil
	}
	is.valid = true

	if mkv.error != nil {
		return mkv.error
	}
	is.key = mkv.kv.K
	is.value = mkv.kv.V

	return nil
}

func (is *iterSort) Key() []byte {
	return is.key
}

func (is *iterSort) Value() []byte {
	return is.value
}

func (is *iterSort) Valid() bool {
	return is.valid
}

func (is *iterSort) Close() {
	for _, it := range is.iterators {
		if it != nil {
			it.Close()
		}
	}
}

// mergeIter merges sorted input channels into a sorted output channel
//
// Sorting is done by comparing keys from key-value pairs.
//
// The input channels are closed externally
func mergeIter(done <-chan struct{}, cmp func(KV, KV) bool, in ...chan *maybeKV) <-chan maybeKV {
	out := make(chan maybeKV)
	cords := make([]*maybeKV, len(in))
	go func() {
		defer close(out)
		var zombie []int
		for {
			curr := -1
			for i, cord := range cords {
				if cord == nil {
					select {
					case cord = <-in[i]:
						cords[i] = cord
					case <-done:
						return
					}
					// closed channel becomes zombie
					if cord == nil {
						zombie = append(zombie, i)
						continue
					}
				}
				if cord.error != nil {
					// prioritize errors
					curr = i
					break
				}
				if curr == -1 {
					curr = i
				} else if cmp(cords[i].kv, cord.kv) {
					curr = i
				}
			}
			if curr == -1 {
				return
			}
			select {
			case <-done:
				return
			case out <- *cords[curr]:
				cords[curr] = nil
			}
			// if zombie, then kill
			if len(zombie) > 0 {
				for _, i := range zombie {
					cords[i] = cords[len(cords)-1]
					cords = cords[:len(cords)-1]
					in[i] = in[len(in)-1]
					in = in[:len(in)-1]
				}
				zombie = nil
			}
		}
	}()

	return out
}

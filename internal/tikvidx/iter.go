package tikvidx

import (
	"bytes"
	"context"
	"time"

	"github.com/nlnwa/gowarcserver/index"
	"github.com/nlnwa/gowarcserver/timestamp"
	"github.com/tikv/client-go/v2/txnkv/transaction"
)

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
	prefix   []byte
	key      []byte
	value    []byte
	valid    bool
	cmp      func(int64, int64) bool
}

func NewIterClosest(_ context.Context, tx *transaction.KVTxn, key string, closest string) (*iterClosest, error) {
	ic := new(iterClosest)
	if t, err := time.Parse(timestamp.CDX, closest); err != nil {
		return nil, err
	} else {
		ic.cmp = CompareClosest(t.Unix())
	}

	ic.prefix = []byte(cdxPrefix + key)
	k := []byte(cdxPrefix + key + " " + closest)

	// initialize forward iterator
	forward, err := tx.Iter(k, []byte(cdxEOF))
	if err != nil {
		return nil, err
	}
	ic.forward = forward

	// initialize backward iterator
	backward, err := tx.IterReverse(k)
	if err != nil {
		return nil, err
	}
	ic.backward = backward

	return ic, ic.Next()
}

func (ic *iterClosest) Next() error {
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
	ic.key = it.Key()
	ic.value = it.Value()

	return it.Next()
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
	keys      [][]byte
	key       []byte
	value     []byte
	valid     bool
	cmp       func([]byte, []byte) bool
	next      <-chan maybeKV
}

func newIter(ctx context.Context, tx *transaction.KVTxn, req index.SearchRequest) (*iterSort, error) {
	is := new(iterSort)

	// initialize iterators
	var results []chan *maybeKV
	var err error
	for _, key := range req.Keys() {
		k := []byte(cdxPrefix + key)
		var it iterator
		switch req.Sort() {
		case index.SortDesc:
			it, err = tx.IterReverse(k)
		case index.SortClosest:
			it, err = NewIterClosest(ctx, tx, key, req.Closest())
		case index.SortAsc:
			fallthrough
		default:
			it, err = tx.Iter(k, []byte(cdxEOF))
		}
		if err != nil {
			break
		}
		is.iterators = append(is.iterators, it)
		results = append(results, make(chan *maybeKV))
	}
	if err != nil {
		defer is.Close()
		return nil, err
	}
	if len(results) == 0 {
		return nil, nil
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
		cmp = func(a KV, b KV) bool {
			return CompareClosest(t.Unix())(a.ts(), b.ts())
		}
	case index.SortAsc:
		fallthrough
	default:
		cmp = func(a KV, b KV) bool {
			return CompareAsc(a.ts(), b.ts())
		}
	}

	is.next = mergeIter(ctx.Done(), cmp, results...)

	for i, iter := range is.iterators {
		i := i
		go func(iter iterator, ch chan<- *maybeKV) {
			defer close(ch)
			for iter.Valid() && bytes.HasPrefix(iter.Key(), []byte(cdxPrefix)) {
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
		}(iter, results[i])
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

/*
 * Copyright 2022 National Library of Norway.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package tikvidx

import (
	"context"
	"time"

	"github.com/nlnwa/gowarcserver/server/api"
	"github.com/tikv/client-go/v2/rawkv"

	"github.com/nlnwa/gowarcserver/index"
	"github.com/nlnwa/gowarcserver/timestamp"
)

type comparator func(KV, KV) bool

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

type closestScanner struct {
	fKeys, bKeys     [][]byte
	fValues, bValues [][]byte
	fIndex, bIndex   int
	cmp              func(int64, int64) bool
}

const startDate = "19700101000000"
const endbyte = "\xff"

func scanClosest(ctx context.Context, client *rawkv.Client, key string, closest string, options ...rawkv.RawOption) ([][]byte, [][]byte, error) {
	ic := new(closestScanner)
	if t, err := time.Parse(timestamp.CDX, closest); err != nil {
		return nil, nil, err
	} else {
		ic.cmp = CompareClosest(t.Unix())
	}

	startKey := []byte(cdxPrefix + key + " " + closest)
	limit := 10
	var err error

	ic.fKeys, ic.fValues, err = client.Scan(ctx, startKey, []byte(cdxPrefix+key+endbyte), limit, options...)
	if err != nil {
		return nil, nil, err
	}

	// scan backward
	ic.bKeys, ic.bValues, err = client.ReverseScan(ctx, startKey, []byte(cdxPrefix+key+" "+startDate), limit, options...)
	if err != nil {
		return nil, nil, err
	}

	var keys [][]byte
	var values [][]byte
	for {
		k, v, valid := ic.next()
		if !valid {
			return keys, values, nil
		}
		keys = append(keys, k)
		values = append(values, v)
	}
}

func (cs *closestScanner) next() ([]byte, []byte, bool) {
	var ft int64
	var bt int64

	// get forward ts
	if len(cs.fKeys) > cs.fIndex {
		fts, _ := time.Parse(timestamp.CDX, cdxKey(cs.fKeys[cs.fIndex]).ts())
		ft = fts.Unix()
	}

	// get backward ts
	if len(cs.bKeys) > cs.bIndex {
		bts, _ := time.Parse(timestamp.CDX, cdxKey(cs.bKeys[cs.bIndex]).ts())
		bt = bts.Unix()
	}

	var itKeys [][]byte
	var itValues [][]byte
	var i *int
	if ft != 0 && bt != 0 {
		// find closest of forward and backward
		isForward := cs.cmp(ft, bt)
		if isForward {
			itKeys = cs.fKeys
			i = &cs.fIndex
		} else {
			itKeys = cs.bKeys
			i = &cs.bIndex
		}
	} else if ft != 0 {
		itKeys = cs.fKeys
		i = &cs.fIndex
	} else if bt != 0 {
		itKeys = cs.bKeys
		i = &cs.bIndex
	} else {
		return nil, nil, false
	}
	key := itKeys[*i]
	value := itValues[*i]
	*i++

	return key, value, true
}

type maybeKV struct {
	kv    KV
	error error
}

func getComparator(req index.SearchRequest) (comparator, error) {
	switch req.Sort() {
	case index.SortDesc:
		return func(a KV, b KV) bool {
			return CompareDesc(a.ts(), b.ts())
		}, nil
	case index.SortAsc:
		fallthrough
	case index.SortNone:
		fallthrough
	case index.SortClosest:
		fallthrough
	default:
		return func(a KV, b KV) bool {
			return CompareAsc(a.ts(), b.ts())
		}, nil
	}
}

type scan func(context.Context, []byte, []byte, int, ...rawkv.RawOption) ([][]byte, [][]byte, error)

type iter struct {
	key   []byte
	value []byte
	valid bool
	next  <-chan maybeKV
}

func newIter(ctx context.Context, client *rawkv.Client, req index.SearchRequest) (iterator, error) {
	cmp, err := getComparator(req)
	if err != nil {
		return nil, err
	}

	getScanner := func(sort index.Sort) scan {
		switch sort {
		case index.SortDesc:
			if len(req.Keys()) == 1 {
				return client.ReverseScan
			} else {
				return client.Scan
			}
		case index.SortAsc:
			fallthrough
		case index.SortNone:
			fallthrough
		case index.SortClosest:
			fallthrough
		default:
			return client.Scan
		}
	}

	makeStartKey := func(endKey []byte) []byte {
		return append([]byte(api.MatchType(string(endKey), req.MatchType())), 0)
	}

	makeEndkey := func(startKey []byte) []byte {
		endKey := make([]byte, len(startKey)+1)
		copy(endKey, startKey)
		endKey[len(endKey)-1] = 0xff
		return endKey
	}

	var results []chan *maybeKV
	for i, key := range req.Keys() {
		results = append(results, make(chan *maybeKV))

		go func(scan scan, key []byte, ch chan<- *maybeKV, done <-chan struct{}) {
			defer close(ch)
			scanLimit := 256
			startKey := key
			endKey := makeEndkey(startKey)
			for {
				keys, values, err := scan(ctx, startKey, endKey, scanLimit)
				if err != nil {
					select {
					case <-done:
						return
					case ch <- &maybeKV{error: err}:
						return
					}
				}
				for j, k := range keys {
					select {
					case <-done:
						return
					case ch <- &maybeKV{kv: KV{K: k, V: values[j]}}:
					}
				}
				if len(keys) < scanLimit {
					return
				}
				startKey = makeStartKey(keys[len(keys)-1])
			}
		}(getScanner(req.Sort()), []byte(cdxPrefix+key), results[i], ctx.Done())
	}

	is := new(iter)
	is.next = mergeIter(ctx.Done(), cmp, results...)

	return is, is.Next()
}

// Next updates the next key, value and validity.
func (is *iter) Next() error {
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

func (is *iter) Key() []byte {
	return is.key
}

func (is *iter) Value() []byte {
	return is.value
}

func (is *iter) Valid() bool {
	return is.valid
}

func (is *iter) Close() {
	// noop
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

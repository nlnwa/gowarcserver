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

	"github.com/tikv/client-go/v2/rawkv"

	"github.com/nlnwa/gowarcserver/index"
	"github.com/nlnwa/gowarcserver/internal/keyvalue"
	"github.com/nlnwa/gowarcserver/timestamp"
)

// iterator mimics tikv's internal iterator interface
type iterator interface {
	Next() error
	Key() []byte
	Value() []byte
	Valid() bool
	Close()
}

type closestScanner struct {
	fKeys, fValues [][]byte
	bKeys, bValues [][]byte
	fIndex, bIndex int
	cmp            func(int64, int64) bool
}

const startDate = "19700101000000"
const endbyte = "\xff"

func scanClosest(client *rawkv.Client, ctx context.Context, key string, closest string, limit int, options ...rawkv.RawOption) ([][]byte, [][]byte, error) {
	ic := new(closestScanner)
	if t, err := timestamp.Parse(closest); err != nil {
		return nil, nil, err
	} else {
		ic.cmp = timestamp.CompareClosest(t.Unix())
	}
	startKey := []byte(cdxPrefix + key + closest)

	if limit == 0 || limit > rawkv.MaxRawKVScanLimit {
		limit = rawkv.MaxRawKVScanLimit
	}

	var err error

	ic.fKeys, ic.fValues, err = client.Scan(ctx, startKey, []byte(cdxPrefix+key+endbyte), limit, options...)
	if err != nil {
		return nil, nil, err
	}

	// scan backward
	ic.bKeys, ic.bValues, err = client.ReverseScan(ctx, startKey, []byte(cdxPrefix+key+startDate), limit, options...)
	if err != nil {
		return nil, nil, err
	}

	var keys [][]byte
	var values [][]byte
	for count := 0; count < limit; count++ {
		k, v, valid := ic.next()
		if !valid {
			break
		}
		keys = append(keys, k)
		values = append(values, v)
	}
	return keys, values, nil
}

func (cs *closestScanner) next() ([]byte, []byte, bool) {
	var ft int64
	var bt int64

	// get forward ts
	if len(cs.fKeys) > cs.fIndex {
		// We trust ts from DB, so no need to check error
		ft = keyvalue.CdxKeyTs(cs.fKeys[cs.fIndex]).Unix()
	}
	// get backward ts
	if len(cs.bKeys) > cs.bIndex {
		// We trust ts from DB, so no need to check error
		bt = keyvalue.CdxKeyTs(cs.bKeys[cs.bIndex]).Unix()
	}

	var isForward bool
	if ft != 0 && bt != 0 {
		// find closest of forward and backward
		isForward = cs.cmp(ft, bt)
	} else if ft != 0 {
		isForward = true
	} else if bt != 0 {
		isForward = false
	} else {
		return nil, nil, false
	}
	if isForward {
		defer func() { cs.fIndex++ }()
		return cs.fKeys[cs.fIndex], cs.fValues[cs.fIndex], true
	} else {
		defer func() { cs.bIndex++ }()
		return cs.bKeys[cs.bIndex], cs.bValues[cs.bIndex], true
	}
}

type maybeKV struct {
	key   []byte
	value []byte
	error error
}

type scanner func([]byte) ([][]byte, [][]byte, error)

type iter struct {
	key   []byte
	value []byte
	valid bool
	next  <-chan maybeKV
}

func newIter(ctx context.Context, client *rawkv.Client, req index.Request) (iterator, error) {
	limit := req.Limit()
	if limit == 0 || limit > rawkv.MaxRawKVScanLimit {
		limit = rawkv.MaxRawKVScanLimit
	}

	scan := func() scanner {
		switch req.Sort() {
		case index.SortDesc:
			return func(key []byte) ([][]byte, [][]byte, error) {
				return client.ReverseScan(ctx, append(key, 0xff), key, limit)
			}
		case index.SortClosest:
			return func(key []byte) ([][]byte, [][]byte, error) {
				return scanClosest(client, ctx, req.Key(), req.Closest(), limit)
			}
		case index.SortAsc:
			fallthrough
		case index.SortNone:
			fallthrough
		default:
			return func(key []byte) ([][]byte, [][]byte, error) {
				return client.Scan(ctx, key, append(key, 0xff), limit)
			}
		}
	}()

	result := make(chan maybeKV)

	k := req.Key()
	key := []byte(cdxPrefix + k)

	go func(scan scanner, key []byte, ch chan<- maybeKV, done <-chan struct{}) {
		defer close(ch)
		keys, values, err := scan(key)
		if err != nil {
			select {
			case <-done:
				return
			case ch <- maybeKV{error: err}:
				return
			}
		}
		for j, k := range keys {
			select {
			case <-done:
				return
			case ch <- maybeKV{key: k, value: values[j]}:
			}
		}
	}(scan, key, result, ctx.Done())

	is := &iter{
		next: result,
	}

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
	is.key = mkv.key
	is.value = mkv.value

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

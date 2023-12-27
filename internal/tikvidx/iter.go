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

type maybeKV struct {
	k     []byte
	v     []byte
	error error
}

type scanner func([]byte, []byte) ([][]byte, [][]byte, error)

func repeatScan(scan scanner, key, endKey []byte, result chan<- maybeKV, done <-chan struct{}) {
	defer close(result)
	for {
		keys, values, err := scan(key, endKey)
		if err != nil {
			select {
			case <-done:
				return
			case result <- maybeKV{error: err}:
				return
			}
		}
		for j, k := range keys {
			select {
			case <-done:
				return
			case result <- maybeKV{k: k, v: values[j]}:
			}
		}
		select {
		case <-done:
			return
		default:
		}
		if len(keys) < rawkv.MaxRawKVScanLimit {
			return
		}
		key = append(keys[len(keys)-1], 0x0)
	}
}

var startDate = []byte("19700101000000")
var endDate = []byte("99991231235959")

type closestIter struct {
	forward, backward chan maybeKV
	a, b              *maybeKV
	kv                *maybeKV
	valid             bool
	cmp               func(int64, int64) bool
	done              chan struct{}
	limit             int
}

func newClosestIter(ctx context.Context, client *rawkv.Client, req index.Request) (iterator, error) {
	t, err := timestamp.Parse(req.Closest())
	if err != nil {
		return nil, err
	}

	prefix, startKey := keyvalue.ClosestWithPrefix(req, cdxPrefix)
	forwardEndKey := append(prefix, endDate...)
	backwardEndKey := append(prefix, startDate...)

	limit := req.Limit()
	if limit == 0 || limit > rawkv.MaxRawKVScanLimit {
		limit = rawkv.MaxRawKVScanLimit
	}

	var fScanner scanner = func(key, endKey []byte) ([][]byte, [][]byte, error) {
		return client.Scan(ctx, key, endKey, limit)
	}
	var bScanner scanner = func(key, endKey []byte) ([][]byte, [][]byte, error) {
		return client.ReverseScan(ctx, key, endKey, limit)
	}

	forwardChannel := make(chan maybeKV)
	backwardChannel := make(chan maybeKV)

	done := make(chan struct{})
	go repeatScan(fScanner, startKey, forwardEndKey, forwardChannel, done)
	go repeatScan(bScanner, startKey, backwardEndKey, backwardChannel, done)

	iter := &closestIter{
		cmp:      timestamp.CompareClosest(t.Unix()),
		forward:  forwardChannel,
		backward: backwardChannel,
		done:     done,
		limit:    req.Limit(),
	}

	return iter, iter.Next()
}

func (ci *closestIter) Key() []byte {
	return ci.kv.k
}

func (ci *closestIter) Value() []byte {
	return ci.kv.v
}

func (ci *closestIter) Valid() bool {
	return ci.valid
}

func (ci *closestIter) Close() {
	close(ci.done)
}

func (ci *closestIter) Next() error {
	var ft int64
	var bt int64
	ci.valid = true

	if ci.a == nil {
		f, ok := <-ci.forward
		if ok {
			ci.a = &f
		}
	}
	if ci.b == nil {
		b, ok := <-ci.backward
		if ok {
			ci.b = &b
		}
	}
	if ci.a != nil {
		ft = keyvalue.CdxKey(ci.a.k).Unix()
	}
	if ci.b != nil {
		bt = keyvalue.CdxKey(ci.b.k).Unix()
	}

	var isForward bool
	if ft != 0 && bt != 0 {
		// find closest of forward and backward
		if ci.cmp(ft, bt) {
			isForward = true
		}
	} else if ft != 0 {
		isForward = true
	} else if bt != 0 {
		// pass
	} else {
		ci.valid = false
		ci.kv = nil
		return nil
	}
	if isForward {
		ci.kv = ci.a
		ci.a = nil
	} else {
		ci.kv = ci.b
		ci.b = nil
	}
	return nil
}

type iter struct {
	key   []byte
	value []byte
	valid bool
	next  <-chan maybeKV
	done  chan<- struct{}
	limit int
}

func newIter(ctx context.Context, key []byte, client *rawkv.Client, req index.Request) (iterator, error) {
	limit := req.Limit()
	if limit == 0 || limit > rawkv.MaxRawKVScanLimit {
		limit = rawkv.MaxRawKVScanLimit
	}
	endKey := append(key, 0xff)
	var scan scanner
	if req.Sort() == index.SortDesc {
		scan = func(key []byte, endKey []byte) ([][]byte, [][]byte, error) {
			return client.ReverseScan(ctx, endKey, key, limit)
		}
	} else {
		scan = func(key []byte, endKey []byte) ([][]byte, [][]byte, error) {
			return client.Scan(ctx, key, endKey, limit)
		}
	}
	result := make(chan maybeKV)
	done := make(chan struct{})

	go repeatScan(scan, key, endKey, result, done)

	is := &iter{
		next:  result,
		done:  done,
		limit: req.Limit(),
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
	is.key = mkv.k
	is.value = mkv.v

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
	close(is.done)
}

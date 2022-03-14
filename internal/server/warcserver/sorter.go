/*
 * Copyright 2020 National Library of Norway.
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

package warcserver

import (
	"github.com/dgraph-io/badger/v3"
	"github.com/nlnwa/gowarcserver/internal/database"
	"github.com/nlnwa/gowarcserver/internal/timestamp"
	"sort"
)

type value struct {
	ts int64
	k  []byte
}

type sorter struct {
	closest int64
	reverse bool
	values  []value
}

func (s *sorter) add(k []byte) {
	t, _ := timestamp.Parse(Key(k).ts())
	ts := t.Unix()

	s.values = append(s.values, value{ts, k})
}

func (s *sorter) sort() {
	var cmp func(int64, int64) bool

	// sort closest, reverse or forward
	if s.closest > 0 {
		cmp = func(ts1 int64, ts2 int64) bool {
			return timestamp.AbsInt64(s.closest-ts1) < timestamp.AbsInt64(s.closest-ts2)
		}
	} else if s.reverse {
		cmp = func(ts1 int64, ts2 int64) bool {
			return ts2 < ts1
		}
	} else {
		cmp = func(ts1 int64, ts2 int64) bool {
			return ts1 < ts2
		}
	}

	sort.Slice(s.values, func(i, j int) bool {
		return cmp(s.values[i].ts, s.values[j].ts)
	})
}

func (s *sorter) walk(txn *badger.Txn, perItemFn database.PerItemFunc) error {
	for _, value := range s.values {
		item, err := txn.Get(value.k)
		if err != nil {
			return err
		}
		if perItemFn(item) {
			break
		}
	}
	return nil
}

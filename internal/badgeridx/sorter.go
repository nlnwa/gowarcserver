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

package badgeridx

import (
	"sort"

	"github.com/dgraph-io/badger/v3"
	"github.com/nlnwa/gowarcserver/timestamp"
)

type value struct {
	ts int64
	k  []byte
}

type Sorter struct {
	Closest int64
	Reverse bool
	Values  []value
}

func NewSorter(closest int64, reverse bool) Sorter {
	return Sorter{
		Closest: closest,
		Reverse: reverse,
		Values:  []value{},
	}
}

func (s *Sorter) Add(k []byte) {
	t, _ := timestamp.Parse(cdxKey(k).ts())
	ts := t.Unix()

	s.Values = append(s.Values, value{ts, k})
}

func (s *Sorter) Sort() {
	var cmp func(int64, int64) bool

	// sort Closest, Reverse or forward
	if s.Closest > 0 {
		cmp = func(ts1 int64, ts2 int64) bool {
			return timestamp.AbsInt64(s.Closest-ts1) < timestamp.AbsInt64(s.Closest-ts2)
		}
	} else if s.Reverse {
		cmp = func(ts1 int64, ts2 int64) bool {
			return ts2 < ts1
		}
	} else {
		cmp = func(ts1 int64, ts2 int64) bool {
			return ts1 < ts2
		}
	}

	sort.Slice(s.Values, func(i, j int) bool {
		return cmp(s.Values[i].ts, s.Values[j].ts)
	})
}

func (s *Sorter) Walk(txn *badger.Txn, perItemFn PerItemFunc) error {
	for _, value := range s.Values {
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

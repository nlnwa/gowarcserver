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
	"github.com/nlnwa/gowarcserver/internal/database"
	"github.com/nlnwa/gowarcserver/internal/timestamp"
	"sort"
	"strings"

	"github.com/dgraph-io/badger/v3"
)

type value struct {
	ts int64
	v  []byte
}

type sorter struct {
	closest int64
	values  []value
}

func (s *sorter) add(item *badger.Item) {
	ts, _ := timestamp.From14ToTime(strings.Split(string(item.Key()), " ")[1])
	v := value{ts.Unix(), item.KeyCopy(nil)}
	s.values = append(s.values, v)
}

func (s *sorter) sort() {
	sort.Slice(s.values, func(i, j int) bool {
		ts1 := s.values[i].ts
		ts2 := s.values[j].ts
		return timestamp.AbsInt64(s.closest-ts1) < timestamp.AbsInt64(s.closest-ts2)
	})
}

func (s *sorter) walk(txn *badger.Txn, perItemFn database.PerItemFunction) error {
	for _, value := range s.values {
		item, err := txn.Get(value.v)
		if err != nil {
			return err
		}
		if perItemFn(item) {
			break
		}
	}
	return nil
}

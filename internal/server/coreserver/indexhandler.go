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

package coreserver

import (
	"fmt"
	"net/http"
	"strconv"

	"github.com/nlnwa/gowarcserver/internal/database"

	"github.com/dgraph-io/badger/v3"
)

type IndexHandler struct {
	db *database.CdxDbIndex
}

func (h IndexHandler) ListIds(w http.ResponseWriter, r *http.Request) {
	l := r.URL.Query().Get("limit")
	count, err := strconv.Atoi(l)
	if err != nil {
		count = 100
	}

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")

	err = h.db.ListIds(func(item *badger.Item) (stopIteration bool) {
		count--
		if count < 0 {
			return true
		}
		key := item.KeyCopy(nil)
		val, _ := item.ValueCopy(nil)
		_, err := fmt.Fprintf(w, "%s %s\n", key, val)
		return err != nil
	})
	if err != nil {
		http.Error(w, fmt.Sprintf("error listing id index: %v", err), http.StatusInternalServerError)
	}
}

func (h IndexHandler) ListFileNames(w http.ResponseWriter, r *http.Request) {
	l := r.URL.Query().Get("limit")
	count, err := strconv.Atoi(l)
	if err != nil {
		count = 100
	}

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")

	err = h.db.ListFileNames(func(item *badger.Item) (stopIteration bool) {
		count--
		if count < 0 {
			return true
		}
		_, err := fmt.Fprintf(w, "%v\n", string(item.KeyCopy(nil)))
		return err != nil
	})
	if err != nil {
		http.Error(w, fmt.Sprintf("error listing file index: %v", err), http.StatusInternalServerError)
	}
}

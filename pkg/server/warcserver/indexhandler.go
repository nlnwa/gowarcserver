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
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/dgraph-io/badger/v3"
	cdx "github.com/nlnwa/gowarc/proto"
	"github.com/nlnwa/gowarcserver/pkg/index"
	"github.com/nlnwa/gowarcserver/pkg/loader"
)

type indexHandler struct {
	loader *loader.Loader
	db     *index.Db
}

func (h *indexHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var renderFunc RenderFunc = func(w http.ResponseWriter, record *cdx.Cdx, cdxApi *cdxServerApi) error {
		cdxj, err := json.Marshal(cdxjTopywbJson(record))
		if err != nil {
			return err
		}
		switch cdxApi.output {
		case "json":
			fmt.Fprintf(w, "%s\n", cdxj)
		default:
			fmt.Fprintf(w, "%s %s %s\n", record.Ssu, record.Sts, cdxj)
		}
		return nil
	}

	cdxApi, err := parseCdxServerApi(w, r, renderFunc)
	if err != nil {
		handleError(err, w)
		return
	}

	var defaultPerItemFunc index.PerItemFunction = func(item *badger.Item) (stopIteration bool) {
		k := item.Key()
		if !cdxApi.dateRange.eval(k) {
			return false
		}

		return cdxApi.writeItem(item)
	}

	var defaultAfterIterationFunc index.AfterIterationFunction = func(txn *badger.Txn) error {
		return nil
	}

	if cdxApi.sort.closest != "" {
		h.db.Search(cdxApi.key, false, cdxApi.sort.add, cdxApi.sort.write)
	} else {
		h.db.Search(cdxApi.key, cdxApi.sort.reverse, defaultPerItemFunc, defaultAfterIterationFunc)
	}

	// If no hits with http, try https
	if cdxApi.count == 0 && strings.Contains(cdxApi.key, "http:") {
		cdxApi.key = strings.ReplaceAll(cdxApi.key, "http:", "https:")

		if cdxApi.sort.closest != "" {
			h.db.Search(cdxApi.key, false, cdxApi.sort.add, cdxApi.sort.write)
		} else {
			h.db.Search(cdxApi.key, cdxApi.sort.reverse, defaultPerItemFunc, defaultAfterIterationFunc)
		}
	}
}

type pywbJson struct {
	Urlkey    string `json:"urlkey"`
	Timestamp string `json:"timestamp"`
	Url       string `json:"url"`
	Mime      string `json:"mime"`
	Status    string `json:"status"`
	Digest    string `json:"digest"`
	Length    string `json:"length"`
	Offset    string `json:"offset"`
	Filename  string `json:"filename"`
}

func cdxjTopywbJson(record *cdx.Cdx) *pywbJson {
	js := &pywbJson{
		Urlkey:    record.Ssu,
		Timestamp: record.Sts,
		Url:       strings.ReplaceAll(record.Uri, "&", "%26"),
		Mime:      record.Mct,
		Status:    record.Hsc,
		Digest:    record.Sha,
		Length:    record.Rle,
		Offset:    "",
		Filename:  "",
	}
	return js
}

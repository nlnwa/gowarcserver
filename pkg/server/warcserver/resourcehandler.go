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
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/dgraph-io/badger/v2"
	cdx "github.com/nlnwa/gowarc/proto"
	"github.com/nlnwa/gowarc/warcoptions"
	"github.com/nlnwa/gowarc/warcrecord"
	"github.com/nlnwa/gowarc/warcwriter"
	"github.com/nlnwa/gowarcserver/pkg/index"
	"github.com/nlnwa/gowarcserver/pkg/loader"
)

type resourceHandler struct {
	loader *loader.Loader
	db     *index.DB
}

func (h *resourceHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var renderFunc RenderFunc = func(w http.ResponseWriter, record *cdx.Cdx, cdxApi *cdxServerApi) error {
		warcid := record.Rid
		if len(warcid) > 0 && warcid[0] != '<' {
			warcid = "<" + warcid + ">"
		}

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		warcRecord, err := h.loader.Get(ctx, record.Rid)
		if err != nil {
			return err
		}
		defer warcRecord.Close()

		cdxj, err := json.Marshal(cdxjTopywbJson(record))
		if err != nil {
			return err
		}
		switch cdxApi.output {
		case "json":
			renderWarcContent(w, warcRecord, cdxApi, fmt.Sprintf("%s\n", cdxj))
		case "content":
			switch v := warcRecord.Block().(type) {
			case *warcrecord.RevisitBlock:
				r, err := v.Response()
				if err != nil {
					return err
				}
				renderContent(w, v, r)
			case warcrecord.HttpResponseBlock:
				r, err := v.Response()
				if err != nil {
					return err
				}
				renderContent(w, v, r)
			default:
				w.Header().Set("Content-Type", "text/plain")
				warcRecord.WarcHeader().Write(w)
				fmt.Fprintln(w)
				rb, err := v.RawBytes()
				if err != nil {
					return err
				}
				io.Copy(w, rb)
			}
		default:
			renderWarcContent(w, warcRecord, cdxApi, fmt.Sprintf("%s %s %s\n", record.Ssu, record.Sts, cdxj))
		}

		return nil
	}

	cdxApi, err := parseCdxServerApi(w, r, renderFunc)
	if err != nil {
		handleError(err, w)
		return
	}
	cdxApi.limit = 1

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

	if cdxApi.count == 0 {
		handleError(fmt.Errorf("Not found"), w)
	}
}

func renderWarcContent(w http.ResponseWriter, warcRecord warcrecord.WarcRecord, cdxApi *cdxServerApi, cdx string) {
	w.Header().Set("Warcserver-Cdx", cdx)
	w.Header().Set("Link", "<"+warcRecord.WarcHeader().Get(warcrecord.WarcTargetURI)+">; rel=\"original\"")
	w.Header().Set("WARC-Target-URI", warcRecord.WarcHeader().Get(warcrecord.WarcTargetURI))
	w.Header().Set("Warcserver-Source-Coll", cdxApi.collection)
	w.Header().Set("Content-Type", "application/warc-record")
	w.Header().Set("Memento-Datetime", warcRecord.WarcHeader().Get(warcrecord.WarcDate))
	w.Header().Set("Warcserver-Type", "warc")

	warcWriter := warcwriter.NewWriter(&warcoptions.WarcOptions{
		Strict:   false,
		Compress: false,
	})
	_, err := warcWriter.WriteRecord(w, warcRecord)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
	}
}

func renderContent(w http.ResponseWriter, v warcrecord.PayloadBlock, r *http.Response) {
	for k, vl := range r.Header {
		for _, v := range vl {
			w.Header().Set(k, v)
		}
	}
	w.WriteHeader(r.StatusCode)
	p, err := v.PayloadBytes()
	if err != nil {
		return
	}
	io.Copy(w, p)
}

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
	"github.com/dgraph-io/badger/v3"
	cdx "github.com/nlnwa/gowarc/proto"
	"github.com/nlnwa/gowarc/warcoptions"
	"github.com/nlnwa/gowarc/warcrecord"
	"github.com/nlnwa/gowarc/warcwriter"
	"github.com/nlnwa/gowarcserver/pkg/index"
	"github.com/nlnwa/gowarcserver/pkg/loader"
	"github.com/nlnwa/gowarcserver/pkg/server/handlers"
	"io"
	"net/http"
)

type ResourceHandler struct {
	loader *loader.Loader
	db     *index.DB
}

func (h *ResourceHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	cdxApi, err := parseCdxServerApi(r)
	if err != nil {
		handlers.HandleError(w, err)
		return
	}

	var renderFunc RenderFunc = func(record *cdx.Cdx, cdxApi *cdxServerApi) error {
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
			handlers.HandleError(w, err)
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
				renderContent(w, r, v)
			case warcrecord.HttpResponseBlock:
				r, err := v.Response()
				if err != nil {
					return err
				}
				renderContent(w, r, v)
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

	cdxApi.limit = 1

	defaultPerItemFn := func(item *badger.Item) (stopIteration bool) {
		k := item.Key()
		if !cdxApi.dateRange.eval(k) {
			return false
		}

		return cdxApi.writeItem(item, renderFunc)
	}
	defaultAfterIterationFn := func(txn *badger.Txn) error {
		return nil
	}

	cdxApi.sortedSearch(h.db, renderFunc, defaultPerItemFn, defaultAfterIterationFn)

	if cdxApi.count == 0 {
		handlers.HandleError(w, fmt.Errorf("Not found"))
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

func renderContent(w http.ResponseWriter, r *http.Response, v warcrecord.PayloadBlock) {
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
	_, _ = io.Copy(w, p)
}

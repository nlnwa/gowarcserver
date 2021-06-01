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
	"io/ioutil"
	"net/http"
	"net/url"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v3"
	cdx "github.com/nlnwa/gowarc/proto"
	"github.com/nlnwa/gowarcserver/pkg/index"
	"github.com/nlnwa/gowarcserver/pkg/loader"
	log "github.com/sirupsen/logrus"
)

type indexHandler struct {
	db                *index.DB
	loader            *loader.Loader
	childUrls         []url.URL
	childQueryTimeout time.Duration
}

func (h *indexHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {

	var renderFunc RenderFunc = func(w http.ResponseWriter, record *cdx.Cdx, cdxApi *cdxServerApi) error {
		cdxj, err := json.Marshal(cdxjToPywbJson(record))
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
	// TODO: structure code so that this comment is not necessary
	// value used in multiple routines, member variable 'key' should only be
	// used in the local search routine as it is mutated!
	cdxApi, err := parseCdxServerApi(w, r, renderFunc)
	if err != nil {
		handleError(err, w)
		return
	}

	// local page render + children
	workCount := 1 + len(h.childUrls)

	var waitGroup sync.WaitGroup
	waitGroup.Add(workCount)
	childQueryResponse := make(chan []byte, workCount-1)

	go func() {
		waitGroup.Wait()
		close(childQueryResponse)
	}()

	go func() {
		defer waitGroup.Done()

		defaultPerItemFn := func(item *badger.Item) (stopIteration bool) {
			k := item.Key()
			if !cdxApi.dateRange.eval(k) {
				return false
			}

			return cdxApi.writeItem(item)
		}

		defaultAfterIterFunc := func(txn *badger.Txn) error {
			return nil
		}

		search := func(api *cdxServerApi) {
			if api.sort.closest != "" {
				h.db.Search(cdxApi.key, false, cdxApi.sort.add, cdxApi.sort.write)
			} else {
				h.db.Search(cdxApi.key, cdxApi.sort.reverse, defaultPerItemFn, defaultAfterIterFunc)
			}
		}

		search(cdxApi)
		// If no hits with http, try https
		if cdxApi.count == 0 && strings.Contains(cdxApi.key, "http:") {
			cdxApi.key = strings.ReplaceAll(cdxApi.key, "http:", "https:")
			search(cdxApi)
		}
	}()

	// TODO: share this code between resourcehandler, indexhandler and searchhandler
	for _, childUrl := range h.childUrls {
		u := childUrl
		go func(u url.URL) {
			defer waitGroup.Done()

			client := http.Client{
				Timeout: h.childQueryTimeout,
			}

			u.Path = path.Join(u.Path, "warcserver", cdxApi.collection, "index")
			query := r.URL.Query()
			u.RawQuery = query.Encode()
			log.Info(u.String())
			resp, err := client.Get(u.String())
			if err != nil {
				log.Warnf("Query to %s resultet in error: %v", u, err)
				return
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				log.Warnf("Query to %s got status code %d", u, resp.StatusCode)
				return
			}

			bodyBytes, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				log.Printf("Failed to read response from child url %s: %v", u, err)
				return
			}
			childQueryResponse <- bodyBytes
		}(u)
	}

	for responseBytes := range childQueryResponse {
		w.Write(responseBytes)
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

func cdxjToPywbJson(record *cdx.Cdx) *pywbJson {
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

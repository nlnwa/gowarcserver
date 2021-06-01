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

package server

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"path"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v3"
	"github.com/golang/protobuf/jsonpb"
	"github.com/golang/protobuf/proto"
	cdx "github.com/nlnwa/gowarc/proto"
	"github.com/nlnwa/gowarcserver/pkg/index"
	"github.com/nlnwa/gowarcserver/pkg/loader"
	"github.com/nlnwa/gowarcserver/pkg/surt"
	whatwg "github.com/nlnwa/whatwg-url/errors"
	log "github.com/sirupsen/logrus"
)

type searchHandler struct {
	loader            *loader.Loader
	db                *index.DB
	childUrls         []url.URL
	childQueryTimeout time.Duration
}

var jsonMarshaler = &jsonpb.Marshaler{}

func (h *searchHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	uri := r.URL.Query().Get("url")
	childCount := len(h.childUrls)

	var waitGroup sync.WaitGroup
	waitGroup.Add(childCount)
	childQueryResponse := make(chan []byte, childCount)

	go func() {
		waitGroup.Wait()
		close(childQueryResponse)
	}()

	for _, childUrl := range h.childUrls {
		u := childUrl
		go func(u *url.URL) {
			defer waitGroup.Done()

			client := http.Client{
				Timeout: h.childQueryTimeout,
			}

			u.Path = path.Join(u.Path, "search")
			query := u.Query()
			query.Add("url", uri)
			u.RawQuery = query.Encode()
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
		}(&u)
	}

	key, err := surt.SsurtString(uri, true)
	if err != nil {
		h.handleError(err, w)
		return
	}

	results, err := h.querySelf(key)
	for _, result := range results {
		cdxj, err := jsonMarshaler.MarshalToString(result)
		if err != nil {
			log.Error(err)
			continue
		}
		fmt.Fprintf(w, "%s %s %s %s\n\n", result.Ssu, result.Sts, result.Srt, cdxj)
	}

	for responseBytes := range childQueryResponse {
		w.Write(responseBytes)
	}
}

func (h *searchHandler) querySelf(key string) ([]*cdx.Cdx, error) {
	var results []*cdx.Cdx
	perItemFn := func(item *badger.Item) bool {
		result := &cdx.Cdx{}
		err := item.Value(func(v []byte) error {
			err := proto.Unmarshal(v, result)
			if err != nil {
				return err
			}
			results = append(results, result)
			return nil
		})
		if err != nil {
			log.Errorf("perItemFn error: %s", err)
		}

		return false
	}
	afterIterFn := func(txn *badger.Txn) error {
		return nil
	}
	err := h.db.Search(key, false, perItemFn, afterIterFn)
	return results, err
}

func (h *searchHandler) handleError(err error, w http.ResponseWriter) {
	if err == nil {
		return
	}

	w.Header().Set("Content-Type", "text/plain")
	fmt.Fprint(w, err)

	// if the error is from a malformed url being parsed, then the url is invalid
	if _, ok := err.(*whatwg.UrlError); ok {
		// 422: the url is unprocessanble.
		w.WriteHeader(http.StatusUnprocessableEntity)
	} else {
		// 500: unexpected error receved
		w.WriteHeader(http.StatusInternalServerError)
	}
}

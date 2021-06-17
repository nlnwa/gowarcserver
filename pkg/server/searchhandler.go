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
	"net/http"

	"github.com/dgraph-io/badger/v3"
	cdx "github.com/nlnwa/gowarc/proto"
	"github.com/nlnwa/gowarcserver/pkg/index"
	"github.com/nlnwa/gowarcserver/pkg/loader"
	"github.com/nlnwa/gowarcserver/pkg/surt"
	whatwg "github.com/nlnwa/whatwg-url/errors"
	log "github.com/sirupsen/logrus"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

type searchHandler struct {
	loader *loader.Loader
	db     *index.DB
}

var jsonMarshaler = &protojson.MarshalOptions{}

func (h *searchHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	uri := r.URL.Query().Get("url")
	key, err := surt.SsurtString(uri, true)
	if err != nil {
		h.handleError(err, w)
		return
	}

	log.Infof("request url: %v, key: %v", uri, key)

	perItemFn := func(item *badger.Item) bool {
		result := &cdx.Cdx{}
		err := item.Value(func(v []byte) error {
			err = proto.Unmarshal(v, result)
			if err != nil {
				return err
			}

			cdxj := jsonMarshaler.Format(result)
			fmt.Fprintf(w, "%s %s %s %s\n\n", result.Ssu, result.Sts, result.Srt, cdxj)

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
	err = h.db.Search(key, false, perItemFn, afterIterFn)
	if err != nil {
		log.Warnf("Failed to search db: %v", err)
	}
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

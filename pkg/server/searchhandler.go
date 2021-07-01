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
	"github.com/nlnwa/gowarcserver/pkg/index"
	"github.com/nlnwa/gowarcserver/pkg/loader"
	"github.com/nlnwa/gowarcserver/pkg/surt"
	cdx "github.com/nlnwa/gowarcserver/proto"
	log "github.com/sirupsen/logrus"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

type searchHandler struct {
	loader   *loader.Loader
	db       *index.DB
}

var jsonMarshaler = protojson.MarshalOptions{}

func (h *searchHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	uri := r.URL.Query().Get("url")
	key, err := surt.SsurtString(uri, true)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
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
			_, err = fmt.Fprintf(w, "%s %s %s %s\n\n", result.Ssu, result.Sts, result.Srt, cdxj)
			if err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			log.Errorf("failed to render cdx record: %v", err)
		}
		return false
	}

	afterIterFn := func(txn *badger.Txn) error {
		return nil
	}

	err = h.db.Search(key, false, perItemFn, afterIterFn)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

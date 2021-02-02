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

	"github.com/dgraph-io/badger/v2"
	"github.com/golang/protobuf/jsonpb"
	"github.com/golang/protobuf/proto"
	cdx "github.com/nlnwa/gowarc/proto"
	"github.com/nlnwa/gowarcserver/pkg/index"
	"github.com/nlnwa/gowarcserver/pkg/loader"
	"github.com/nlnwa/gowarcserver/pkg/surt"
	"github.com/sirupsen/logrus"
)

type searchHandler struct {
	loader *loader.Loader
	db     *index.Db
}

var jsonMarshaler = &jsonpb.Marshaler{}

func (h *searchHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	uri := r.URL.Query().Get("url")
	key, err := surt.SsurtString(uri, true)
	if err != nil {
		h.handleError(err, w)
		return
	}

	logrus.Infof("request url: %v, key: %v", uri, key)
	h.db.Search(key, false, func(item *badger.Item) bool {
		result := &cdx.Cdx{}
		//k := item.Key()
		err := item.Value(func(v []byte) error {
			proto.Unmarshal(v, result)

			cdxj, err := jsonMarshaler.MarshalToString(result)
			if err != nil {
				return err
			}
			fmt.Fprintf(w, "%s %s %s %s\n\n", result.Ssu, result.Sts, result.Srt, cdxj)

			return nil
		})
		if err != nil {
			//return err
		}
		return false
	}, func(txn *badger.Txn) error {
		return nil
	})

}

func (h *searchHandler) handleError(err error, w http.ResponseWriter) {
	if err != nil {
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(404)
		fmt.Fprintf(w, "Error: %v\n", err)
	}
}

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
	"sync"

	"github.com/dgraph-io/badger/v3"
	"github.com/golang/protobuf/jsonpb"
	"github.com/golang/protobuf/proto"
	cdx "github.com/nlnwa/gowarc/proto"
	"github.com/nlnwa/gowarcserver/pkg/index"
	"github.com/nlnwa/gowarcserver/pkg/loader"
	"github.com/nlnwa/gowarcserver/pkg/server/localhttp"
	"github.com/nlnwa/gowarcserver/pkg/surt"
	log "github.com/sirupsen/logrus"
)

type searchHandler struct {
	loader   *loader.Loader
	db       *index.DB
	children *localhttp.Children
}

var jsonMarshaler = &jsonpb.Marshaler{}

func (h *searchHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	localhttp.AggregatedQuery(h, w, r)
}

func (h *searchHandler) ServeLocalHTTP(wg *sync.WaitGroup, r *http.Request) (*localhttp.Writer, error) {
	uri := r.URL.Query().Get("url")
	key, err := surt.SsurtString(uri, true)
	if err != nil {
		return nil, err
	}

	log.Infof("request url: %v, key: %v", uri, key)
	localWriter := localhttp.NewWriter()

	perItemFn := func(item *badger.Item) bool {
		result := &cdx.Cdx{}
		err := item.Value(func(v []byte) error {
			proto.Unmarshal(v, result)

			cdxj, err := jsonMarshaler.MarshalToString(result)
			if err != nil {
				return err
			}
			fmt.Fprintf(localWriter, "%s %s %s %s\n\n", result.Ssu, result.Sts, result.Srt, cdxj)

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
	h.db.Search(key, false, perItemFn, afterIterFn)

	return localWriter, nil
}

func (h *searchHandler) PredicateFn(r *http.Response) bool {
	return r.StatusCode >= 200 && r.StatusCode < 300
}

func (h *searchHandler) Children() *localhttp.Children {
	return h.children
}

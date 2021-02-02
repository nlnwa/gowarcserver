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
	"github.com/dgraph-io/badger/v2"
	"github.com/gorilla/mux"
	cdx "github.com/nlnwa/gowarc/proto"
	"github.com/nlnwa/whatwg-url/url"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"net/http"
	"strconv"
)

var jsonMarshaler = &protojson.MarshalOptions{}

type RenderFunc func(w http.ResponseWriter, record *cdx.Cdx, cdxApi *cdxServerApi) error

type cdxServerApi struct {
	collection string
	key        string
	matchType  string
	dateRange  *dateRange
	limit      int
	filter     *filters
	sort       *sorter
	output     string
	w          http.ResponseWriter
	count      int
	renderFunc RenderFunc
}

func parseCdxServerApi(w http.ResponseWriter, r *http.Request, renderFunc RenderFunc) (*cdxServerApi, error) {
	var err error
	c := &cdxServerApi{
		collection: mux.Vars(r)["collection"],
		w:          w,
		renderFunc: renderFunc,
		output:     r.URL.Query().Get("output"),
	}

	var sort string
	closest := r.URL.Query().Get("closest")
	if closest != "" {
		sort = "closest"
	} else {
		sort = r.URL.Query().Get("sort")
	}

	url, err := url.ParseRef("http://example.com", r.RequestURI)
	if c.key, c.matchType, err = parseKey(url.SearchParams().Get("url"), r.URL.Query().Get("matchType")); err != nil {
		return nil, err
	}

	c.dateRange = parseDateRange(r.URL.Query().Get("from"), r.URL.Query().Get("to"))
	if c.limit, err = strconv.Atoi(r.URL.Query().Get("limit")); err != nil {
		c.limit = 0
	}

	c.filter = parseFilter(r.URL.Query()["filter"])

	if c.sort, err = c.parseSort(sort, closest, c.matchType); err != nil {
		return nil, err
	}

	return c, nil
}

func (c *cdxServerApi) writeItem(item *badger.Item) (stopIteration bool) {
	result := &cdx.Cdx{}
	err := item.Value(func(v []byte) error {
		proto.Unmarshal(v, result)
		if c.filter.eval(result) {
			if err := c.renderFunc(c.w, result, c); err != nil {
				return err
			}

			c.count++
		}
		return nil
	})
	if c.limit > 0 && c.count >= c.limit {
		return true
	}
	if err != nil {
		return true
	}
	return false
}

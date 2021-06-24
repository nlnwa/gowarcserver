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
	"fmt"
	"github.com/dgraph-io/badger/v3"
	"github.com/gorilla/mux"
	"github.com/nlnwa/gowarcserver/pkg/index"
	"github.com/nlnwa/gowarcserver/pkg/timestamp"
	cdx "github.com/nlnwa/gowarcserver/proto"
	"google.golang.org/protobuf/proto"
	"net/http"
	"strconv"
	"strings"
)

type RenderFunc func(record *cdx.Cdx) error

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

const (
	SortClosest = "closest"
	SortReverse = "reverse"
)

var sorts = []string{SortClosest, SortReverse}

const (
	MatchTypeExact  = "exact"
	MatchTypePrefix = "prefix"
	MatchTypeHost   = "host"
	MatchTypeDomain = "domain"
)

var matchTypes = []string{MatchTypeDomain, MatchTypePrefix, MatchTypeHost, MatchTypeExact}

const (
	OutputCdxj = "cdxj"
	OutputJson = "json"
	OutputLink = "link"
	OutputText = "text"
)

var outputs = []string{OutputCdxj, OutputJson, OutputLink, OutputText}

type CdxjServerApi struct {
	Collection string
	Url        string
	From       string
	To         string
	MatchType  string
	Limit      int
	Sort       string
	Closest    int64
	Output     string
	Filter     []string
	Fields     []string
}

// Contains retuns true if string e is contained in string slice.
func Contains(s []string, e string) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}

func ParseCdxjApi(r *http.Request) (*CdxjServerApi, error) {
	cdxjApi := new(CdxjServerApi)
	vars := mux.Vars(r)
	query := r.URL.Query()
	cdxjApi.Collection = vars["collection"]

	url := query.Get("url")
	if url == "" {
		return nil, fmt.Errorf("missing required query parameter \"url\"")
	}
	cdxjApi.Url = url

	from := query.Get("from")
	if from != "" {
		cdxjApi.From = From(from)
	}

	to := query.Get("to")
	if to != "" {
		cdxjApi.To = To(to)
	}

	matchType := query.Get("matchType")
	if matchType != "" {
		if !Contains(matchTypes, matchType) {
			return nil, fmt.Errorf("matchType=\"%s\"; value must be one of: exact, prefix, host or domain", matchType)
		}
		cdxjApi.MatchType = matchType
	} else {
		cdxjApi.MatchType = MatchTypeExact
	}

	limit := query.Get("limit")
	if limit != "" {
		l, err := strconv.ParseUint(limit, 10, 32)
		if err != nil {
			return nil, fmt.Errorf("limit must be a positive integer, was %s", limit)
		}
		cdxjApi.Limit = int(l)
	}

	closest := query.Get("closest")
	if closest != "" {
		ts, err := timestamp.From14ToTime(closest)
		if err != nil {
			return nil, fmt.Errorf("closest failed to parse: %w", err)
		}
		cdxjApi.Closest = ts.Unix()
	}

	sort := query.Get("sort")
	if sort != "" {
		if !Contains(sorts, sort) {
			return nil, fmt.Errorf("sort must be either reverse or closest, was: %s", matchType)
		}
		cdxjApi.Sort = sort
	}

	if cdxjApi.Sort == SortClosest && cdxjApi.MatchType != MatchTypeExact && closest == "" {
		return nil, fmt.Errorf("sort=closest requires a closest parameter and matchType=exact")
	}

	output := query.Get("output")
	if output != "" {
		if !Contains(outputs, output) {
			return nil, fmt.Errorf("output must be one of cdxj, json, link or text, was: %s", output)
		}
		cdxjApi.Output = output
	} else {
		cdxjApi.Output = OutputCdxj
	}

	filter, ok := query["filter"]
	if ok {
		cdxjApi.Filter = filter
	}

	fields := query.Get("fields")
	if fields != "" {
		cdxjApi.Fields = strings.Split(fields, ",")
	}

	return cdxjApi, nil
}

type Key string

func (k Key) ts() string {
	return strings.Split(string(k), " ")[1]
}

type DbCdxServer struct {
	*index.DB
}

// search the db and render items with renderFunc
func (c DbCdxServer) search(api *CdxjServerApi, renderFunc RenderFunc) (int, error) {
	key, err := parseKey(api.Url, api.MatchType)
	if err != nil {
		return 0, err
	}

	dateRange := DateRange{
		from: api.From,
		to:   api.To,
	}

	filter := parseFilter(api.Filter)

	//url, err := whatUrl.ParseRef("http://www.example.com", r.RequestURI)
	//if err != nil {
	//	return err
	//}

	sorter := parseSort(api.Sort, api.Closest)
	count := 0

	perItemFn := func(item *badger.Item) (stopIteration bool) {
		key := Key(item.Key())

		if !dateRange.contains(key.ts()) {
			return false
		}

		result := new(cdx.Cdx)
		err := item.Value(func(v []byte) error {
			if err := proto.Unmarshal(v, result); err != nil {
				return err
			}

			if filter.eval(result) {
				if err := renderFunc(result); err != nil {
					return err
				}
				count++
			}
			return nil
		})
		if err != nil {
			return true
		}

		if api.Limit > 0 && count <= api.Limit {
			return true
		} else {
			return false
		}
	}

	afterIterFn := func(txn *badger.Txn) error {
		return nil
	}

	sortPerItemFn := func(item *badger.Item) bool {
		key := Key(item.Key())

		if !dateRange.contains(key.ts()) {
			return false
		}

		sorter.add(item)
		return false
	}

	sortAfterIterFn := func(txn *badger.Txn) error {
		sorter.sort()
		return sorter.walk(txn, perItemFn)
	}

	f := perItemFn
	a := afterIterFn

	if api.Closest != 0 {
		f = sortPerItemFn
		a = sortAfterIterFn
	}

	for {
		err := c.DB.Search(key, api.Sort == SortReverse, f, a)
		if err != nil {
			return count, err
		}

		if strings.Contains(key, "http:") {
			// no results found, try https
			key = strings.ReplaceAll(key, "http:", "https:")
		} else {
			return count, nil
		}
	}
}

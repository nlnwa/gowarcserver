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
	"net/http"
	"strconv"
	"strings"

	"github.com/dgraph-io/badger/v3"
	"github.com/gorilla/mux"
	"github.com/nlnwa/gowarcserver/pkg/index"
	"github.com/nlnwa/gowarcserver/pkg/timestamp"
	cdx "github.com/nlnwa/gowarcserver/proto"
	log "github.com/sirupsen/logrus"
	"google.golang.org/protobuf/proto"
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
		// Offset must be empty string or else pywb will try to use it's internal index.
		Offset: "",
		// Filename must be an empty string or else pywb will try to use it's internal index.
		Filename: "",
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

	// The following outputs is not implemented:
	// OutputLink = "link"
	// OutputText = "text"

	// OutputContent is not part of the CDXJ API but is used by pywb to request warc record content from it's warcserver
	// and used in a similar fashion by the ResourceHandler.
	OutputContent = "content"
)

var outputs = []string{OutputCdxj, OutputJson, OutputContent}

// CdxjServerApi implements https://pywb.readthedocs.io/en/latest/manual/cdxserver_api.html.
type CdxjServerApi struct {
	Collection string
	Url        string
	From       string
	To         string
	MatchType  string
	Limit      uint
	Sort       string
	Closest    int64
	Output     string
	Filter     []string
	Fields     []string
}

// Contains returns true if string e is contained in string slice s.
func Contains(s []string, e string) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}

// ParseCdxjApi parses a *http.Request into an *CdxjServerApi.
func ParseCdxjApi(r *http.Request) (*CdxjServerApi, error) {
	vars := mux.Vars(r)
	query := r.URL.Query()

	cdxjApi := new(CdxjServerApi)

	cdxjApi.Collection = vars["collection"]

	url := query.Get("url")
	if url == "" {
		return nil, fmt.Errorf("missing required query parameter \"url\"")
	}
	cdxjApi.Url = url

	from := query.Get("from")
	cdxjApi.From = From(from)

	to := query.Get("to")
	cdxjApi.To = To(to)

	matchType := query.Get("matchType")
	if matchType != "" {
		if !Contains(matchTypes, matchType) {
			return nil, fmt.Errorf("matchType must be one of %v, was: %s", matchTypes, matchType)
		}
		cdxjApi.MatchType = matchType
	} else {
		// Default to exact
		cdxjApi.MatchType = MatchTypeExact
	}

	limit := query.Get("limit")
	if limit != "" {
		l, err := strconv.ParseUint(limit, 10, 32)
		if err != nil {
			return nil, fmt.Errorf("limit must be a positive integer, was %s", limit)
		}
		cdxjApi.Limit = uint(l)
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
			return nil, fmt.Errorf("sort must be one of %v, was: %s", sorts, sort)
		}
		cdxjApi.Sort = sort
	}

	if cdxjApi.Sort == SortClosest && cdxjApi.MatchType != MatchTypeExact && closest == "" {
		return nil, fmt.Errorf("sort=closest requires a closest parameter and matchType=exact")
	}

	output := query.Get("output")
	if output != "" {
		if !Contains(outputs, output) {
			return nil, fmt.Errorf("output must be one of %v, was: %s", outputs, output)
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

// DbCdxServer implements searching the index database via a CDXJ API
type DbCdxServer struct {
	*index.DB
}

// search the index database and render each item with the provided renderFunc.
func (c DbCdxServer) search(api *CdxjServerApi, renderFunc RenderFunc) (uint, error) {
	key, err := parseKey(api.Url, api.MatchType)
	if err != nil {
		return 0, err
	}

	dateRange := DateRange{
		from: api.From,
		to:   api.To,
	}

	filter := parseFilter(api.Filter)

	sorter := &sorter{
		closest: api.Closest,
	}

	count := uint(0)

	perItemFn := func(item *badger.Item) (stopIteration bool) {
		key := Key(item.Key())

		contains, err := dateRange.contains(key.ts())
		if err != nil {
			log.Warnf("%s", err)
			return false
		}
		if !contains {
			log.Debugf("key timestamp not in range")
			return false
		}

		result := new(cdx.Cdx)
		err = item.Value(func(v []byte) error {
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
			log.WithError(err).WithField("url", api.Url).WithField("key", key).Error("failed to process item value")
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

		contains, err := dateRange.contains(key.ts())
		if err != nil {
			log.Warnf("%s", err)
			return false
		}
		if !contains {
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

	if api.Sort == SortClosest {
		f = sortPerItemFn
		a = sortAfterIterFn
	}

	for {
		err := c.DB.Search(key, api.Sort == SortReverse, f, a)
		if err != nil {
			return count, err
		}
		if count == 0 && strings.Contains(key, "http:") {
			// no results found, try https next round
			key = strings.Replace(key, "http:", "https:", 1)
		} else {
			return count, nil
		}
	}
}

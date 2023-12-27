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

package api

import (
	"fmt"
	"net/http"
	"regexp"
	"strconv"
	"strings"

	"github.com/nlnwa/gowarcserver/index"
	"github.com/nlnwa/gowarcserver/surt"
	"github.com/nlnwa/gowarcserver/timestamp"
	"github.com/nlnwa/whatwg-url/url"
)

const (
	SortClosest = "closest"
	SortReverse = "reverse"
)

var sorts = []string{SortClosest, SortReverse}

const (
	MatchTypeVerbatim = "verbatim"
	MatchTypeExact    = "exact"
	MatchTypePrefix   = "prefix"
	MatchTypeHost     = "host"
	MatchTypeDomain   = "domain"
)

var matchTypes = []string{
	MatchTypeDomain,
	MatchTypePrefix,
	MatchTypeHost,
	MatchTypeExact,
	MatchTypeVerbatim,
}

const (
	OutputCdxj = "cdxj"
	OutputJson = "json"

	// The following outputs is not implemented:
	// OutputLink = "link"
	// OutputText = "text"

)

var outputs = []string{OutputCdxj, OutputJson}

// CoreAPI implements a subset of https://pywb.readthedocs.io/en/latest/manual/cdxserver_api.html.
type CoreAPI struct {
	Collection string
	Url        *url.Url
	DateRange  *DateRange
	MatchType  string
	Limit      int
	Sort       string
	Closest    string
	Output     string
	Filter     []string
	Fields     []string
}

func (capi *CoreAPI) Uri() *url.Url {
	return capi.Url
}

func ClosestAPI(closest string, u *url.Url) SearchRequest {
	return SearchRequest{
		CoreAPI: &CoreAPI{
			Url:       u,
			Sort:      SortClosest,
			Closest:   closest,
			MatchType: MatchTypeExact,
			Limit:     10,
		},
	}
}

func Request(coreAPI *CoreAPI) SearchRequest {
	return SearchRequest{
		CoreAPI: coreAPI,
	}
}

type SearchRequest struct {
	*CoreAPI
	FilterMap map[string]string
}

func (c SearchRequest) Closest() string {
	if c.CoreAPI == nil {
		return ""
	}
	return c.CoreAPI.Closest
}

func (c SearchRequest) Ssurt() string {
	if c.CoreAPI == nil {
		return ""
	}
	if c.CoreAPI.Url == nil {
		return ""
	}
	return surt.UrlToSsurt(c.CoreAPI.Url)
}

func (c SearchRequest) Sort() index.Sort {
	if c.CoreAPI == nil {
		return index.SortNone
	}
	switch c.CoreAPI.Sort {
	case SortReverse:
		return index.SortDesc
	case SortClosest:
		return index.SortClosest
	default:
		return index.SortAsc
	}
}

func (c SearchRequest) DateRange() index.DateRange {
	if c.CoreAPI == nil {
		return &DateRange{}
	}
	return c.CoreAPI.DateRange
}

func (c SearchRequest) Filter() index.Filter {
	if c.CoreAPI == nil {
		return Filter{}
	}
	return ParseFilter(c.CoreAPI.Filter, c.FilterMap)
}

func (c SearchRequest) Limit() int {
	if c.CoreAPI == nil {
		return 0
	}
	return c.CoreAPI.Limit
}

func (c SearchRequest) MatchType() index.MatchType {
	if c.CoreAPI == nil {
		return index.MatchTypeExact
	}
	switch c.CoreAPI.MatchType {
	case MatchTypeExact:
		return index.MatchTypeExact
	case MatchTypePrefix:
		return index.MatchTypePrefix
	case MatchTypeHost:
		return index.MatchTypeHost
	case MatchTypeDomain:
		return index.MatchTypeDomain
	case MatchTypeVerbatim:
		return index.MatchTypeVerbatim
	default:
		return index.MatchTypeExact
	}
}

// contains returns true if string e is contained in string slice s.
func contains(s []string, e string) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}

var schemeRegExp = regexp.MustCompile(`^[a-z][a-z0-9+\-.]+(:.*)`)

// Parse parses the request r into a *CoreAPI.
func Parse(r *http.Request) (*CoreAPI, error) {
	var err error
	query := r.URL.Query()

	coreApi := new(CoreAPI)

	// currently the "cdx" does not accept collection as a query or param
	// coreApi.Collection = "all"

	matchType := query.Get("matchType")
	if matchType != "" {
		if !contains(matchTypes, matchType) {
			return nil, fmt.Errorf("matchType must be one of %v, was: %s", matchTypes, matchType)
		}
		coreApi.MatchType = matchType
	}

	urlStr := query.Get("url")
	if urlStr != "" {
		if !schemeRegExp.MatchString(urlStr) {
			urlStr = "http://" + urlStr
		}
		u, err := url.Parse(urlStr)
		if err != nil {
			return nil, err
		}
		coreApi.Url = u
	}

	if coreApi.DateRange, err = NewDateRange(query.Get("from"), query.Get("to")); err != nil {
		return nil, err
	}

	limit := query.Get("limit")
	if limit != "" {
		l, err := strconv.Atoi(limit)
		if err != nil {
			return nil, fmt.Errorf("limit must be a positive integer, was %s", limit)
		}
		coreApi.Limit = l
	}

	closest := query.Get("closest")
	if closest != "" {
		_, err := timestamp.Parse(closest)
		if err != nil {
			return nil, fmt.Errorf("closest failed to parse: %w", err)
		}
		coreApi.Closest = closest
	}

	sort := query.Get("sort")
	if sort != "" {
		if !contains(sorts, sort) {
			return nil, fmt.Errorf("sort must be one of %v, was: %s", sorts, sort)
		} else if sort == SortClosest && closest == "" {
			sort = ""
		} else if sort == SortClosest && coreApi.Url == nil {
			return nil, fmt.Errorf("sort=closest is not valid without urls")
		}
		coreApi.Sort = sort
	}

	output := query.Get("output")
	if output != "" {
		if !contains(outputs, output) {
			return nil, fmt.Errorf("output must be one of %v, was: %s", outputs, output)
		}
		coreApi.Output = output
	}

	filter, ok := query["filter"]
	if ok {
		coreApi.Filter = filter
	}

	fields := query.Get("fields")
	if fields != "" {
		coreApi.Fields = strings.Split(fields, ",")
	}

	return coreApi, nil
}

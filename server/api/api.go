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

)

var outputs = []string{OutputCdxj, OutputJson}

// CoreAPI implements a subset of https://pywb.readthedocs.io/en/latest/manual/cdxserver_api.html.
type CoreAPI struct {
	Collection string
	Urls       []*url.Url
	FromTo     *DateRange
	MatchType  string
	Limit      int
	Sort       string
	Closest    string
	Output     string
	Filter     []string
	Fields     []string
}

type SearchAPI struct {
	*CoreAPI
}

func (c SearchAPI) Closest() string {
	return c.CoreAPI.Closest
}

func (c SearchAPI) Key() string {
	return MatchType(surt.UrlToSsurt(c.CoreAPI.Urls[0]), c.CoreAPI.MatchType)
}

func (c SearchAPI) Keys() []string {
	keys := make([]string, len(c.Urls))
	for i, u := range c.CoreAPI.Urls {
		keys[i] = MatchType(surt.UrlToSsurt(u), c.CoreAPI.MatchType)
	}
	return keys
}

func (c SearchAPI) Sort() index.Sort {
	switch c.CoreAPI.Sort {
	case SortReverse:
		return index.SortDesc
	case SortClosest:
		return index.SortClosest
	default:
		return index.SortAsc
	}
}

func (c SearchAPI) DateRange() index.DateRange {
	return c.CoreAPI.FromTo
}

func (c SearchAPI) Filter() index.Filter {
	return ParseFilter(c.CoreAPI.Filter)
}

func (c SearchAPI) Limit() int {
	return c.CoreAPI.Limit
}

func (c SearchAPI) MatchType() string {
	return c.CoreAPI.MatchType
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

var schemeRegExp = regexp.MustCompile(`^([a-z][a-z0-9+\-.]*):`)

// Parse parses the request r into a *CoreAPI.
func Parse(r *http.Request) (*CoreAPI, error) {
	var err error
	query := r.URL.Query()

	coreApi := new(CoreAPI)

	// currently the "cdx" does not accept collection as a query or param
	coreApi.Collection = "all"

	matchType := query.Get("matchType")
	if matchType != "" {
		if !contains(matchTypes, matchType) {
			return nil, fmt.Errorf("matchType must be one of %v, was: %s", matchTypes, matchType)
		}
		coreApi.MatchType = matchType
	} else {
		// Default to exact
		coreApi.MatchType = MatchTypeExact
	}

	urls := query["url"]
	if len(urls) == 1 && !schemeRegExp.MatchString(urls[0]) {
		u := urls[0]
		// Add http scheme
		urls = []string{
			"http://" + u,
		}
		// Add https scheme only for exact match to get results for both http/https
		// If match type is prefix, domain or host the scheme part will be stripped so no need.
		if coreApi.MatchType == MatchTypeExact {
			urls = append(urls, "https://"+u)
		}
	}
	for _, urlStr := range urls {
		u, err := url.Parse(urlStr)
		if err != nil {
			return nil, err
		}
		coreApi.Urls = append(coreApi.Urls, u)
	}

	if coreApi.FromTo, err = NewDateRange(query.Get("from"), query.Get("to")); err != nil {
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
		} else if sort == SortClosest && len(coreApi.Urls) == 0 {
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

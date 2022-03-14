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
	"github.com/gorilla/mux"
	"github.com/nlnwa/gowarcserver/internal/timestamp"
	"github.com/nlnwa/gowarcserver/schema"
	url "github.com/nlnwa/whatwg-url/url"
	"net/http"
	"regexp"
	"strconv"
	"strings"
)

type PerCdxFunc func(cdx *schema.Cdx) error

type pywbJson struct {
	Urlkey    string `json:"urlkey,omitempty"`
	Timestamp string `json:"timestamp"`
	Url       string `json:"url"`
	Mime      string `json:"mime,omitempty"`
	Status    string `json:"status,omitempty"`
	Digest    string `json:"digest"`
	Length    string `json:"length,omitempty"`
	Offset    string `json:"offset,omitempty"`
	Filename  string `json:"filename,omitempty"`
}

func cdxjToPywbJson(record *schema.Cdx) *pywbJson {
	return &pywbJson{
		Urlkey:    record.Ssu,
		Timestamp: record.Sts,
		Url:       record.Uri,
		Mime:      record.Mct,
		Status:    record.Hsc,
		Digest:    record.Sha,
		Length:    record.Rle,
		// Offset must be empty string or else pywb will try to use it's internal index.
		// Filename must be an empty string or else pywb will try to use it's internal index.
	}
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

)

var outputs = []string{OutputCdxj, OutputJson}

// CdxjServerApi encapsulates a subset of https://pywb.readthedocs.io/en/latest/manual/cdxserver_api.html.
type CdxjServerApi struct {
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

// Contains returns true if string e is contained in string slice s.
func Contains(s []string, e string) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}

var schemeRegExp = regexp.MustCompile(`^([a-z][a-z0-9+\-.]*):`)

// ParseCdxjApi parses a *http.Request into a *CdxjServerApi.
func ParseCdxjApi(r *http.Request) (*CdxjServerApi, error) {
	var err error
	vars := mux.Vars(r)
	query := r.URL.Query()

	cdxjApi := new(CdxjServerApi)

	cdxjApi.Collection = vars["collection"]

	urls, ok := query["url"]
	if !ok {
		return nil, fmt.Errorf("missing required query parameter \"url\"")
	}
	if len(urls) == 1 && !schemeRegExp.MatchString(urls[0]) {
		u := urls[0]
		urls = []string{
			"http://" + u,
			"https://" + u,
		}
	}
	for _, urlStr := range urls {
		u, err := url.Parse(urlStr)
		if err != nil {
			return nil, err
		}
		cdxjApi.Urls = append(cdxjApi.Urls, u)
	}

	if cdxjApi.FromTo, err = NewDateRange(query.Get("from"), query.Get("to")); err != nil {
		return nil, err
	}

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
		l, err := strconv.Atoi(limit)
		if err != nil {
			return nil, fmt.Errorf("limit must be a positive integer, was %s", limit)
		}
		cdxjApi.Limit = l
	}

	closest := query.Get("closest")
	if closest != "" {
		_, err := timestamp.Parse(closest)
		if err != nil {
			return nil, fmt.Errorf("closest failed to parse: %w", err)
		}
		cdxjApi.Closest = closest
	}

	sort := query.Get("sort")
	if sort != "" {
		if !Contains(sorts, sort) {
			return nil, fmt.Errorf("sort must be one of %v, was: %s", sorts, sort)
		}
		if closest == "" && sort == SortClosest {
			sort = ""
		}
		cdxjApi.Sort = sort
	}

	output := query.Get("output")
	if output != "" {
		if !Contains(outputs, output) {
			return nil, fmt.Errorf("output must be one of %v, was: %s", outputs, output)
		}
		cdxjApi.Output = output
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

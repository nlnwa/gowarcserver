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

	"github.com/julienschmidt/httprouter"
	"github.com/nlnwa/gowarcserver/internal/surt"
	"github.com/nlnwa/gowarcserver/internal/timestamp"
	"github.com/nlnwa/gowarcserver/schema"
	"github.com/nlnwa/whatwg-url/url"
)

type pywbJson struct {
	Urlkey    string `json:"urlkey,omitempty"`
	Timestamp string `json:"timestamp"`
	Url       string `json:"url"`
	Mime      string `json:"mime,omitempty"`
	Status    string `json:"status,omitempty"`
	Digest    string `json:"digest"`
	Length    string `json:"length,omitempty"`
}

func cdxToPywbJson(cdx *schema.Cdx) *pywbJson {
	return &pywbJson{
		Urlkey:    cdx.GetSsu(),
		Timestamp: timestamp.TimeTo14(cdx.GetSts().AsTime()),
		Url:       cdx.GetUri(),
		Mime:      cdx.GetMct(),
		Status:    strconv.Itoa(int(cdx.GetHsc())),
		Digest:    cdx.GetDig(),
		Length:    strconv.Itoa(int(cdx.GetRle())),
	}
}

func parseResourceRequest(r *http.Request) (closestRequest, error) {
	params := httprouter.ParamsFromContext(r.Context())

	// closest parameter
	p0 := params.ByName("timestamp")
	// remove trailing 'id_'
	closest := p0[:len(p0)-3]

	// url parameter
	p1 := params.ByName("url")
	// remove leading '/'
	uri := p1[1:]

	// we must add on any query parameters
	if q := r.URL.Query().Encode(); len(q) > 0 {
		uri += "?" + q
	}
	// and fragment
	if len(r.URL.Fragment) > 0 {
		// and fragment
		uri += "#" + r.URL.Fragment
	}

	u, err := url.Parse(uri)
	if err != nil {
		return closestRequest{}, fmt.Errorf("failed to parse uri: %w", err)
	}
	key := surt.UrlToSsurt(u)

	return closestRequest{
		rawUrl:  uri,
		key:     key,
		closest: closest,
		limit:   1,
	}, nil
}

type closestRequest struct {
	rawUrl  string
	key     string
	closest string
	limit   int
}

func (c closestRequest) Key() string {
	return c.key
}

func (c closestRequest) Closest() string {
	return c.closest
}

func (c closestRequest) Limit() int {
	return c.limit
}

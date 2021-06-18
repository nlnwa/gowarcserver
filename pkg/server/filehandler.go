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

	"github.com/nlnwa/gowarcserver/pkg/index"
	"github.com/nlnwa/gowarcserver/pkg/loader"
	"github.com/nlnwa/gowarcserver/pkg/server/localhttp"
)

type fileHandler struct {
	loader   *loader.Loader
	db       *index.DB
	children *localhttp.Children
}

func (h *fileHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	localhttp.AggregatedQuery(h, w, r)
}

func (h *fileHandler) ServeLocalHTTP(r *http.Request) (*localhttp.Writer, error) {
	files, err := h.db.ListFileNames()
	if err != nil {
		return nil, fmt.Errorf("error reading files: %v", err)
	}

	localWriter := localhttp.NewWriter()
	localWriter.Header().Set("Content-Type", "text/plain")
	for _, f := range files {
		fmt.Fprintf(localWriter, "%v\n", f)
	}
	return localWriter, nil
}

func (h *fileHandler) PredicateFn(r *http.Response) bool {
	return r.StatusCode >= 200 && r.StatusCode < 300
}

func (h *fileHandler) Children() *localhttp.Children {
	return h.children
}

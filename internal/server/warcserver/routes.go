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
	"github.com/nlnwa/gowarcserver/internal/database"
	"github.com/nlnwa/gowarcserver/internal/loader"
	"github.com/nlnwa/gowarcserver/internal/server/handlers"
	"net/http"
	"net/url"
	"time"
)

func handleRoot(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_, _ = fmt.Fprintf(w, `["all"]`)
}

func RegisterProxy(r *mux.Router, children []*url.URL, timeout time.Duration) {
	indexHandler := handlers.AggregatedHandler(children, timeout)
	resourceHandler := handlers.FirstHandler(children, timeout)

	// JSON list of available endpoints
	r.HandleFunc("/", handleRoot)
	// Direct index
	r.Handle("/{collection}/index", indexHandler)
	// Direct resource
	r.Handle("/{collection}/resource", resourceHandler)
}

func Register(r *mux.Router, loader *loader.Loader, db *database.CdxDbIndex) {
	indexHandler := &IndexHandler{DbCdxServer{db}}
	resourceHandler := &ResourceHandler{
		DbCdxServer: DbCdxServer{db},
		loader:      loader,
	}

	// https://pywb.readthedocs.io/en/latest/manual/warcserver.html#warcserver-api

	// JSON list of available endpoints
	r.HandleFunc("/", handleRoot)
	// Direct index
	r.Handle("/{collection}/index", indexHandler)
	// Direct resource
	r.Handle("/{collection}/resource", resourceHandler)
}

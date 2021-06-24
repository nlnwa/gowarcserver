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
	"net/http"
	"net/url"
	"time"

	"github.com/gorilla/mux"
	"github.com/nlnwa/gowarcserver/pkg/index"
	"github.com/nlnwa/gowarcserver/pkg/loader"
	"github.com/nlnwa/gowarcserver/pkg/server/handlers"
)

func RegisterRoutes(r *mux.Router, db *index.DB, loader *loader.Loader, children []url.URL, timeout time.Duration) {
	var resourceHandler http.Handler
	var indexHandler http.Handler

	if len(children) > 0 {
		indexHandler = handlers.Aggregated(children, timeout)
		resourceHandler = handlers.FirstHandler(children, timeout)
	} else {
		indexHandler = &IndexHandler{DbCdxServer{db},}
		resourceHandler = &ResourceHandler{
			DbCdxServer: DbCdxServer{db},
			loader: loader,
		}
	}

	// https://pywb.readthedocs.io/en/latest/manual/warcserver.html#warcserver-api

	// JSON list of available endpoints
	r.Handle("/", &rootHandler{})
	// Direct index
	r.Handle("/{collection}/index", indexHandler)
	// Direct resource
	r.Handle("/{collection}/resource", resourceHandler)
}

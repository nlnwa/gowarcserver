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

	"github.com/julienschmidt/httprouter"
	"github.com/nlnwa/gowarcserver/internal/database"
	"github.com/nlnwa/gowarcserver/internal/loader"
)

func Register(r *httprouter.Router, middleware func(http.Handler) http.Handler, pathPrefix string, loader *loader.Loader, db *database.CdxDbIndex) {
	var indexHandler http.Handler
	var resourceHandler http.Handler

	indexHandler = IndexHandler{DbCdxServer{db}}
	resourceHandler = ResourceHandler{
		DbCdxServer: DbCdxServer{db},
		Loader:      loader,
	}
	if middleware != nil {
		indexHandler = middleware(indexHandler)
		resourceHandler = middleware(resourceHandler)
	}

	// https://pywb.readthedocs.io/en/latest/manual/warcserver.html#warcserver-api
	r.Handler("GET", pathPrefix+"/cdx", indexHandler)
	r.Handler("GET", pathPrefix+"/web/:closest/*url", resourceHandler)
}

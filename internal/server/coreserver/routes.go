/*
 * Copyright 2021 National Library of Norway.
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

package coreserver

import (
	"github.com/julienschmidt/httprouter"
	"github.com/nlnwa/gowarcserver/internal/database"
	"github.com/nlnwa/gowarcserver/internal/loader"
	"net/http"
)

func Register(r *httprouter.Router, middleware func(http.Handler) http.Handler, pathPrefix string, loader *loader.Loader, db *database.CdxDbIndex) {
	indexHandler := IndexHandler{db}
	r.Handler("GET", pathPrefix+"/ids", http.HandlerFunc(indexHandler.ListIds))
	r.Handler("GET", pathPrefix+"/files", http.HandlerFunc(indexHandler.ListFileNames))
	r.Handler("GET", pathPrefix+"/search", http.HandlerFunc(indexHandler.Search))
	r.Handler("GET", pathPrefix+"/id/{id}", contentHandler{loader})
}

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
	"github.com/nlnwa/gowarcserver/internal/index"
	"github.com/nlnwa/gowarcserver/internal/loader"
	"github.com/nlnwa/gowarcserver/internal/server/api"
	"net/http"
)

func Register(r *httprouter.Router, mw func(http.Handler) http.Handler, pathPrefix string, l loader.RecordLoader, db *index.DB) {
	coreHandler := Handler{api.DbAdapter{DB: db}, l}

	r.Handler("GET", pathPrefix+"/id", mw(http.HandlerFunc(coreHandler.listId)))
	r.Handler("GET", pathPrefix+"/ids", mw(http.HandlerFunc(coreHandler.listIds)))
	r.Handler("GET", pathPrefix+"/id/:urn", mw(http.HandlerFunc(coreHandler.getStorageRefByURN)))
	r.Handler("GET", pathPrefix+"/file", mw(http.HandlerFunc(coreHandler.listFile)))
	r.Handler("GET", pathPrefix+"/files", mw(http.HandlerFunc(coreHandler.listFiles)))
	r.Handler("GET", pathPrefix+"/file/:filename", mw(http.HandlerFunc(coreHandler.getFileInfoByFilename)))
	r.Handler("GET", pathPrefix+"/cdx", mw(http.HandlerFunc(coreHandler.listCdx)))
	r.Handler("GET", pathPrefix+"/cdxs", mw(http.HandlerFunc(coreHandler.listCdxs)))
	r.Handler("GET", pathPrefix+"/search", mw(http.HandlerFunc(coreHandler.search)))
	r.Handler("GET", pathPrefix+"/record/:urn", mw(http.HandlerFunc(coreHandler.loadRecordByUrn)))
}

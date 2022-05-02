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
	"github.com/nlnwa/gowarcserver/internal/index"
	"github.com/nlnwa/gowarcserver/internal/server/api"
	"net/http"

	"github.com/julienschmidt/httprouter"
	"github.com/nlnwa/gowarcserver/internal/loader"
)

func Register(r *httprouter.Router, mw func(http.Handler) http.Handler, pathPrefix string, loader *loader.Loader, db *index.DB) {
	handler := Handler{
		db:     api.DbAdapter{DB: db},
		loader: loader,
	}

	// https://pywb.readthedocs.io/en/latest/manual/warcserver.html#warcserver-api
	r.Handler("GET", pathPrefix+"/cdx", mw(http.HandlerFunc(handler.index)))
	r.Handler("GET", pathPrefix+"/web/:timestamp/*url", mw(http.HandlerFunc(handler.resource)))
}

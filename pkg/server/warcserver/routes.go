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
	"github.com/gorilla/mux"
	"github.com/nlnwa/gowarcserver/pkg/index"
	"github.com/nlnwa/gowarcserver/pkg/loader"
)

func RegisterRoutes(r *mux.Router, db *index.Db, loader *loader.Loader) {
	r.Handle("/", &rootHandler{})
	r.Handle("/{collection}/index", &indexHandler{loader: loader, db: db})
	r.Handle("/{collection}/resource", &resourceHandler{loader: loader, db: db})
}

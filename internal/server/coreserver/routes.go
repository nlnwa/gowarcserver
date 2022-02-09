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
	"github.com/gorilla/mux"
	"github.com/nlnwa/gowarcserver/internal/database"
	"github.com/nlnwa/gowarcserver/internal/loader"
)

func Register(r *mux.Router, loader loader.RecordLoader, db *database.CdxDbIndex) {
	indexHandler := IndexHandler{db}
	r.HandleFunc("/ids", indexHandler.ListIds)
	r.HandleFunc("/files", indexHandler.ListFileNames)
	r.HandleFunc("/search", indexHandler.Search)
	r.Handle("/id/{id}", contentHandler{loader})
}

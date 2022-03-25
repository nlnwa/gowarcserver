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

package coreserver

import (
	"fmt"
	"net/http"

	"github.com/julienschmidt/httprouter"
	"github.com/nlnwa/gowarc"
	"github.com/nlnwa/gowarcserver/internal/loader"
)

type contentHandler struct {
	loader loader.RecordLoader
}

func (h contentHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	params := httprouter.ParamsFromContext(r.Context())
	warcId := params.ByName("id")
	if warcId == "" {
		http.Error(w, `missing required parameter "id"`, http.StatusBadRequest)
		return
	}
	if warcId[0] != '<' {
		warcId = "<" + warcId
	}
	if warcId[len(warcId)-1] != '>' {
		warcId = warcId + ">"
	}

	record, err := h.loader.Load(r.Context(), warcId)
	if err != nil {
		msg := fmt.Sprintf("failed to load record: %v", err)
		http.Error(w, msg, http.StatusInternalServerError)
		return
	}
	defer record.Close()

	w.Header().Add("content-type", "application/octet-stream")
	marshaler := gowarc.NewMarshaler()

	// write records until all relevant records are written
	continuation := record
	for continuation != nil {
		continuation, _, err = marshaler.Marshal(w, continuation, 0)
		if err != nil {
			msg := fmt.Sprintf("failed to marshal record: %v", err)
			http.Error(w, msg, http.StatusInternalServerError)
		}
	}
}

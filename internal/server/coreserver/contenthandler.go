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
	"context"
	"fmt"
	"io"
	"net/http"

	"github.com/nlnwa/gowarcserver/internal/loader"

	"github.com/gorilla/mux"
	"github.com/nlnwa/gowarc"
)

type contentHandler struct {
	loader loader.RecordLoader
}

func (h contentHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	warcId, ok := mux.Vars(r)["id"]
	if !ok {
		http.NotFound(w, r)
		return
	}
	if len(warcId) > 0 && warcId[0] != '<' {
		warcId = "<" + warcId + ">"
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	record, err := h.loader.Load(ctx, warcId)
	if err != nil {
		msg := fmt.Sprintf("Failed to load record: %v", err)
		http.Error(w, msg, http.StatusInternalServerError)
		return
	}
	defer func() {
		_ = record.Close()
	}()

	switch v := record.Block().(type) {
	case gowarc.PayloadBlock:
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, err = record.WarcHeader().Write(w)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		byteReader, err := v.PayloadBytes()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		_, err = io.Copy(w, byteReader)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	default:
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, err = record.WarcHeader().Write(w)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		rb, err := v.RawBytes()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		_, err = io.Copy(w, rb)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}
}

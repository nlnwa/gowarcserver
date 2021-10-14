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

package server

import (
	"context"
	"io"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/nlnwa/gowarc"
	"github.com/nlnwa/gowarcserver/pkg/loader"
	log "github.com/sirupsen/logrus"
)

type contentHandler struct {
	loader *loader.Loader
}

func (h *contentHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	warcid := mux.Vars(r)["id"]
	if len(warcid) > 0 && warcid[0] != '<' {
		warcid = "<" + warcid + ">"
	}

	log.Debugf("request id: %v", warcid)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	record, err := h.loader.Get(ctx, warcid)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer func() {
		err := record.Close()
		if err != nil {
			log.Warnf("failed to close warc record: %s", err)
		}
	}()

	switch v := record.Block().(type) {
	case gowarc.PayloadBlock:
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
		w.Header().Set("Content-Type", "text/plain")
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

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
	"fmt"
	"io"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/nlnwa/gowarc/warcrecord"
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
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(404)
		if _, werr := w.Write([]byte("Document not found\n")); werr != nil {
			log.Warn("Failed to write 404 message to body: ", werr)
		}
		return
	}
	defer record.Close()

	switch v := record.Block().(type) {
	case *warcrecord.RevisitBlock:
		r, err := v.Response()
		if err != nil {
			log.Warnf("%v", err)
			return
		}
		renderContent(w, v, r)
	case warcrecord.HttpResponseBlock:
		r, err := v.Response()
		if err != nil {
			log.Warnf("%v", err)
			return
		}
		renderContent(w, v, r)
	default:
		w.Header().Set("Content-Type", "text/plain")
		_, err = record.WarcHeader().Write(w)
		if err != nil {
			log.Warnf("Failed to write response header to %s", r.URL)
			return
		}

		fmt.Fprintln(w)
		rb, err := v.RawBytes()
		if err != nil {
			log.Warnf("Failed to get raw bytes: %v", err)
			return
		}
		_, err = io.Copy(w, rb)
		if err != nil {
			log.Warnf("Failed to writer content for request to %s", r.URL)
		}
	}
}

func renderContent(w http.ResponseWriter, v warcrecord.PayloadBlock, r *http.Response) {
	for k, vl := range r.Header {
		for _, v := range vl {
			w.Header().Set(k, v)
		}
	}
	p, err := v.PayloadBytes()
	if err != nil {
		return
	}
	_, err = io.Copy(w, p)
	if err != nil {
		log.Warnf("Failed to writer content for request to %s", r.Request.URL)
	}
}

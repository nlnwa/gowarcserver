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

package handlers

import (
	"fmt"
	"github.com/nlnwa/gowarc"
	"io"
	"net/http"
)

// RenderRecord renders gowarc.WarcRecord rec as a binary stream.
func RenderRecord(w http.ResponseWriter, rec gowarc.WarcRecord) (int64, error) {
	w.Header().Add("Content-Type", "application/octet-stream")

	marshaler := gowarc.NewMarshaler()

	var err error
	var written int64
	var n int64

	continuation := rec
	for continuation != nil {
		continuation, n, err = marshaler.Marshal(w, continuation, 0)
		written += n
		if err != nil {
			break
		}
	}
	return written, err
}

// RenderContent renders the HTTP payload.
func RenderContent(w http.ResponseWriter, r gowarc.HttpResponseBlock) error {
	p, err := r.PayloadBytes()
	if err != nil {
		return fmt.Errorf("failed to retrieve payload bytes: %w", err)
	}

	return render(w, *r.HttpHeader(), r.HttpStatusCode(), p)
}

func RenderRedirect(w http.ResponseWriter, location string) {
	w.Header().Set("Location", location)
	w.Header().Set("Content-Length", "0")
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")

	// Write status line
	w.WriteHeader(http.StatusFound)
}

func render(w http.ResponseWriter, h http.Header, code int, r io.Reader) error {
	// Write headers
	for key, values := range h {
		for i, value := range values {
			if i == 0 {
				w.Header().Set(key, value)
			} else {
				w.Header().Add(key, value)
			}
		}
	}

	// Write status line
	w.WriteHeader(code)

	// We are done if no reader
	if r == nil {
		return nil
	}

	// Write HTTP payload
	_, err := io.Copy(w, r)
	if err != nil {
		return fmt.Errorf("failed to write HTTP payload: %w", err)
	}

	return nil
}

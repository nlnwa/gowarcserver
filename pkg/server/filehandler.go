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
	"fmt"
	"net/http"

	"github.com/nlnwa/gowarcserver/pkg/index"
	"github.com/nlnwa/gowarcserver/pkg/loader"
	log "github.com/sirupsen/logrus"
)

type fileHandler struct {
	loader *loader.Loader
	db     *index.Db
}

func (h *fileHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	files, err := h.db.ListFileNames()
	if err != nil {
		log.Fatalf("error reading files: %v", err)
	}
	w.Header().Set("Content-Type", "text/plain")
	for _, f := range files {
		fmt.Fprintf(w, "%v\n", f)
	}
}

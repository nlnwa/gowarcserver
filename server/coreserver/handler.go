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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/julienschmidt/httprouter"
	"github.com/nlnwa/gowarcserver/index"
	"github.com/nlnwa/gowarcserver/loader"
	"github.com/nlnwa/gowarcserver/server/api"
	"github.com/nlnwa/gowarcserver/server/handlers"
	"github.com/rs/zerolog/log"
	"google.golang.org/protobuf/encoding/protojson"
)

var lf = []byte("\n")

type Handler struct {
	index.CdxAPI
	index.FileAPI
	index.IdAPI
	loader.StorageRefResolver
	loader.WarcLoader
}

func (h Handler) search(w http.ResponseWriter, r *http.Request) {
	coreAPI, err := api.Parse(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	start := time.Now()
	count := 0
	defer func() {
		log.Debug().Str("request", fmt.Sprintf("%+v", coreAPI)).Msgf("Found %d items in %s", count, time.Since(start))
	}()

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	response := make(chan index.CdxResponse)

	if err = h.CdxAPI.Search(ctx, api.SearchAPI{CoreAPI: coreAPI}, response); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		log.Error().Err(err).Msgf("Search failed: %+v", coreAPI)
		return
	}

	for res := range response {
		if res.Error != nil {
			log.Warn().Err(res.Error).Msg("response error")
			continue
		}
		v, err := protojson.Marshal(res)
		if err != nil {
			log.Warn().Err(err).Msg("failed to marshal result")
			continue
		}
		if count > 0 {
			_, _ = w.Write(lf)
		}
		_, err = io.Copy(w, bytes.NewReader(v))
		if err != nil {
			log.Warn().Err(err).Msg("failed to write result")
			return
		}
		count++
	}
	_, _ = w.Write(lf)
}

type storageRef struct {
	Id       string `json:"id"`
	Filename string `json:"filename"`
	Offset   int64  `json:"offset"`
}

func (h Handler) listIds(w http.ResponseWriter, r *http.Request) {
	limit := parseLimit(r)

	response := make(chan index.IdResponse)
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	if err := h.IdAPI.ListStorageRef(ctx, limit, response); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		log.Error().Err(err).Msg("Failed to list ids")
		return
	}
	start := time.Now()
	count := 0
	defer func() {
		log.Debug().Msgf("Found %d items in %s", count, time.Since(start))
	}()

	for res := range response {
		if res.Error != nil {
			log.Warn().Err(res.Error).Msg("response error")
			continue
		}
		filename, offset, err := parseStorageRef(res.Value)
		if err != nil {
			log.Warn().Err(err).Msgf("failed to parse storage ref: %s", res.Value)
			continue
		}
		ref := &storageRef{
			Id:       res.Key,
			Filename: filename,
			Offset:   offset,
		}
		v, err := json.Marshal(ref)
		if err != nil {
			log.Warn().Err(err).Msgf("failed to marshal storage ref: %+v", ref)
			continue
		}
		if count > 0 {
			_, _ = w.Write(lf)
		}
		_, err = io.Copy(w, bytes.NewReader(v))
		if err != nil {
			log.Warn().Err(err).Msgf("failed to write storage ref: %+v", ref)
			return
		}
		count++
	}
	_, _ = w.Write(lf)
}

func (h Handler) getStorageRefByURN(w http.ResponseWriter, r *http.Request) {
	params := httprouter.ParamsFromContext(r.Context())
	urn := params.ByName("urn")

	storageRef, err := h.StorageRefResolver.Resolve(r.Context(), urn)
	if err != nil {
		err := fmt.Errorf("failed to resolve storage ref: %s: %w", urn, err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		log.Error().Err(err).Msg("")
		return
	}
	_, err = fmt.Fprintln(w, storageRef)
	if err != nil {
		log.Warn().Err(err).Msgf("Failed to write storage ref: %s", storageRef)
	}
}

func (h Handler) listFiles(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	limit := parseLimit(r)
	responses := make(chan index.FileInfoResponse)

	if err := h.FileAPI.ListFileInfo(ctx, limit, responses); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		log.Error().Err(err).Msg("Failed to list files")
		return
	}

	start := time.Now()
	count := 0
	defer func() {
		log.Debug().Msgf("Found %d items in %s", count, time.Since(start))
	}()

	for res := range responses {
		if res.Error != nil {
			log.Warn().Err(res.Error).Msg("response error")
			continue
		}
		v, err := protojson.Marshal(res.Fileinfo)
		if err != nil {
			log.Warn().Err(err).Msg("failed to marshal file info")
			continue
		}
		if count > 0 {
			_, _ = w.Write(lf)
		}
		_, err = io.Copy(w, bytes.NewReader(v))
		if err != nil {
			log.Warn().Err(err).Msg("failed to write file info")
			return
		}
		count++
	}
	_, _ = w.Write(lf)
}

func (h Handler) getFileInfoByFilename(w http.ResponseWriter, r *http.Request) {
	params := httprouter.ParamsFromContext(r.Context())
	filename := params.ByName("filename")

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	fileInfo, err := h.FileAPI.GetFileInfo(ctx, filename)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		log.Error().Err(err).Msgf("Failed to get file info: %s", filename)
		return
	}
	_, err = fmt.Fprintln(w, protojson.Format(fileInfo))
	if err != nil {
		log.Warn().Err(err).Msgf("Failed to write file info: %s", protojson.Format(fileInfo))
	}
}

func (h Handler) listCdxs(w http.ResponseWriter, r *http.Request) {
	limit := parseLimit(r)
	responses := make(chan index.CdxResponse)
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	if err := h.CdxAPI.List(ctx, limit, responses); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		log.Error().Err(err).Msg("Failed to list cdx records")
		return
	}

	start := time.Now()
	count := 0
	defer func() {
		log.Debug().Msgf("Found %d items in %s", count, time.Since(start))
	}()

	for res := range responses {
		if res.Error != nil {
			log.Warn().Err(res.Error).Msg("response error")
			continue
		}
		v, err := protojson.Marshal(res.Cdx)
		if err != nil {
			log.Warn().Err(err).Msg("failed to marshal cdx to json")
			continue
		}
		if count > 0 {
			_, _ = w.Write(lf)
		}
		_, err = io.Copy(w, bytes.NewReader(v))
		if err != nil {
			log.Warn().Err(err).Msg("failed to write cdx record")
			return
		}
		count++
	}
	_, _ = w.Write(lf)
}

func (h Handler) loadRecordByUrn(w http.ResponseWriter, r *http.Request) {
	params := httprouter.ParamsFromContext(r.Context())
	warcId := params.ByName("urn")

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	record, closer, err := h.WarcLoader.LoadById(ctx, warcId)
	if err != nil {
		err := fmt.Errorf("failed to load record '%s': %w", warcId, err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		log.Error().Err(err).Msg("")
		return
	}
	defer closer.Close()
	defer record.Close()

	_, err = handlers.RenderRecord(w, record)
	if err != nil {
		log.Warn().Err(err).Msgf("Failed to write record '%s': %v", warcId, record)
	}
}

func parseStorageRef(ref string) (filename string, offset int64, err error) {
	n := strings.IndexRune(ref, ':')
	if n == -1 {
		err = fmt.Errorf("invalid storage ref, missing scheme delimiter ':'")
		return
	}
	ref = ref[n+1:]
	n = strings.IndexRune(ref, '#')
	if n == -1 {
		err = fmt.Errorf("invalid storage ref, missing offset delimiter '#'")
		return
	}
	filename = ref[:n]
	offset, err = strconv.ParseInt(ref[n+1:], 10, 64)
	if err != nil {
		err = fmt.Errorf("invalid storage ref, invalid offset: %w", err)
		return
	}
	return
}

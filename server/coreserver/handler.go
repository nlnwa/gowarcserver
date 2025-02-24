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
	"github.com/nlnwa/gowarcserver/internal/keyvalue"
	"github.com/nlnwa/gowarcserver/loader"
	"github.com/nlnwa/gowarcserver/server/api"
	"github.com/nlnwa/gowarcserver/server/handlers"
	"github.com/rs/zerolog/log"
	"google.golang.org/protobuf/encoding/protojson"
)

var lf = []byte("\n")

type Handler struct {
	DebugAPI           keyvalue.DebugAPI
	CdxAPI             index.CdxAPI
	FileAPI            index.FileAPI
	IdAPI              index.IdAPI
	ReportAPI          index.ReportAPI
	StorageRefResolver loader.StorageRefResolver
	WarcLoader         loader.WarcLoader
}

func (h Handler) debug(w http.ResponseWriter, r *http.Request) {
	if h.DebugAPI == nil {
		http.Error(w, "Debug API not implemented", http.StatusNotImplemented)
		return
	}

	coreAPI, err := api.Parse(r.URL.Query())
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	req := keyvalue.DebugRequest{
		Key:     r.URL.Query().Get("key"),
		Request: coreAPI,
	}

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	response := make(chan keyvalue.CdxResponse)

	if err := h.DebugAPI.Debug(ctx, req, response); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	start := time.Now()
	count := 0
	defer func() {
		log.Debug().Msgf("Found %d items in %s", count, time.Since(start))
	}()

	enc := json.NewEncoder(w)
	for res := range response {
		err = enc.Encode(res)
		if err != nil {
			log.Warn().Err(err).Msg("failed to marshal result")
			continue
		}
		count++
	}
}

func (h Handler) search(w http.ResponseWriter, r *http.Request) {
	coreAPI, err := api.Parse(r.URL.Query())
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	start := time.Now()
	count := 0
	defer func() {
		log.Debug().Str("request", fmt.Sprintf("%+v", coreAPI)).Msgf("Found %d items in %s", count, time.Since(start))
	}()

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	response := make(chan index.CdxResponse)

	if err = h.CdxAPI.Search(ctx, coreAPI, response); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		log.Error().Err(err).Msgf("Search failed: %+v", coreAPI)
		return
	}

	for res := range response {
		if res.GetError() != nil {
			log.Warn().Err(res.GetError()).Msg("failed result")
			continue
		}
		v, err := protojson.Marshal(res.GetCdx())
		if err != nil {
			log.Warn().Err(err).Msg("failed to marshal result")
			continue
		}
		_, err = io.Copy(w, bytes.NewReader(v))
		if err != nil {
			log.Warn().Err(err).Msg("failed to write result")
			return
		}
		_, _ = w.Write(lf)
		count++
	}
}

type storageRef struct {
	Id       string `json:"id"`
	Filename string `json:"filename"`
	Offset   int64  `json:"offset"`
}

func (h Handler) listIds(w http.ResponseWriter, r *http.Request) {
	coreAPI, err := api.Parse(r.URL.Query())
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	response := make(chan index.IdResponse)
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	if err := h.IdAPI.ListStorageRef(ctx, coreAPI, response); err != nil {
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
		if res.GetError() != nil {
			log.Warn().Err(res.GetError()).Msg("failed result")
			continue
		}
		filename, offset, err := parseStorageRef(res.GetValue())
		if err != nil {
			log.Warn().Err(err).Msgf("failed to parse storage ref: %s", res.GetValue())
			continue
		}
		ref := &storageRef{
			Id:       res.GetId(),
			Filename: filename,
			Offset:   offset,
		}
		v, err := json.Marshal(ref)
		if err != nil {
			log.Warn().Err(err).Msgf("failed to marshal storage ref: %+v", ref)
			continue
		}
		_, err = io.Copy(w, bytes.NewReader(v))
		if err != nil {
			log.Warn().Err(err).Msgf("failed to write storage ref: %+v", ref)
			return
		}
		_, _ = w.Write(lf)
		count++
	}
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
	coreAPI, err := api.Parse(r.URL.Query())
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	responses := make(chan index.FileInfoResponse)

	if err := h.FileAPI.ListFileInfo(ctx, coreAPI, responses); err != nil {
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
		if res.GetError() != nil {
			log.Warn().Err(res.GetError()).Msg("failed result")
			continue
		}
		v, err := protojson.Marshal(res.GetFileInfo())
		if err != nil {
			log.Warn().Err(err).Msg("failed to marshal file info")
			continue
		}
		_, err = io.Copy(w, bytes.NewReader(v))
		if err != nil {
			log.Warn().Err(err).Msg("failed to write file info")
			return
		}
		_, _ = w.Write(lf)
		count++
	}
}

func (h Handler) getFileInfoByFilename(w http.ResponseWriter, r *http.Request) {
	params := httprouter.ParamsFromContext(r.Context())
	filename := params.ByName("filename")

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	fileInfo, err := h.FileAPI.GetFileInfo(ctx, filename)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		log.Error().Err(err).Msgf("Failed to get file info: %s", filename)
		return
	}
	b, err := protojson.Marshal(fileInfo)
	if err != nil {
		log.Warn().Err(err).Msgf("Failed to marshal file info: %s", fileInfo)
		return
	}
	_, err = io.Copy(w, bytes.NewReader(b))
	if err != nil {
		log.Warn().Err(err).Msgf("Failed to write report")
	}
	_, _ = w.Write(lf)
}

func (h Handler) loadRecordByUrn(w http.ResponseWriter, r *http.Request) {
	params := httprouter.ParamsFromContext(r.Context())
	warcId := params.ByName("urn")

	record, err := h.WarcLoader.LoadById(r.Context(), warcId)
	if record != nil {
		defer record.Close()
	}
	if err != nil {
		err := fmt.Errorf("failed to load record '%s': %w", warcId, err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		log.Error().Err(err).Msg("")
		return
	}
	if record == nil {
		http.NotFound(w, r)
		return
	}
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

func (h Handler) createReport(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	coreAPI, err := api.Parse(r.Form)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	report, err := h.ReportAPI.CreateReport(ctx, coreAPI)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		log.Error().Err(err).Msg("Failed to generate report")
		return
	}
	b, err := protojson.Marshal(report)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		log.Error().Err(err).Msgf("Failed to marshal report: %v", report)
	}

	w.WriteHeader(http.StatusAccepted)
	_, err = io.Copy(w, bytes.NewReader(b))
	if err != nil {
		log.Warn().Err(err).Msgf("Failed to write report")
		return
	}
	_, _ = w.Write(lf)
}

func (h Handler) getReport(w http.ResponseWriter, r *http.Request) {
	params := httprouter.ParamsFromContext(r.Context())
	id := params.ByName("id")

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	report, err := h.ReportAPI.GetReport(ctx, id)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		log.Error().Err(err).Msgf("Failed to get report: %s", id)
		return
	}
	if report == nil {
		http.NotFound(w, r)
		return
	}

	b, err := protojson.Marshal(report)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		log.Error().Err(err).Msgf("Failed to marshal report: %v", report)
	}
	_, err = io.Copy(w, bytes.NewReader(b))
	if err != nil {
		log.Warn().Err(err).Msgf("Failed to write report")
		return
	}
	_, _ = w.Write(lf)
}

func (h Handler) listReports(w http.ResponseWriter, r *http.Request) {
	coreAPI, err := api.Parse(r.URL.Query())
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	responses := make(chan index.ReportResponse)

	if err := h.ReportAPI.ListReports(ctx, coreAPI, responses); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		log.Error().Err(err).Msg("Failed to list reports")
		return
	}

	start := time.Now()
	count := 0
	defer func() {
		log.Debug().Msgf("Found %d items in %s", count, time.Since(start))
	}()

	for res := range responses {
		if res.GetError() != nil {
			log.Warn().Err(res.GetError()).Msg("failed report result")
			continue
		}
		v, err := protojson.Marshal(res.GetReport())
		if err != nil {
			log.Warn().Err(err).Msg("failed to marshal report")
			continue
		}
		_, err = io.Copy(w, bytes.NewReader(v))
		if err != nil {
			log.Warn().Err(err).Msg("failed to write report")
			return
		}
		_, _ = w.Write(lf)
		count++
	}
}

func (h Handler) deleteReport(w http.ResponseWriter, r *http.Request) {
	params := httprouter.ParamsFromContext(r.Context())
	id := params.ByName("id")

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	err := h.ReportAPI.DeleteReport(ctx, id)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		log.Error().Err(err).Msgf("Failed to delete report: %s", id)
		return
	}
}

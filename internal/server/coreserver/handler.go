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
	"encoding/json"
	"fmt"
	"github.com/julienschmidt/httprouter"
	"github.com/nlnwa/gowarcserver/internal/index"
	"github.com/nlnwa/gowarcserver/internal/loader"
	"github.com/nlnwa/gowarcserver/internal/server/api"
	"github.com/nlnwa/gowarcserver/internal/server/handlers"
	"github.com/nlnwa/gowarcserver/schema"
	"github.com/rs/zerolog/log"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/dgraph-io/badger/v3"
)

type Handler struct {
	db     api.DbAdapter
	loader loader.RecordLoader
}

// keyHandler returns a http.HandlerFunc that outputs the keys given in db.
func keyHandler(db *badger.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		limit := parseLimit(r)

		start := time.Now()
		count := 0

		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		err := index.Walk(db, opts, func(item *badger.Item) (stopIteration bool) {
			_, err := fmt.Fprintln(w, string(item.Key()))
			count++
			if count >= limit {
				return true
			}
			return err != nil
		})
		if err != nil {
			api.HandleError(w, count, err)
		}
		if count > 0 {
			log.Debug().Msgf("Found %d items in %s", count, time.Since(start))
		}
	}
}

func (h Handler) listId(w http.ResponseWriter, r *http.Request) {
	keyHandler(h.db.IdIndex)(w, r)
}

func (h Handler) listFile(w http.ResponseWriter, r *http.Request) {
	keyHandler(h.db.FileIndex)(w, r)
}

func (h Handler) listCdx(w http.ResponseWriter, r *http.Request) {
	keyHandler(h.db.CdxIndex)(w, r)
}

func (h Handler) search(w http.ResponseWriter, r *http.Request) {
	coreAPI, err := api.Parse(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	start := time.Now()

	count, err := h.db.Search(coreAPI, func(cdx *schema.Cdx) error {
		b, err := protojson.Marshal(cdx)
		if err != nil {
			return err
		}
		_, err = fmt.Fprintln(w, string(b))
		return err
	})
	if err != nil {
		api.HandleError(w, count, err)
	}
	if count > 0 {
		log.Debug().Msgf("Found %d items in %s", count, time.Since(start))
	}
}

type storageRef struct {
	Id       string `json:"id"`
	Filename string `json:"filename"`
	Offset   int64  `json:"offset"`
}

func (h Handler) listIds(w http.ResponseWriter, r *http.Request) {
	limit := parseLimit(r)

	start := time.Now()
	count := 0

	opts := badger.DefaultIteratorOptions
	opts.PrefetchSize = limit
	err := index.Walk(h.db.IdIndex, opts, func(item *badger.Item) (stopIteration bool) {
		err := item.Value(func(val []byte) error {
			filename, offset, err := parseStorageRef(string(val))
			if err != nil {
				return err
			}
			storageRef := &storageRef{
				Id:       string(item.Key()),
				Filename: filename,
				Offset:   offset,
			}
			b, err := json.Marshal(storageRef)
			if err != nil {
				return err
			}
			_, err = fmt.Fprintln(w, string(b))
			return err
		})
		if err != nil {
			log.Warn().Err(err).Msgf("Failed to output id '%s'", string(item.Key()))
		}
		count++
		if count >= limit {
			return true
		}
		return err != nil
	})
	if err != nil {
		api.HandleError(w, count, err)
	} else if count > 0 {
		log.Debug().Msgf("Found %d items in %s", count, time.Since(start))
	}
}

func (h Handler) getStorageRefByURN(w http.ResponseWriter, r *http.Request) {
	params := httprouter.ParamsFromContext(r.Context())
	urn := params.ByName("urn")

	storageRef, err := h.db.Resolve(urn)
	if err != nil {
		msg := fmt.Sprintf("failed to resolve storage reference of urn: %v", err)
		http.Error(w, msg, http.StatusInternalServerError)
		return
	}
	_, err = fmt.Fprintln(w, storageRef)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (h Handler) listFiles(w http.ResponseWriter, r *http.Request) {
	limit := parseLimit(r)

	start := time.Now()
	count := 0

	opts := badger.DefaultIteratorOptions
	opts.PrefetchSize = limit

	err := index.Walk(h.db.FileIndex, opts, func(item *badger.Item) (stopIteration bool) {
		fileInfo := new(schema.Fileinfo)
		err := item.Value(func(val []byte) error {
			return proto.Unmarshal(val, fileInfo)
		})
		if err != nil {
			log.Warn().Err(err).Msgf("Failed to unmarshal FileInfo: %s", string(item.Key()))
		}
		b, err := protojson.Marshal(fileInfo)
		if err != nil {
			log.Warn().Err(err).Msgf("Failed to marshal fileInfo to JSON: %s", string(item.Key()))
		}
		_, err = fmt.Fprintln(w, string(b))
		if err != nil {
			log.Warn().Err(err).Msgf("Failed to write fileInfo: %s", string(item.Key()))
		}
		count++
		if count >= limit {
			return true
		}
		return err != nil
	})
	if err != nil {
		api.HandleError(w, count, err)
	} else if count > 0 {
		log.Debug().Msgf("Found %d items in %s", count, time.Since(start))
	}
}

func (h Handler) getFileInfoByFilename(w http.ResponseWriter, r *http.Request) {
	params := httprouter.ParamsFromContext(r.Context())
	filename := params.ByName("filename")

	fileInfo, err := h.db.GetFileInfo(filename)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	_, err = fmt.Fprintln(w, protojson.Format(fileInfo))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (h Handler) listCdxs(w http.ResponseWriter, r *http.Request) {
	limit := parseLimit(r)

	start := time.Now()
	count := 0

	opts := badger.DefaultIteratorOptions
	opts.PrefetchSize = limit

	err := index.Walk(h.db.CdxIndex, opts, func(item *badger.Item) (stopIteration bool) {
		cdx := new(schema.Cdx)
		err := item.Value(func(val []byte) error {
			return proto.Unmarshal(val, cdx)
		})
		if err != nil {
			log.Warn().Err(err).Msgf("Failed to unmarshal Cdx: %s", string(item.Key()))
		}
		b, err := protojson.Marshal(cdx)
		if err != nil {
			log.Warn().Err(err).Msgf("Failed to marshal JSON: %s", string(item.Key()))
		}
		_, err = fmt.Fprintln(w, string(b))
		if err != nil {
			log.Warn().Err(err).Msgf("Failed to write: %s", string(item.Key()))
		}
		count++
		if count >= limit {
			return true
		}
		return err != nil
	})
	if err != nil {
		api.HandleError(w, count, err)
	} else if count > 0 {
		log.Debug().Msgf("Found %d items in %s", count, time.Since(start))
	}
}

func (h Handler) loadRecordByUrn(w http.ResponseWriter, r *http.Request) {
	params := httprouter.ParamsFromContext(r.Context())
	warcId := params.ByName("urn")

	record, err := h.loader.Load(r.Context(), warcId)
	if err != nil {
		msg := fmt.Sprintf("failed to load record '%s': %v", warcId, err)
		http.Error(w, msg, http.StatusInternalServerError)
		return
	}
	defer record.Close()

	n, err := handlers.RenderRecord(w, record)
	if n == 0 {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
	if err != nil {
		log.Warn().Err(err).Msgf("Failed to write record: %v", record)
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

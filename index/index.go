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

package index

import (
	"errors"
	"path/filepath"
	"strings"
	"time"

	"github.com/nlnwa/gowarc"
	"github.com/rs/zerolog/log"
)

type Indexer interface {
	Index(string) error
}

func NewIndexer(w RecordWriter, options ...Option) func(string) {
	opts := new(Options)
	for _, apply := range options {
		apply(opts)
	}

	indexer, ok := w.(Indexer)

	return func(path string) {
		// file
		dir := filepath.Dir(path)
		base := filepath.Base(path)
		if opts.isExcluded(dir) || !opts.filter(base) {
			return
		}

		// if writer implements Index interface we call its index interface
		if ok {
			if err := indexer.Index(path); err != nil {
				if errors.Is(err, AlreadyIndexedError) {
					log.Debug().Err(err).Msgf("%s", path)
				} else if err != nil {
					log.Error().Err(err).Msgf("%s", path)
				}
				return
			}
		}

		index(path, w, opts)
	}
}

func index(filename string, r RecordWriter, opts *Options) {
	start := time.Now()

	filter := func(wr gowarc.WarcRecord, validation *gowarc.Validation) bool {
		if !validation.Valid() {
			log.Debug().Msg(validation.Error())
		}
		// only index response and revisit records of type application/http
		if wr.Type() == gowarc.Response || wr.Type() == gowarc.Revisit {
			// of type application/http
			if strings.HasPrefix(wr.WarcHeader().Get(gowarc.ContentType), gowarc.ApplicationHttp) {
				return true
			}
		}
		return false
	}

	count, total, err := readFile(filename, r, filter, opts.warcRecordOption...)
	if err != nil {
		log.Error().Err(err).Msgf("Indexing failed: %s", filename)
	}

	log.Info().Msgf("Indexed %5d of %5d records in %10v: %s\n", count, total, time.Since(start), filename)
}

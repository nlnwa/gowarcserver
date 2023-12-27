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

package badgeridx

import (
	"os"

	"github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/badger/v4/options"
	"github.com/rs/zerolog/log"
)

// badgerLogger is a log adapter that implements badger.Logger
type badgerLogger struct {
	prefix string
}

func (l badgerLogger) Errorf(fmt string, args ...interface{}) {
	log.Error().Msgf(l.prefix+fmt, args...)
}

func (l badgerLogger) Warningf(fmt string, args ...interface{}) {
	log.Warn().Msgf(l.prefix+fmt, args...)
}

func (l badgerLogger) Infof(fmt string, args ...interface{}) {
	log.Trace().Msgf(l.prefix+fmt, args...)
}

func (l badgerLogger) Debugf(fmt string, args ...interface{}) {
	log.Trace().Msgf(l.prefix+fmt, args...)
}

func newBadgerDB(dir string, compression options.CompressionType, readOnly bool, silent bool) (*badger.DB, error) {
	// create database directory if not exists
	if err := os.MkdirAll(dir, 0777); err != nil {
		return nil, err
	}
	var logger badger.Logger
	if !silent {
		logger = badgerLogger{prefix: "Badger: "}
	}
	opts := badger.DefaultOptions(dir).
		WithLogger(logger).
		WithCompression(compression).
		WithReadOnly(readOnly)

	if compression == options.ZSTD {
		opts.WithZSTDCompressionLevel(5)
	}

	return badger.Open(opts)
}

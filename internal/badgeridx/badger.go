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

// logger is a log adapter that implements badger.Logger
type logger struct {
	prefix string
}

func (l logger) Errorf(fmt string, args ...interface{}) {
	log.Error().Msgf(l.prefix+fmt, args...)
}

func (l logger) Warningf(fmt string, args ...interface{}) {
	log.Warn().Msgf(l.prefix+fmt, args...)
}

func (l logger) Infof(fmt string, args ...interface{}) {
	log.Trace().Msgf(l.prefix+fmt, args...)
}

func (l logger) Debugf(fmt string, args ...interface{}) {
	log.Trace().Msgf(l.prefix+fmt, args...)
}

func newBadgerDB(dir string, compression options.CompressionType, readOnly bool) (*badger.DB, error) {
	// create database directory if not exists
	if err := os.MkdirAll(dir, 0777); err != nil {
		return nil, err
	}
	opts := badger.DefaultOptions(dir).
		WithLogger(logger{prefix: "Badger: "}).
		WithCompression(compression).
		WithReadOnly(readOnly)

	if compression == options.ZSTD {
		opts.WithZSTDCompressionLevel(5)
	}

	return badger.Open(opts)
}

type PerItemFunc func(*badger.Item) (stopIteration bool)
type AfterIterFunc func(txn *badger.Txn) error

// walk iterates db using iterator opts and processes items with fn.
func walk(db *badger.DB, opts badger.IteratorOptions, fn PerItemFunc) error {
	return db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(opts)
		defer it.Close()
		it.Rewind()
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			if fn(item) {
				break
			}
		}
		return nil
	})
}

// Get value stored at key from db.
func Get(db *badger.DB, key []byte) (value []byte, err error) {
	err = db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}
		value, err = item.ValueCopy(nil)
		return err
	})
	return
}

// Search iterates over keys in db prefixed with key and applies PerItemFunc f to each item value.
func Search(db *badger.DB, key string, reverse bool, f PerItemFunc, a AfterIterFunc) error {
	return db.View(func(txn *badger.Txn) error {
		prefix := []byte(key)
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		opts.Prefix = prefix
		opts.Reverse = reverse

		it := txn.NewIterator(opts)
		defer it.Close()

		// see https://github.com/dgraph-io/badger/issues/436 for details regarding reverse seeking
		seekKey := key
		if reverse {
			seekKey += string(rune(0xff))
		}

		for it.Seek([]byte(seekKey)); it.ValidForPrefix(prefix); it.Next() {
			if f(it.Item()) {
				break
			}
		}
		return a(txn)
	})
}

/*
 * Copyright 2022 National Library of Norway.
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
	"time"

	badgerOptions "github.com/dgraph-io/badger/v4/options"
	"github.com/nlnwa/gowarcserver/index"
)

func defaultDbOptions() *Options {
	return &Options{
		Compression:  badgerOptions.Snappy,
		BatchMaxSize: 10000,
		BatchMaxWait: 5 * time.Second,
		GcInterval:   15 * time.Second,
		Path:         ".",
	}
}

type Options struct {
	Compression  badgerOptions.CompressionType
	BatchMaxSize int
	BatchMaxWait time.Duration
	GcInterval   time.Duration
	Path         string
	ReadOnly     bool
	Database     string
	Index        index.Indexer
	Silent       bool
}

type Option func(opts *Options)

func WithCompression(c string) Option {
	return func(opts *Options) {
		compression, err := parseCompression(c)
		if err != nil {
			panic(err)
		}
		opts.Compression = compression
	}
}

func WithDir(d string) Option {
	return func(opts *Options) {
		opts.Path = d
	}
}

func WithBatchMaxSize(size int) Option {
	return func(opts *Options) {
		opts.BatchMaxSize = size
	}
}

func WithBatchMaxWait(t time.Duration) Option {
	return func(opts *Options) {
		opts.BatchMaxWait = t
	}
}

func WithGcInterval(t time.Duration) Option {
	return func(opts *Options) {
		opts.GcInterval = t
	}
}

func WithDatabase(db string) Option {
	return func(opts *Options) {
		opts.Database = db
	}
}

func WithReadOnly(readOnly bool) Option {
	return func(opts *Options) {
		opts.ReadOnly = readOnly
	}
}

func WithIndexer(indexer index.Indexer) Option {
	return func(opts *Options) {
		opts.Index = indexer
	}
}

func WithoutBadgerLogging() Option {
	return func(opts *Options) {
		opts.Silent = true
	}
}

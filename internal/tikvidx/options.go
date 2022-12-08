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

package tikvidx

import (
	"time"
)

func defaultOptions() *Options {
	return &Options{
		BatchMaxSize:    255,
		BatchMaxWait:    5 * time.Second,
		BatchMaxRetries: 3,
	}
}

type Options struct {
	BatchMaxSize    int
	BatchMaxWait    time.Duration
	BatchMaxRetries int
	ReadOnly        bool
	PdAddr          []string
	Database        string
}

type Option func(opts *Options)

func WithPDAddress(pdAddr []string) Option {
	return func(opts *Options) {
		opts.PdAddr = pdAddr
	}
}

func WithReadOnly(readOnly bool) Option {
	return func(opts *Options) {
		opts.ReadOnly = readOnly
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

func WithDatabase(db string) Option {
	return func(opts *Options) {
		opts.Database = db
	}
}

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
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"

	"github.com/rs/zerolog/log"
)

type AutoIndexOptions struct {
	MaxDepth int
	Paths    []string
	Options
}
type AutoIndexOption func(*AutoIndexOptions)

func WithMaxDepth(depth int) AutoIndexOption {
	return func(opts *AutoIndexOptions) {
		opts.MaxDepth = depth
	}
}

func WithPaths(paths []string) AutoIndexOption {
	return func(opts *AutoIndexOptions) {
		opts.Paths = paths
	}
}

func WithExcludeDirs(res ...*regexp.Regexp) AutoIndexOption {
	return func(opts *AutoIndexOptions) {
		opts.Excludes = res
	}
}

type Queue interface {
	Add(path string)
}

type AutoIndexer struct {
	Queue
	opts *AutoIndexOptions
}

func NewAutoIndexer(s Queue, options ...AutoIndexOption) AutoIndexer {
	opts := new(AutoIndexOptions)
	for _, apply := range options {
		apply(opts)
	}

	return AutoIndexer{
		Queue: s,
		opts:  opts,
	}
}

func (a AutoIndexer) Run(ctx context.Context) error {
	for _, path := range a.opts.Paths {
		err := a.index(ctx.Done(), path)
		if err != nil {
			log.Warn().Msgf(`Error indexing "%s": %v`, path, err)
		}
	}
	return nil
}

func (a AutoIndexer) index(done <-chan struct{}, path string) error {
	select {
	case <-done:
		return nil
	default:
	}
	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("failed to get file info: %w", err)
	}
	if info.IsDir() {
		if err := a.walk(done, path, 0); err != nil {
			return err
		}
	} else {
		a.Add(path)
	}
	return nil
}

func (a AutoIndexer) walk(done <-chan struct{}, dir string, currentDepth int) error {
	if a.opts.isExcluded(dir) {
		return nil
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return fmt.Errorf(`failed to read directory "%s": %w`, dir, err)
	}
	for _, entry := range entries {
		select {
		case <-done:
			return nil
		default:
		}
		path := filepath.Join(dir, entry.Name())
		if !entry.IsDir() {
			a.Queue.Add(path)
		} else if currentDepth < a.opts.MaxDepth {
			err = a.walk(done, path, currentDepth+1)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

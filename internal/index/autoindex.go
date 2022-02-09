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
	"fmt"
	"os"
	"path/filepath"
	"time"
)

type autoIndexer struct {
	watcher *watcher
}

type Scheduler interface {
	Schedule(job string, batchWindow time.Duration)
}

func NewAutoIndexer(s Scheduler, paths []string, opts ...Option) (*autoIndexer, error) {
	settings := defaultOptions()
	for _, opt := range opts {
		opt(settings)
	}

	a := new(autoIndexer)

	perDirFn := func(name string) bool {
		return !settings.isExcluded(name)
	}

	perFileFn := func(name string) {
		if settings.filter(name) {
			s.Schedule(name, 0)
		}
	}

	if settings.Watch {
		w, err := newWatcher()
		if err != nil {
			return nil, err
		}
		a.watcher = w

		perDirFn = func(name string) bool {
			if settings.isExcluded(name) {
				return false
			}
			_ = w.Add(name)
			return true
		}

		onFileChanged := func(name string) {
			if settings.filter(name) {
				s.Schedule(name, 10*time.Second)
			}
		}
		go w.Watch(onFileChanged)
	}

	for _, path := range paths {
		_ = index(path, settings.MaxDepth, perFileFn, perDirFn)
	}

	return a, nil
}

func (a *autoIndexer) Close() {
	a.watcher.Close()
}

func index(path string, maxDepth int, perFileFn func(string), perDirFn func(string) bool) error {
	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf(`: %s: %w`, path, err)
	}
	if info.IsDir() {
		walk(path, 0, maxDepth, perFileFn, perDirFn)
	} else {
		perFileFn(path)
	}
	return nil
}

func walk(dir string, currentDepth int, maxDepth int, perFileFn func(string), perDirFn func(string) bool) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	for _, entry := range entries {
		name := filepath.Join(dir, entry.Name())
		if !entry.IsDir() {
			perFileFn(name)
		} else if currentDepth < maxDepth {
			if perDirFn(name) {
				walk(name, currentDepth+1, maxDepth, perFileFn, perDirFn)
			}
		}
	}
}

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

type autoindexer struct {
	watcher *watcher
}

type Worker func(job string, batchWindow time.Duration)

func NewAutoIndexer(work Worker, paths []string, opts ...Option) (*autoindexer, error) {
	settings := defaultOptions()
	for _, opt := range opts {
		opt(settings)
	}

	a := new(autoindexer)

	// default is noop
	perDirFn := func(dir string) {}

	perFileFn := func(file string) {
		if !settings.isWhitelisted(file) {
			return
		}
		work(file, 0)
	}

	if settings.Watch {
		w, err := newWatcher()
		if err != nil {
			return nil, err
		}
		a.watcher = w

		perDirFn = func(dir string) {
			_ = w.Add(dir)
		}

		onFileChanged := func(file string) {
			if !settings.isWhitelisted(file) {
				return
			}
			work(file, 10*time.Second)
		}
		go w.Watch(onFileChanged)
	}

	for _, path := range paths {
		_ = index(path, settings.MaxDepth, perDirFn, perFileFn)
	}

	return a, nil
}

func (a *autoindexer) Close() {
	a.watcher.Close()
}

func index(path string, maxDepth int, perDirFn func(string), perFileFn func(string)) error {
	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf(`: %s: %w`, path, err)
	}
	if info.IsDir() {
		walk(path, 0, maxDepth, perDirFn, perFileFn)
	} else {
		perFileFn(path)
	}
	return nil
}

func walk(dir string, currentDepth int, maxDepth int, perDirFn func(string), perFileFn func(string)) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	for _, entry := range entries {
		if !entry.IsDir() {
			perFileFn(filepath.Join(dir, entry.Name()))
		} else if currentDepth < maxDepth {
			walk(filepath.Join(dir, entry.Name()), currentDepth+1, maxDepth, perDirFn, perFileFn)
		}
	}
	perDirFn(dir)
}

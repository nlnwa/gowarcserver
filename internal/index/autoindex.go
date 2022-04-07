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
	done      chan struct{}
	perFileFn func(string)
	perDirFn  func(string) bool
	watcher   *watcher
	settings  *Options
}

type Scheduler interface {
	Schedule(job string, batchWindow time.Duration)
}

func NewAutoIndexer(s Scheduler, opts ...Option) (*autoIndexer, error) {
	a := new(autoIndexer)

	settings := defaultOptions()
	for _, opt := range opts {
		opt(settings)
	}
	a.settings = settings

	done := make(chan struct{})
	a.done = done

	isDone := func() bool {
		select {
		case <-done:
			return true
		default:
			return false
		}
	}

	perDirFn := func(name string) bool {
		if isDone() {
			return false
		}
		return !settings.isExcluded(name)
	}

	perFileFn := func(name string) {
		if isDone() {
			return
		}
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
			if isDone() {
				return false
			}
			if settings.isExcluded(name) {
				return false
			}
			_ = w.Add(name)
			return true
		}

		onFileChanged := func(name string) {
			if isDone() {
				return
			}
			if settings.filter(name) {
				s.Schedule(name, 10*time.Second)
			}
		}
		go w.Watch(onFileChanged)
	}
	a.perFileFn = perFileFn
	a.perDirFn = perDirFn

	return a, nil
}

func (a *autoIndexer) Index(path string) error {
	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("failed to get file info: %w", err)
	}
	if info.IsDir() {
		if err := walk(path, 0, a.settings.MaxDepth, a.perFileFn, a.perDirFn); err != nil {
			return err
		}
	} else {
		a.perFileFn(path)
	}
	return nil
}

func (a *autoIndexer) Close() {
	close(a.done)
	a.watcher.Close()
}

func walk(dir string, currentDepth int, maxDepth int, perFileFn func(string), perDirFn func(string) bool) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return fmt.Errorf(`failed to read directory "%s": %w`, dir, err)
	}
	for _, entry := range entries {
		name := filepath.Join(dir, entry.Name())
		if !entry.IsDir() {
			perFileFn(name)
		} else if currentDepth < maxDepth {
			if perDirFn(name) {
				err = walk(name, currentDepth+1, maxDepth, perFileFn, perDirFn)
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}

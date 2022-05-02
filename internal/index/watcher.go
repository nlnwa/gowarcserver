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
	"github.com/fsnotify/fsnotify"
	"github.com/rs/zerolog/log"
	"os"
)

func newWatcher() (*watcher, error) {
	w, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, err
	}
	return &watcher{
		Watcher: w,
	}, nil
}

type watcher struct {
	*fsnotify.Watcher
}

func (w *watcher) Close() {
	if w == nil {
		return
	}
	_ = w.Watcher.Close()
}

func (w *watcher) Add(dir string) error {
	if w == nil {
		return nil
	}
	return w.Watcher.Add(dir)
}

func (w *watcher) Watch(perFileFn func(string)) {
	for {
		select {
		case event, ok := <-w.Events:
			if !ok {
				return
			}
			if event.Op&fsnotify.Write == fsnotify.Write {
				perFileFn(event.Name)
			} else if event.Op&fsnotify.Create == fsnotify.Create {
				if info, err := os.Stat(event.Name); err != nil {
					log.Warn().Msgf("Watcher failed to stat new file event: %v: %v", event.Name, err)
					continue
				} else if !info.Mode().IsDir() {
					continue
				}

				if err := w.Add(event.Name); err != nil {
					log.Warn().Msgf("Watcher failed to add new directory: %s, %v", event.Name, err)
				}
			}
		case err, ok := <-w.Errors:
			if !ok {
				return
			}
			log.Warn().Msgf("Watcher error: %v", err)
		}
	}
}

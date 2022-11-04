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
	"sync"
	"time"

	"github.com/rs/zerolog/log"
)

type indexWorker struct {
	jobs   chan string
	done   chan struct{}
	jobMap map[string]*time.Timer
	mx     sync.Mutex
	wg     sync.WaitGroup
}

// Indexer is the interface that wraps the Index method
type Indexer interface {
	Index(string) error
}

func NewWorker(a Indexer, nrOfWorkers int) *indexWorker {
	iw := &indexWorker{
		jobs:   make(chan string, nrOfWorkers),
		done:   make(chan struct{}),
		jobMap: map[string]*time.Timer{},
	}

	for i := 0; i < nrOfWorkers; i++ {
		go func() {
			for {
				select {
				case job := <-iw.jobs:
					if err := a.Index(job); err != nil {
						log.Error().Err(err).Msgf("Index job: %s", job)
					}
					iw.wg.Done()
					iw.mx.Lock()
					delete(iw.jobMap, job)
					iw.mx.Unlock()
				case <-iw.done:
					return
				}
			}
		}()
	}

	return iw
}

func (iw *indexWorker) Close() {
	// Wait for all timers to complete
	iw.wg.Wait()
	// before closing workers.
	close(iw.done)
}

func (iw *indexWorker) Schedule(job string, batchWindow time.Duration) {
	iw.mx.Lock()
	timer, ok := iw.jobMap[job]
	iw.mx.Unlock()

	if ok {
		timer.Stop()
		timer.Reset(batchWindow)
	} else {
		iw.wg.Add(1)
		if batchWindow == 0 {
			iw.jobs <- job
		} else {
			iw.jobMap[job] = time.AfterFunc(batchWindow, func() {
				iw.jobs <- job
			})
		}
	}
}

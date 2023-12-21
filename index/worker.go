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
)

type WorkQueue struct {
	queue chan string
	wg    sync.WaitGroup
}

type Worker func(string)

func NewWorkQueue(execute Worker, concurrency int) *WorkQueue {
	iw := &WorkQueue{
		queue: make(chan string, concurrency),
	}

	for i := 0; i < concurrency; i++ {
		iw.wg.Add(1)
		go func() {
			defer iw.wg.Done()
			for job := range iw.queue {
				execute(job)
			}
		}()
	}

	return iw
}

func (iw *WorkQueue) Close() {
	// close the queue
	close(iw.queue)
	// and wait for it to drain
	iw.wg.Wait()
}

// Add job to work queue
func (iw *WorkQueue) Add(job string) {
	iw.queue <- job
}

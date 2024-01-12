/*
 * Copyright 2023 National Library of Norway.
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
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestWorkQueue(t *testing.T) {
	concurrency := 10000
	jobs := 1000000
	executed := new(atomic.Int32)

	var m sync.Mutex
	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	getTimeout := func() time.Duration {
		m.Lock()
		defer m.Unlock()
		return time.Duration(r.Intn(10)) * time.Millisecond
	}

	perJobFn := func(path string) {
		time.Sleep(getTimeout())
		executed.Add(1)
	}

	queue := NewWorkQueue(perJobFn, concurrency)
	for i := 0; i < jobs; i++ {
		queue.Add(strconv.Itoa(i))
	}
	queue.Close()
	queue.Wait()

	queueLength := len(queue.queue)
	if queueLength != 0 {
		t.Errorf("expected queue to be empty, but got %d jobs", queueLength)
	}
	if executed.Load() != int32(jobs) {
		t.Errorf("expected %d jobs to have been executed, but got %d", jobs, executed.Load())
	}
}

func TestAddToClosedWorkQueue(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("expected panic")
		}
	}()
	queue := NewWorkQueue(func(string) {}, 1)
	queue.Close()
	queue.Add("this should panic")
}

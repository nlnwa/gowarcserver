/*
 * Copyright 2020 National Library of Norway.
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
	"github.com/nlnwa/gowarcserver/internal/index"
	"github.com/nlnwa/gowarcserver/internal/timestamp"
	"github.com/rs/zerolog/log"
)

func CompareClosest(ts int64) func(int64, int64) bool {
	return func(ts1 int64, ts2 int64) bool {
		return timestamp.AbsInt64(ts-ts1) < timestamp.AbsInt64(ts-ts2)
	}
}

func CompareAsc(a int64, b int64) bool {
	return a <= b
}

func CompareDesc(a int64, b int64) bool {
	return a > b
}

// mergeSort sorts elements of input channels to output channel according to sort direction.
func mergeSort(done <-chan struct{}, cmp func(*index.CdxResponse, *index.CdxResponse) bool, in ...chan index.CdxResponse) <-chan index.CdxResponse {
	out := make(chan index.CdxResponse)
	cords := make([]*index.CdxResponse, len(in))

	go func() {
		var zombie []int
		for {
			var curr *index.CdxResponse
			for i, cord := range cords {
				if cord == nil {
					v, ok := <-in[i]
					if !ok {
						// closed channel becomes a zombie
						zombie = append(zombie, i)
						continue
					}
					cord = &v
				}
				if cmp(curr, cord) {
					curr = cord
				}
			}
			if curr == nil {
				return
			}
			select {
			case out <- *curr:
			case <-done:
				return
			}
			if len(zombie) > 0 {
				// kill zombies to avoid checking on them every round
				for _, i := range zombie {
					cords[i] = cords[len(cords)-1]
					cords = cords[:len(cords)-1]
					in[i] = in[len(in)-1]
					in = in[:len(in)-1]
				}
				zombie = nil
			}
		}
	}()

	return out
}

// mergeSort merges sorted input channels to a sorted output channel
//
// Sorting is done by comparing keys from key-value pairs.
//
// The input channels are closed externally
func mergeIter(done <-chan struct{}, cmp func(KV, KV) bool, in ...chan *maybeKV) <-chan maybeKV {
	out := make(chan maybeKV)
	cords := make([]*maybeKV, len(in))
	go func() {
		defer close(out)
		var zombie []int
		for {
			curr := -1
			for i, cord := range cords {
				if cord == nil {
					select {
					case cord = <-in[i]:
						cords[i] = cord
					case <-done:
						return
					}
					// closed channel becomes zombie
					if cord == nil {
						zombie = append(zombie, i)
						continue
					}
				}
				if cord.error != nil {
					log.Error().Err(cord.error).Msg("cord error")
					// prioritize errors
					curr = i
					break
				}
				if curr == -1 {
					curr = i
				} else if cmp(cords[i].kv, cord.kv) {
					curr = i
				}
			}
			if curr == -1 {
				return
			}
			select {
			case <-done:
				return
			case out <- *cords[curr]:
				cords[curr] = nil
				curr = -1
			}
			// if zombie, then kill
			if len(zombie) > 0 {
				for _, i := range zombie {
					cords[i] = cords[len(cords)-1]
					cords = cords[:len(cords)-1]
					in[i] = in[len(in)-1]
					in = in[:len(in)-1]
				}
				zombie = nil
			}
		}
	}()

	return out
}

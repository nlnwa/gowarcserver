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

package badgeridx

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v3"
	"github.com/nlnwa/gowarcserver/index"
	"github.com/nlnwa/gowarcserver/schema"
	"github.com/nlnwa/gowarcserver/timestamp"
	"github.com/rs/zerolog/log"
	"google.golang.org/protobuf/proto"
)

type PerCdxFunc func(cdx *schema.Cdx) error

type cdxKey string

func (k cdxKey) ts() string {
	return strings.Split(string(k), " ")[1]
}

func cdxFromItem(item *badger.Item) (cdx *schema.Cdx, err error) {
	err = item.Value(func(val []byte) error {
		result := new(schema.Cdx)
		if err := proto.Unmarshal(val, result); err != nil {
			return err
		}
		cdx = result
		return nil
	})
	return
}

func (db *DB) List(ctx context.Context, limit int, results chan<- index.CdxResponse) error {
	go func() {
		_ = db.CdxIndex.View(func(txn *badger.Txn) error {
			opts := badger.DefaultIteratorOptions
			opts.PrefetchSize = limit
			iter := txn.NewIterator(opts)
			defer iter.Close()
			defer close(results)

			count := 0
			for iter.Seek(nil); iter.Valid(); iter.Next() {
				select {
				case <-ctx.Done():
					results <- index.CdxResponse{Error: ctx.Err()}
					return nil
				default:
				}

				if count >= limit {
					return nil
				}
				count++

				cdx, err := cdxFromItem(iter.Item())
				if err != nil {
					results <- index.CdxResponse{Error: err}
					return nil
				}
				results <- index.CdxResponse{Cdx: cdx, Error: nil}
			}
			return nil
		})
	}()
	return nil
}

// Closest returns the first closest cdx value
func (db *DB) Closest(_ context.Context, request index.Request, results chan<- index.CdxResponse) error {
	go func() {
		_ = db.CdxIndex.View(func(txn *badger.Txn) error {
			defer close(results)

			key := request.Key()
			closest := request.Closest()

			// prefix
			prefix := []byte(key)
			// forward seek key
			fk := []byte(key + closest)
			// backward seek key
			bk := []byte(key + closest + string(rune(0xff)))

			opts := badger.DefaultIteratorOptions
			opts.PrefetchSize = 1
			opts.Prefix = prefix

			forward := txn.NewIterator(opts)
			defer forward.Close()
			forward.Seek(fk)

			// check if we got a literal match on forward seek key (fast path)
			if forward.ValidForPrefix(fk) {
				cdx, err := cdxFromItem(forward.Item())
				if err == nil {
					results <- index.CdxResponse{Cdx: cdx}
					return nil
				}
				// report error and move iterator forward
				results <- index.CdxResponse{Error: err}
				forward.Next()
			}

			opts.Reverse = true
			backward := txn.NewIterator(opts)
			defer backward.Close()
			backward.Seek(bk)

			t, _ := timestamp.Parse(closest)
			cl := t.Unix()

			for {
				var ft int64
				var bt int64

				// get forward ts
				if forward.ValidForPrefix(prefix) {
					t, _ = timestamp.Parse(cdxKey(forward.Item().Key()).ts())
					ft = t.Unix()
				}
				// get backward ts
				if backward.ValidForPrefix(prefix) {
					t, _ = timestamp.Parse(cdxKey(backward.Item().Key()).ts())
					bt = t.Unix()
				}

				var iter *badger.Iterator

				if ft != 0 && bt != 0 {
					// find closest of forward and backward
					isForward := timestamp.AbsInt64(cl-ft) < timestamp.AbsInt64(cl-bt)
					if isForward {
						iter = forward
					} else {
						iter = backward
					}
				} else if ft != 0 {
					iter = forward
				} else if bt != 0 {
					iter = backward
				} else {
					// found nothing
					return nil
				}
				cdx, err := cdxFromItem(iter.Item())
				if err == nil {
					results <- index.CdxResponse{Cdx: cdx}
					return nil
				}
				// report error and move iterator forward
				results <- index.CdxResponse{Error: err}
				iter.Next()
			}
		})
	}()
	return nil
}

func (db *DB) Search(ctx context.Context, search index.Request, results chan<- index.CdxResponse) error {
	keyLen := len(search.Keys())

	if keyLen == 0 {
		return errors.New("search request is missing keys")
	} else if keyLen == 1 {
		if search.Sort() == index.SortClosest {
			return db.closestUniSearch(ctx, search, results)
		}
		return db.uniSearch(ctx, search, results)
	} else {
		if search.Sort() == index.SortNone {
			return db.unsortedSerialSearch(ctx, search, results)
		}
		return db.sortedParallelSearch(ctx, search, results)
	}
}

// unsortedParallelSearch searches the index database, sorts the results and processes each result with perCdxFunc.
func (db *DB) sortedParallelSearch(ctx context.Context, search index.Request, results chan<- index.CdxResponse) error {
	count := 0
	perItemFn := func(item *badger.Item) error {
		err := item.Value(func(val []byte) error {
			result := new(schema.Cdx)
			err := proto.Unmarshal(val, result)
			if err != nil {
				return err
			}

			// filter (exact, contains, regexp)
			if search.Filter().Eval(result) {
				count++
				results <- index.CdxResponse{Cdx: result, Error: nil}
			}
			return nil
		})
		if err != nil {
			return fmt.Errorf("%s: %w", item.KeyCopy(nil), err)
		}
		return nil
	}

	var closest int64
	if search.Closest() != "" {
		ts, err := timestamp.Parse(search.Closest())
		if err != nil {
			return err
		}
		closest = ts.Unix()
	}
	go func() {
		defer close(results)
		sorter := NewSorter(closest, search.Sort() == index.SortAsc)
		_ = db.CdxIndex.View(func(txn *badger.Txn) error {
			items := make(chan []byte, len(search.Keys()))

			done := make(chan struct{})

			go func() {
				for key := range items {
					sorter.Add(key)
				}
				sorter.Sort()
				done <- struct{}{}
			}()

			// wg is used to synchronize multiple transaction iterators operating simultaneously.
			var wg sync.WaitGroup

			for _, key := range search.Keys() {
				wg.Add(1)
				key := key

				go func() {
					defer wg.Done()
					opts := badger.DefaultIteratorOptions
					opts.PrefetchValues = false
					opts.Prefix = []byte(key)

					it := txn.NewIterator(opts)
					defer it.Close()

					for it.Seek([]byte(key)); it.ValidForPrefix([]byte(key)); it.Next() {
						select {
						case <-ctx.Done():
							results <- index.CdxResponse{Error: ctx.Err()}
							return
						default:
						}

						k := it.Item().KeyCopy(nil)

						// filter from/to
						inDateRange, _ := search.DateRange().ContainsStr(cdxKey(k).ts())
						if inDateRange {
							items <- k
						}
					}
				}()
			}
			wg.Wait()
			close(items)
			<-done

			return sorter.Walk(txn, func(item *badger.Item) (stopIteration bool) {
				if err := perItemFn(item); err != nil {
					return true
				}
				return false
			})
		})
	}()
	return nil
}

func (db *DB) unsortedSerialSearch(ctx context.Context, search index.Request, results chan<- index.CdxResponse) error {
	go func() {
		defer close(results)
		_ = db.CdxIndex.View(func(txn *badger.Txn) error {
			count := 0
			// initialize badger iterators
			keyLen := len(search.Keys())
			iterators := make([]*badger.Iterator, keyLen)
			prefixes := make([][]byte, keyLen)
			for i, key := range search.Keys() {
				prefixes[i] = []byte(key)
				opts := badger.DefaultIteratorOptions
				opts.Prefix = prefixes[i]

				iterators[i] = txn.NewIterator(opts)
				defer iterators[i].Close()

				iterators[i].Seek(prefixes[i])
			}

		OUTER:
			for len(iterators) > 0 {
				// set timestamp to approx max time.Time value
				earliestTimestamp := time.Unix(1<<62, 1<<62)
				earliestIndex := -1
				// find the earliest timestamp
				for i, iter := range iterators {

					select {
					case <-ctx.Done():
						results <- index.CdxResponse{Error: ctx.Err()}
						return nil
					default:
					}

					// if iter is no longer valid, close it, remove it from slice and restart search
					if !iter.ValidForPrefix(prefixes[i]) {
						iteratorsLen := len(iterators)
						iterators[i].Close()
						// remove iterator from list
						iterators[i] = iterators[iteratorsLen-1]
						iterators = iterators[0 : iteratorsLen-1]
						continue OUTER
					}

					item := iter.Item()
					// in the event of parse error we get a zero timestamp
					ts, err := time.Parse(timestamp.CDX, cdxKey(item.Key()).ts())
					if err != nil {
						log.Warn().Err(err).Msgf("Failed to parse timestamp for key: '%s'", string(item.Key()))

						// timestamp is invalid, iterate to next and restart search
						iter.Next()
						continue OUTER
					}

					inRange, _ := search.DateRange().ContainsTime(ts)
					if !inRange {
						// timestamp out of range, iterate to next item and restart search
						iter.Next()
						continue OUTER
					}

					if ts.Before(earliestTimestamp) {
						earliestTimestamp = ts
						earliestIndex = i
					}
				}
				if earliestIndex == -1 {
					break
				}
				iter := iterators[earliestIndex]

				cdx, err := cdxFromItem(iter.Item())
				if err != nil {
					return err
				}

				iter.Next()

				if search.Filter().Eval(cdx) {
					count++
					results <- index.CdxResponse{Cdx: cdx, Error: nil}
				}

				if search.Limit() > 0 && count >= search.Limit() {
					break
				}
			}
			return nil
		})
	}()
	return nil
}

func (db *DB) closestUniSearch(ctx context.Context, search index.Request, results chan<- index.CdxResponse) error {
	// key len is guaranteed to be 1 at this point
	key := search.Keys()[0]
	seek := key + search.Closest()
	ts, err := timestamp.Parse(search.Closest())
	if err != nil {
		return err
	}

	closest := ts.Unix()
	isClosest := func(a int64, b int64) bool {
		return timestamp.AbsInt64(closest-a) <= timestamp.AbsInt64(closest-b)
	}

	count := 0

	go func() {
		_ = db.CdxIndex.View(func(txn *badger.Txn) error {
			defer close(results)
			prefix := []byte(key)

			opts := badger.DefaultIteratorOptions
			opts.Prefix = prefix

			forward := txn.NewIterator(opts)
			defer forward.Close()

			opts.Reverse = true
			backward := txn.NewIterator(opts)
			defer backward.Close()

			fk := []byte(seek)
			forward.Seek(fk)

			bk := []byte(seek + string(rune(0xff)))
			backward.Seek(bk)
			if forward.ValidForPrefix(prefix) && backward.ValidForPrefix(prefix) && bytes.Equal(forward.Item().Key(), backward.Item().Key()) {
				// if forward and backward iterator point to same item we advance the backward iterator
				backward.Next()
			}

			var ft int64
			var bt int64

			f := true
			b := true

			for {
				select {
				case <-ctx.Done():
					results <- index.CdxResponse{Error: ctx.Err()}
					return nil
				default:
				}

				if f && forward.ValidForPrefix(prefix) {
					t, _ := timestamp.Parse(cdxKey(forward.Item().Key()).ts())
					ft = t.Unix()
				} else if f {
					f = false
					ft = 0
				}

				if b && backward.ValidForPrefix(prefix) {
					t, _ := timestamp.Parse(cdxKey(backward.Item().Key()).ts())
					bt = t.Unix()
				} else if b {
					b = false
					bt = 0
				}

				var it *badger.Iterator
				if f && isClosest(ft, bt) {
					it = forward
				} else if b {
					it = backward
				} else {
					return nil
				}

				cdx, err := cdxFromItem(it.Item())
				if err != nil {
					results <- index.CdxResponse{Error: err}
					return nil
				}
				it.Next()

				results <- index.CdxResponse{Cdx: cdx, Error: nil}
				count++

				if search.Limit() > 0 && count >= search.Limit() {
					break
				}
			}
			return nil
		})
	}()
	return nil
}

// uniSearch the index database and render each item with the provided renderFunc.
func (db *DB) uniSearch(ctx context.Context, search index.Request, results chan<- index.CdxResponse) error {
	go func() {
		_ = db.CdxIndex.View(func(txn *badger.Txn) error {
			reverse := search.Sort() == index.SortAsc
			key := search.Keys()[0]
			count := 0
			prefix := []byte(key)
			opts := badger.DefaultIteratorOptions
			opts.PrefetchValues = false
			opts.Prefix = prefix
			opts.Reverse = reverse

			it := txn.NewIterator(opts)
			defer it.Close()
			defer close(results)

			// see https://github.com/dgraph-io/badger/issues/436 for details regarding reverse seeking
			seekKey := key
			if reverse {
				seekKey += string(rune(0xff))
			}

			for it.Seek([]byte(seekKey)); it.ValidForPrefix(prefix); it.Next() {
				cdxResponse := func() (cdxResponse index.CdxResponse) {
					if contains, err := search.DateRange().ContainsStr(cdxKey(it.Item().Key()).ts()); err != nil {
						cdxResponse.Error = err
						return
					} else if !contains {
						return
					}
					if err := it.Item().Value(func(v []byte) error {
						result := new(schema.Cdx)
						if err := proto.Unmarshal(v, result); err != nil {
							cdxResponse.Error = err
						} else if search.Filter().Eval(result) {
							cdxResponse.Cdx = result
						}
						return nil
					}); err != nil {
						cdxResponse.Error = err
					}
					return
				}()
				// skip if empty response
				if cdxResponse == (index.CdxResponse{}) {
					continue
				}
				// send result
				select {
				case <-ctx.Done():
					results <- index.CdxResponse{Error: ctx.Err()}
					return nil
				case results <- cdxResponse:
					if cdxResponse.Error == nil {
						count++
					}
				}
				// stop iteration if limit is reached
				if search.Limit() > 0 && count >= search.Limit() {
					break
				}
			}
			return nil
		})
	}()
	return nil
}

func (db *DB) ListRecords(fn func(warcId string, cdx *schema.Cdx) (stopIteration bool)) error {
	opts := badger.DefaultIteratorOptions
	return walk(db.CdxIndex, opts, func(item *badger.Item) (stopIteration bool) {
		err := item.Value(func(val []byte) error {
			cdx := new(schema.Cdx)
			err := proto.Unmarshal(val, cdx)
			if err != nil {
				return err
			}
			stopIteration = fn(string(item.Key()), cdx)
			return nil
		})
		if err != nil {
			log.Error().Err(err).Msgf("failed get value for key: %s", string(item.Key()))
		}
		return stopIteration || err != nil
	})
}

func (db *DB) GetStorageRef(ctx context.Context, warcId string) (string, error) {
	var storageRef string
	err := db.IdIndex.View(func(txn *badger.Txn) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		item, err := txn.Get([]byte(warcId))
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			storageRef = string(val)
			return nil
		})
	})
	return storageRef, err
}

func (db *DB) ListStorageRef(ctx context.Context, limit int, results chan<- index.IdResponse) error {
	go func() {
		_ = db.IdIndex.View(func(txn *badger.Txn) error {
			opts := badger.DefaultIteratorOptions
			opts.PrefetchSize = limit
			iter := txn.NewIterator(opts)
			defer iter.Close()
			defer close(results)

			count := 0
			var cdxResponse index.IdResponse

			for iter.Seek(nil); iter.Valid(); iter.Next() {
				if count >= limit {
					return nil
				}
				count++

				key := iter.Item().KeyCopy(nil)
				err := iter.Item().Value(func(value []byte) error {
					cdxResponse = index.IdResponse{Key: string(key), Value: string(value)}
					return nil
				})
				if err != nil {
					cdxResponse = index.IdResponse{Error: err}
				}
				select {
				case <-ctx.Done():
					results <- index.IdResponse{Error: ctx.Err()}
					return nil
				case results <- cdxResponse:
				}

			}
			return nil
		})
	}()
	return nil
}

func (db *DB) GetFileInfo(_ context.Context, filename string) (*schema.Fileinfo, error) {
	return db.getFileInfo(filename)
}

func (db *DB) ListFileInfo(ctx context.Context, limit int, results chan<- index.FileInfoResponse) error {
	return db.listFileInfo(ctx, limit, results)
}

// Delete removes all data from the database.
func (db *DB) Delete(ctx context.Context) error {
	var firstErr error
	err := db.IdIndex.DropAll()
	if err != nil && firstErr == nil {
		firstErr = err
	}
	err = db.CdxIndex.DropAll()
	if err != nil && firstErr == nil {
		firstErr = err
	}
	err = db.FileIndex.DropAll()
	if err != nil && firstErr == nil {
		firstErr = err
	}
	return firstErr
}

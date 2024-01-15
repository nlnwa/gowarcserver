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
	"context"

	"github.com/dgraph-io/badger/v4"
	"github.com/nlnwa/gowarcserver/index"
	"github.com/nlnwa/gowarcserver/internal/keyvalue"
	"github.com/nlnwa/gowarcserver/loader"
	"github.com/nlnwa/gowarcserver/schema"
	"github.com/nlnwa/gowarcserver/timestamp"
	"google.golang.org/protobuf/proto"
)

// Assert capabilities

// Assert DB implements the keyvalue.DebugAPI interface.
var _ keyvalue.DebugAPI = (*DB)(nil)

// Assert DB implements the index.CdxAPI interface.
var _ index.CdxAPI = (*DB)(nil)

// Assert DB implements the index.FileAPI interface.
var _ index.FileAPI = (*DB)(nil)

// Assert DB implements the index.IdAPI interface.
var _ index.IdAPI = (*DB)(nil)

// Assert that DB implements index.ReportGenerator
var _ index.ReportGenerator = (*DB)(nil)

// Assert that DB implements loader.StorageRefResolver
var _ loader.StorageRefResolver = (*DB)(nil)

// Assert that DB implements loader.FilePathResolver
var _ loader.FilePathResolver = (*DB)(nil)

// cdxFromItem unmarshals a badger item value into a schema.Cdx.
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

func (db *DB) Debug(ctx context.Context, req keyvalue.DebugRequest, results chan<- keyvalue.CdxResponse) error {
	key := keyvalue.KeyWithPrefix(req.Key, "")

	dateRange := req.DateRange()
	filter := req.Filter()

	go func() {
		_ = db.CdxIndex.View(func(txn *badger.Txn) error {
			count := 0
			opts := badger.DefaultIteratorOptions
			opts.PrefetchValues = false
			opts.Prefix = key

			it := txn.NewIterator(opts)
			defer it.Close()
			defer close(results)

			for it.Seek(key); it.ValidForPrefix(key); it.Next() {
				cdxResponse := func() (cdxResponse *keyvalue.CdxResponse) {
					key := keyvalue.CdxKey(it.Item().Key())
					if !dateRange.Contains(key.Unix()) {
						return nil
					}
					err := it.Item().Value(func(v []byte) error {
						result := new(schema.Cdx)
						if err := proto.Unmarshal(v, result); err != nil {
							return err
						}
						if filter.Eval(result) {
							cdxResponse = &keyvalue.CdxResponse{
								Key:   key,
								Value: result,
							}
						}
						return nil
					})
					if err != nil {
						return &keyvalue.CdxResponse{Error: err}
					}

					return cdxResponse
				}()
				// skip if empty response
				if cdxResponse == nil {
					continue
				}
				// send result
				select {
				case <-ctx.Done():
					results <- keyvalue.CdxResponse{Error: ctx.Err()}
					return nil
				case results <- *cdxResponse:
					if cdxResponse.GetError() == nil {
						count++
					}
				}
				// stop iteration if limit is reached
				if req.Limit() > 0 && count >= req.Limit() {
					break
				}
			}
			return nil
		})
	}()
	return nil
}

// closest returns the closest cdx values in the database.
func (db *DB) closest(ctx context.Context, request index.Request, results chan<- index.CdxResponse) error {
	count := 0
	prefix, key := keyvalue.Closest(request)

	opts := badger.DefaultIteratorOptions
	opts.Prefix = prefix

	ts, err := timestamp.Parse(request.Closest())
	if err != nil {
		return err
	}
	closest := ts.Unix()
	isClosest := timestamp.CompareClosest(closest)
	matchType := request.MatchType()
	_, schemeAndUserInfo, _ := keyvalue.SplitSSURT(request.Ssurt())

	go func() {
		defer close(results)

		_ = db.CdxIndex.View(func(txn *badger.Txn) error {
			opts := badger.DefaultIteratorOptions
			opts.PrefetchSize = request.Limit()
			opts.Prefix = prefix

			forward := txn.NewIterator(opts)
			defer forward.Close()
			forward.Seek(key)

			opts.Reverse = true
			backward := txn.NewIterator(opts)
			defer backward.Close()
			backward.Seek(key)

			for {
				var ft int64
				var bt int64

				// get forward ts
				if forward.ValidForPrefix(prefix) {
					ft = keyvalue.CdxKey(forward.Item().Key()).Unix()
				}
				// get backward ts
				if backward.ValidForPrefix(prefix) {
					bt = keyvalue.CdxKey(backward.Item().Key()).Unix()
				}

				var iter *badger.Iterator

				if ft != 0 && bt != 0 {
					// find closest of forward and backward
					if isClosest(ft, bt) {
						iter = forward
					} else {
						iter = backward
					}
				} else if ft != 0 {
					iter = forward
				} else if bt != 0 {
					iter = backward
				} else {
					return nil
				}
				key := keyvalue.CdxKey(iter.Item().Key())
				if matchType == index.MatchTypeVerbatim {
					if key.SchemeAndUserInfo() != schemeAndUserInfo {
						iter.Next()
						continue
					}
				}
				var cdxResponse keyvalue.CdxResponse
				cdx, err := cdxFromItem(iter.Item())
				if err != nil {
					cdxResponse = keyvalue.CdxResponse{Error: err}
				} else if request.Filter().Eval(cdx) {
					cdxResponse = keyvalue.CdxResponse{Value: cdx}
				} else {
					iter.Next()
					continue
				}

				select {
				case <-ctx.Done():
					results <- keyvalue.CdxResponse{Error: ctx.Err()}
					return nil
				case results <- cdxResponse:
					if cdxResponse.GetError() == nil {
						count++
					}
				}
				if request.Limit() > 0 && count >= request.Limit() {
					break
				}
				iter.Next()
			}
			return nil
		})
	}()
	return nil
}

// Search searches the index database
func (db *DB) Search(ctx context.Context, search index.Request, results chan<- index.CdxResponse) error {
	if search.Sort() == index.SortClosest {
		return db.closest(ctx, search, results)
	}
	return db.search(ctx, search, results)
}

// search the index database.
func (db *DB) search(ctx context.Context, req index.Request, results chan<- index.CdxResponse) error {
	_, schemeAndUserInfo, _ := keyvalue.SplitSSURT(req.Ssurt())
	reverse := req.Sort() == index.SortDesc
	prefix := keyvalue.SearchKey(req)

	// see https://dgraph.io/docs/badger/faq/#reverse-iteration-doesnt-give-me-the-right-results
	key := prefix
	if reverse {
		key = append(key, 0xff)
	}
	dateRange := req.DateRange()
	filter := req.Filter()
	matchType := req.MatchType()

	go func() {
		_ = db.CdxIndex.View(func(txn *badger.Txn) error {
			count := 0
			opts := badger.DefaultIteratorOptions
			opts.PrefetchValues = false
			opts.Prefix = prefix
			opts.Reverse = reverse

			it := txn.NewIterator(opts)
			defer it.Close()
			defer close(results)

			for it.Seek(key); it.ValidForPrefix(prefix); it.Next() {
				cdxResponse := func() (cdxResponse *keyvalue.CdxResponse) {
					key := keyvalue.CdxKey(it.Item().Key())
					if !dateRange.Contains(key.Unix()) {
						return nil
					}
					if matchType == index.MatchTypeVerbatim {
						if key.SchemeAndUserInfo() != schemeAndUserInfo {
							return nil
						}
					}
					err := it.Item().Value(func(v []byte) error {
						result := new(schema.Cdx)
						if err := proto.Unmarshal(v, result); err != nil {
							return err
						}
						if filter.Eval(result) {
							cdxResponse = &keyvalue.CdxResponse{
								Key:   key,
								Value: result,
							}
						}
						return nil
					})
					if err != nil {
						return &keyvalue.CdxResponse{Error: err}
					}

					return cdxResponse
				}()
				// skip if empty response
				if cdxResponse == nil {
					continue
				}
				// send result
				select {
				case <-ctx.Done():
					results <- keyvalue.CdxResponse{Error: ctx.Err()}
					return nil
				case results <- *cdxResponse:
					if cdxResponse.GetError() == nil {
						count++
					}
				}
				// stop iteration if limit is reached
				if req.Limit() > 0 && count >= req.Limit() {
					break
				}
			}
			return nil
		})
	}()
	return nil
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

func (db *DB) ListStorageRef(ctx context.Context, req index.Request, results chan<- index.IdResponse) error {
	go func() {
		limit := req.Limit()
		_ = db.IdIndex.View(func(txn *badger.Txn) error {
			opts := badger.DefaultIteratorOptions
			opts.PrefetchSize = limit
			iter := txn.NewIterator(opts)
			defer iter.Close()
			defer close(results)

			count := 0
			var idResponse keyvalue.IdResponse

			for iter.Seek(nil); iter.Valid(); iter.Next() {
				if limit > 0 && count >= limit {
					return nil
				}
				count++

				key := iter.Item().KeyCopy(nil)
				err := iter.Item().Value(func(value []byte) error {
					idResponse = keyvalue.IdResponse{Key: string(key), Value: string(value)}
					return nil
				})
				if err != nil {
					idResponse = keyvalue.IdResponse{Error: err}
				}
				select {
				case <-ctx.Done():
					results <- keyvalue.IdResponse{Error: ctx.Err()}
					return nil
				case results <- idResponse:
				}

			}
			return nil
		})
	}()
	return nil
}

func (db *DB) GetFileInfo(_ context.Context, filename string) (*schema.FileInfo, error) {
	return db.getFileInfo(filename)
}

func (db *DB) ListFileInfo(ctx context.Context, req index.Request, results chan<- index.FileInfoResponse) error {
	return db.listFileInfo(ctx, req.Limit(), results)
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

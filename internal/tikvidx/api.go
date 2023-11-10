/*
 * Copyright 2022 National Library of Norway.
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
	"context"
	"fmt"
	"strings"

	"github.com/tikv/client-go/v2/rawkv"

	"github.com/nlnwa/gowarcserver/index"
	"github.com/nlnwa/gowarcserver/schema"
	"google.golang.org/protobuf/proto"
)

// Closest returns the first closest cdx value(s).
func (db *DB) Closest(ctx context.Context, req index.Request, res chan<- index.CdxResponse) error {
	_, values, err := scanClosest(db.client, ctx, req.Key(), req.Closest(), req.Limit())
	if err != nil {
		return err
	}

	go func() {
		defer close(res)

		if len(values) == 0 {
			res <- index.CdxResponse{}
			return
		}

		count := 0

		for _, v := range values {
			var cdxResponse index.CdxResponse
			cdx := new(schema.Cdx)
			err := proto.Unmarshal(v, cdx)
			if err != nil {
				cdxResponse = index.CdxResponse{Error: err}
			} else if req.Filter().Eval(cdx) {
				cdxResponse = index.CdxResponse{Cdx: cdx}
			} else {
				continue
			}
			select {
			case <-ctx.Done():
				res <- index.CdxResponse{Error: ctx.Err()}
				return
			case res <- cdxResponse:
				if cdxResponse.Error == nil {
					count++
				}
			}
			if req.Limit() > 0 && count >= req.Limit() {
				break
			}
		}
	}()

	return nil
}

func (db *DB) Search(ctx context.Context, req index.Request, res chan<- index.CdxResponse) error {
	it, err := newIter(ctx, db.client, req)
	if err != nil {
		return err
	}
	if it == nil {
		close(res)
		return nil
	}

	go func() {
		defer close(res)
		defer it.Close()

		count := 0

		for it.Valid() {
			cdxResponse := func() index.CdxResponse {
				inDateRange, err := req.DateRange().ContainsStr(cdxKey(it.Key()).ts())
				if err != nil {
					return index.CdxResponse{Error: err}
				}
				if !inDateRange {
					return index.CdxResponse{}
				}
				cdx := new(schema.Cdx)
				if err := proto.Unmarshal(it.Value(), cdx); err != nil {
					return index.CdxResponse{Error: err}
				} else if req.Filter().Eval(cdx) {
					return index.CdxResponse{Cdx: cdx}
				}
				return index.CdxResponse{}
			}()
			if cdxResponse == (index.CdxResponse{}) {
				// skip
			} else {
				select {
				case <-ctx.Done():
					res <- index.CdxResponse{Error: ctx.Err()}
					return
				case res <- cdxResponse:
					if cdxResponse.Error == nil {
						count++
					}
				}
			}
			if req.Limit() > 0 && count >= req.Limit() {
				break
			}
			if err = it.Next(); err != nil {
				res <- index.CdxResponse{Error: err}
				break
			}
		}
	}()
	return nil
}

func (db *DB) List(ctx context.Context, limit int, res chan<- index.CdxResponse) error {
	if limit == 0 || limit > rawkv.MaxRawKVScanLimit {
		limit = rawkv.MaxRawKVScanLimit
	}
	_, values, err := db.client.Scan(ctx, []byte(cdxPrefix), []byte(cdxPrefix+"\xff"), limit)
	if err != nil {
		return err
	}

	go func() {
		defer close(res)
		for _, v := range values {
			select {
			case <-ctx.Done():
				return
			default:
			}
			cdx := new(schema.Cdx)
			err := proto.Unmarshal(v, cdx)
			if err != nil {
				res <- index.CdxResponse{Error: err}
			} else {
				res <- index.CdxResponse{Cdx: cdx}
			}
		}
	}()

	return nil
}

func (db *DB) GetFileInfo(_ context.Context, filename string) (*schema.Fileinfo, error) {
	return db.getFileInfo(filename)
}

func (db *DB) ListFileInfo(ctx context.Context, limit int, res chan<- index.FileInfoResponse) error {
	if limit == 0 || limit > rawkv.MaxRawKVScanLimit {
		limit = rawkv.MaxRawKVScanLimit
	}
	_, values, err := db.client.Scan(ctx, []byte(filePrefix), []byte(filePrefix+"\xff"), limit)
	if err != nil {
		return err
	}
	go func() {
		defer close(res) // close response channel
		for _, v := range values {
			select {
			case <-ctx.Done():
				return
			default:
			}

			fileInfo := new(schema.Fileinfo)
			err := proto.Unmarshal(v, fileInfo)
			if err != nil {
				res <- index.FileInfoResponse{Error: err}
			} else {
				res <- index.FileInfoResponse{Fileinfo: fileInfo}
			}
		}
	}()

	return nil
}

func (db *DB) GetStorageRef(ctx context.Context, id string) (string, error) {
	b, err := db.client.Get(ctx, []byte(idPrefix+id))
	return string(b), err
}

func (db *DB) ListStorageRef(ctx context.Context, limit int, res chan<- index.IdResponse) error {
	if limit == 0 || limit > rawkv.MaxRawKVScanLimit {
		limit = rawkv.MaxRawKVScanLimit
	}
	keys, values, err := db.client.Scan(ctx, []byte(idPrefix), []byte(idPrefix+"\xff"), limit)
	if err != nil {
		return err
	}

	go func() {
		defer close(res)

		for i, k := range keys {
			select {
			case <-ctx.Done():
				return
			default:
			}
			k := strings.TrimPrefix(string(k), idPrefix)
			res <- index.IdResponse{Key: k, Value: string(values[i])}
		}
	}()

	return nil
}

// Resolve looks up warcId in the id index of the database and returns corresponding storageRef, or an error if not found.
func (db *DB) Resolve(ctx context.Context, warcId string) (string, error) {
	val, err := db.client.Get(ctx, []byte(idPrefix+warcId))
	if err != nil {
		return "", err
	}
	if val == nil {
		return "", nil
	}
	return string(val), nil
}

// ResolvePath looks up filename in file index and returns the path field.
func (db *DB) ResolvePath(filename string) (filePath string, err error) {
	fileInfo, err := db.getFileInfo(filename)
	if err != nil {
		return "", err
	}
	if fileInfo == nil {
		return "", fmt.Errorf("file not found: %s", filename)
	}
	return fileInfo.Path, err
}

// Delete removes all data from the database.
func (db *DB) Delete(ctx context.Context) error {
	var firstErr error
	var err error
	err = db.client.DeleteRange(ctx, []byte(cdxPrefix), []byte(cdxPrefix+"\xff"))
	if err != nil {
		firstErr = err
	}
	err = db.client.DeleteRange(ctx, []byte(filePrefix), []byte(filePrefix+"\xff"))
	if err != nil && firstErr == nil {
		firstErr = err
	}
	err = db.client.DeleteRange(ctx, []byte(idPrefix), []byte(idPrefix+"\xff"))
	if err != nil && firstErr == nil {
		firstErr = err
	}
	return firstErr
}

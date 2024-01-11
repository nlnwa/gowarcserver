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

	"github.com/nlnwa/gowarcserver/index"
	"github.com/nlnwa/gowarcserver/internal/keyvalue"
	"github.com/nlnwa/gowarcserver/schema"
	"google.golang.org/protobuf/proto"
)

// iterator mimics tikv's internal iterator interface
type iterator interface {
	Next() error
	Key() []byte
	Value() []byte
	Valid() bool
	Close()
}

func (db *DB) Search(ctx context.Context, req index.Request, res chan<- index.CdxResponse) error {
	var it iterator
	var err error
	if req.Sort() == index.SortClosest {
		it, err = newClosestIter(ctx, db.client, req)
	} else {
		key := keyvalue.SearchKeyWithPrefix(req, cdxPrefix)
		it, err = newIter(ctx, key, db.client, req)
	}
	if err != nil {
		return err
	}
	if it == nil {
		close(res)
		return nil
	}
	matchType := req.MatchType()
	_, schemeAndUserInfo, _ := keyvalue.SplitSSURT(req.Ssurt())

	go func() {
		defer close(res)
		defer it.Close()

		count := 0

		for it.Valid() {
			cdxResponse := func() *keyvalue.CdxResponse {
				cdxKey := keyvalue.CdxKey(it.Key())
				if !req.DateRange().Contains(cdxKey.Unix()) {
					return nil
				}
				if matchType == index.MatchTypeVerbatim {
					if cdxKey.SchemeAndUserInfo() != schemeAndUserInfo {
						return nil
					}
				}
				cdx := new(schema.Cdx)
				if err := proto.Unmarshal(it.Value(), cdx); err != nil {
					return &keyvalue.CdxResponse{Error: err}
				} else if req.Filter().Eval(cdx) {
					return &keyvalue.CdxResponse{
						Key: cdxKey,
						Cdx: cdx,
					}
				}
				return nil
			}()
			if cdxResponse == nil {
				if err = it.Next(); err != nil {
					res <- keyvalue.CdxResponse{Error: err}
					break
				}
				continue
			}
			select {
			case <-ctx.Done():
				res <- keyvalue.CdxResponse{Error: ctx.Err()}
				return
			case res <- cdxResponse:
				if cdxResponse.GetError() == nil {
					count++
				}
			}
			if req.Limit() > 0 && count >= req.Limit() {
				break
			}
			if err = it.Next(); err != nil {
				res <- keyvalue.CdxResponse{Error: err}
				break
			}
		}
	}()
	return nil
}

func (db *DB) GetFileInfo(_ context.Context, filename string) (*schema.FileInfo, error) {
	return db.getFileInfo(filename)
}

func (db *DB) ListFileInfo(ctx context.Context, req index.Request, res chan<- index.FileInfoResponse) error {
	key := []byte(filePrefix)
	it, err := newIter(ctx, key, db.client, req)
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
			var response keyvalue.FileInfoResponse
			fileInfo := new(schema.FileInfo)
			err := proto.Unmarshal(it.Value(), fileInfo)
			if err != nil {
				response.Error = err
			} else {
				response.FileInfo = fileInfo
			}
			select {
			case <-ctx.Done():
				return
			case res <- response:
				if response.Error == nil {
					count++
				}
			}
			if req.Limit() > 0 && count >= req.Limit() {
				return
			}
			if err = it.Next(); err != nil {
				res <- keyvalue.FileInfoResponse{Error: err}
				return
			}
		}
	}()

	return nil
}

func (db *DB) GetStorageRef(ctx context.Context, id string) (string, error) {
	key := []byte(idPrefix + id)
	b, err := db.client.Get(ctx, key)
	return string(b), err
}

func (db *DB) ListStorageRef(ctx context.Context, req index.Request, res chan<- index.IdResponse) error {
	key := []byte(idPrefix)
	it, err := newIter(ctx, key, db.client, req)
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
			var response keyvalue.IdResponse

			response.Key = strings.TrimPrefix(string(it.Key()), idPrefix)
			response.Value = string(it.Value())

			select {
			case <-ctx.Done():
				return
			case res <- response:
				count++
			}
			if req.Limit() > 0 && count >= req.Limit() {
				return
			}
			if err = it.Next(); err != nil {
				res <- keyvalue.IdResponse{Error: err}
				return
			}
		}
	}()
	return nil
}

// Resolve looks up warcId in the id index of the database and returns corresponding storageRef, or an error if not found.
func (db *DB) Resolve(ctx context.Context, warcId string) (string, error) {
	key := keyvalue.KeyWithPrefix(warcId, idPrefix)
	val, err := db.client.Get(ctx, key)
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
	var err, firstErr error

	idKey := keyvalue.KeyWithPrefix("", idPrefix)
	err = db.client.DeleteRange(ctx, idKey, append(idKey, 0xff))
	if err != nil {
		firstErr = err
	}

	fileKey := keyvalue.KeyWithPrefix("", filePrefix)
	err = db.client.DeleteRange(ctx, fileKey, append(fileKey, 0xff))
	if err != nil && firstErr == nil {
		firstErr = err
	}

	cdxKey := keyvalue.KeyWithPrefix("", cdxPrefix)
	err = db.client.DeleteRange(ctx, cdxKey, append(cdxKey, 0xff))
	if err != nil && firstErr == nil {
		firstErr = err
	}

	return firstErr
}

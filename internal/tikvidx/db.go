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
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/nlnwa/gowarcserver/index"
	"github.com/nlnwa/gowarcserver/internal/keyvalue"
	"github.com/nlnwa/gowarcserver/schema"
	"github.com/rs/zerolog/log"
	"github.com/tikv/client-go/v2/rawkv"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	idPrefix   = "i"
	filePrefix = "f"
	cdxPrefix  = "c"
)

const delimiter = "_"

type DB struct {
	client *rawkv.Client
	batch  chan index.Record
	done   chan struct{}
	wg     sync.WaitGroup
}

func NewDB(options ...Option) (db *DB, err error) {
	opts := defaultOptions()
	for _, opt := range options {
		opt(opts)
	}
	dbName := delimiter
	if opts.Database != "" {
		dbName += opts.Database
	}
	// prefix all keys with name of database
	idPrefix = dbName + delimiter + idPrefix + delimiter
	filePrefix = dbName + delimiter + filePrefix + delimiter
	cdxPrefix = dbName + delimiter + cdxPrefix + delimiter

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	client, err := rawkv.NewClientWithOpts(ctx, opts.PdAddr)
	if err != nil {
		return nil, err
	}

	done := make(chan struct{})

	db = &DB{
		client: client,
		done:   done,
	}

	if opts.ReadOnly {
		return
	}

	db.batch = make(chan index.Record, opts.BatchMaxSize)

	// start batch worker
	db.wg.Add(1)
	go func() {
		defer db.wg.Done()
		ticker := time.NewTimer(opts.BatchMaxWait)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				db.FlushBatch()
				return
			case <-ticker.C:
				db.FlushBatch()
			}
		}
	}()

	return
}

// Close stops the batch workers and closes the index databases.
func (db *DB) Close() {
	close(db.done)
	db.wg.Wait()
	_ = db.client.Close()
}

// addFile checks if file referenced by filePath is indexed or has changed and adds/updates the index accordingly.
func (db *DB) addFile(filePath string) error {
	stat, err := os.Stat(filePath)
	if err != nil {
		return fmt.Errorf("failed to get file info: %s: %w", filePath, err)
	}

	fileSize := stat.Size()
	fileLastModified := stat.ModTime()
	fn := filepath.Base(filePath)
	if fileInfo, err := db.getFileInfo(fn); err == nil && fileInfo != nil {
		if err := fileInfo.GetLastModified().CheckValid(); err != nil {
			return err
		}
		fileInfoLastModified := fileInfo.LastModified.AsTime()
		if fileInfo.Size == fileSize && fileInfoLastModified.Equal(fileLastModified) {
			return index.AlreadyIndexedError
		}
	}

	return db.updateFilePath(filePath)
}

func (db *DB) updateFilePath(filePath string) error {
	var err error
	fileInfo := new(schema.FileInfo)

	fileInfo.Path, err = filepath.Abs(filePath)
	if err != nil {
		return err
	}

	fileInfo.Name = filepath.Base(fileInfo.Path)
	stat, err := os.Stat(fileInfo.Path)
	if err != nil {
		return err
	}

	fileInfo.Size = stat.Size()
	fileInfo.LastModified = timestamppb.New(stat.ModTime())

	return db.putFileInfo(fileInfo)
}

func (db *DB) putFileInfo(fi *schema.FileInfo) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	key, value, err := keyvalue.MarshalFileInfo(fi, filePrefix)
	if err != nil {
		log.Error().Err(err).Msg("Failed to marshal file info")
		return err
	}
	err = db.client.Put(ctx, key, value)
	if err != nil {
		log.Error().Err(err).Msgf("Failed to put file info: %s", fi.Name)
		return err
	}
	return nil
}

func (db *DB) getFileInfo(fileName string) (*schema.FileInfo, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	key := keyvalue.KeyWithPrefix(fileName, filePrefix)
	val, err := db.client.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	if val == nil {
		return nil, nil
	}
	fi := new(schema.FileInfo)
	err = proto.Unmarshal(val, fi)
	if err != nil {
		return nil, err
	}
	return fi, nil
}

// write schedules a Record to be added to the DB via the batch channel.
func (db *DB) write(rec index.Record) {
	select {
	case <-db.done:
		// do nothing
	case db.batch <- rec:
		// added record to batch
	default:
		// batch channel is full so flush batch channel before adding record to batch
		db.FlushBatch()
		db.batch <- rec
	}
}

// see https://github.com/tikv/tikv/blob/a0e8a7a163302bc9a7be5fd5a903b6a156797eb8/src/storage/config.rs#L21
const tikvMaxKeySize = 8 * 1024

func (db *DB) collectBatch() ([][]byte, [][]byte) {
	var keys [][]byte
	var values [][]byte
	for {
		select {
		case r := <-db.batch:
			idKey, idValue, _ := marshalId(r)
			cdxKey, cdxValue, err := marshalCdx(r)
			if err != nil {
				log.Error().Err(err).Msgf("failed to marshal record: %v", r)
				continue
			}
			// check if key size exceeds tikv max key size
			// TODO: store big keys in separate db
			if len(cdxKey) > tikvMaxKeySize {
				log.Warn().Str("key", string(cdxKey)).Msgf("Skipping: cdx key size exceeds tikv max key size (%d): %d", tikvMaxKeySize, len(cdxKey))
				continue
			}
			keys = append(keys, idKey, cdxKey)
			values = append(values, idValue, cdxValue)
		default:
			return keys, values
		}
	}
}

// FlushBatch collects all records in the batch channel and updates the id and cdx indices.
func (db *DB) FlushBatch() {
	keys, values := db.collectBatch()
	if len(keys) == 0 {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	err := db.client.BatchPut(ctx, keys, values)
	if err != nil {
		log.Error().Err(err).Msgf("Batch put failed")
	}
}

// idKV takes a record and returns a key-value pair for the id index.
func marshalId(r index.Record) ([]byte, []byte, error) {
	return keyvalue.MarshalId(r, idPrefix)
}

// marshalCdx takes a record and returns a key-value pair for the cdx index.
func marshalCdx(r index.Record) ([]byte, []byte, error) {
	return keyvalue.MarshalCdxWithPrefix(r, cdxPrefix)
}

func (db *DB) Write(rec index.Record) error {
	db.write(rec)
	return nil
}

func (db *DB) Index(path string) error {
	return db.addFile(path)
}

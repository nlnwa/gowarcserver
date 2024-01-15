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
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/nlnwa/gowarcserver/index"
	"github.com/nlnwa/gowarcserver/internal/keyvalue"
	"github.com/nlnwa/gowarcserver/schema"
	"github.com/rs/zerolog/log"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type DB struct {
	// IdIndex maps record id to storage ref
	IdIndex *badger.DB

	// FileIndex maps filepath to fileinfo
	FileIndex *badger.DB

	// CdxIndex maps cdx key to cdx record
	CdxIndex *badger.DB

	// ReportIndex maps report id to report
	ReportIndex *badger.DB

	batch chan index.Record

	done chan struct{}

	wg sync.WaitGroup

	tasks map[string]context.CancelFunc
}

func NewDB(options ...Option) (db *DB, err error) {
	// Increase GOMAXPROCS as recommended by badger
	// https://github.com/dgraph-io/badger#are-there-any-go-specific-settings-that-i-should-use
	runtime.GOMAXPROCS(128)

	opts := defaultDbOptions()
	for _, opt := range options {
		opt(opts)
	}

	var idIndex *badger.DB
	var fileIndex *badger.DB
	var cdxIndex *badger.DB
	var reportIndex *badger.DB

	batch := make(chan index.Record, opts.BatchMaxSize)
	done := make(chan struct{})

	if idIndex, err = newBadgerDB(path.Join(opts.Path, opts.Database, "id-index"), opts.Compression, opts.ReadOnly, opts.Silent); err != nil {
		return
	}
	if fileIndex, err = newBadgerDB(path.Join(opts.Path, opts.Database, "file-index"), opts.Compression, opts.ReadOnly, opts.Silent); err != nil {
		return
	}
	if cdxIndex, err = newBadgerDB(path.Join(opts.Path, opts.Database, "cdx-index"), opts.Compression, opts.ReadOnly, opts.Silent); err != nil {
		return
	}
	if reportIndex, err = newBadgerDB(path.Join(opts.Path, opts.Database, "report-index"), opts.Compression, opts.ReadOnly, opts.Silent); err != nil {
		return
	}

	db = &DB{
		IdIndex:     idIndex,
		FileIndex:   fileIndex,
		CdxIndex:    cdxIndex,
		ReportIndex: reportIndex,
		batch:       batch,
		done:        done,
		tasks:       make(map[string]context.CancelFunc),
	}

	// We don't need to run batch and gc workers when operating in read-only mode.
	if opts.ReadOnly {
		return
	}

	// batch worker
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

	// gc worker
	db.wg.Add(1)
	go func() {
		defer db.wg.Done()
		ticker := time.NewTimer(opts.GcInterval)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				db.runValueLogGC(0.3)
				return
			case <-ticker.C:
				db.runValueLogGC(0.5)
			}
		}
	}()

	return
}

func (db *DB) runValueLogGC(discardRatio float64) {
	var wg sync.WaitGroup
	for _, m := range []*badger.DB{db.IdIndex, db.FileIndex, db.CdxIndex, db.ReportIndex} {
		m := m
		if m == nil {
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			var err error
			for err == nil {
				err = m.RunValueLogGC(discardRatio)
			}
		}()
	}
	wg.Wait()
}

// Close stops the gc and batch workers and closes the index databases.
func (db *DB) Close() {
	for _, cancel := range db.tasks {
		cancel()
	}
	close(db.done)
	db.wg.Wait()
	_ = db.IdIndex.Close()
	_ = db.FileIndex.Close()
	_ = db.CdxIndex.Close()
	_ = db.ReportIndex.Close()
}

// addFile checks if file is indexed or has not changed since indexing, and adds file to file index.
func (db *DB) addFile(path string) error {
	stat, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("failed to get file info: %s: %w", path, err)
	}

	fileSize := stat.Size()
	fileLastModified := stat.ModTime()
	fn := filepath.Base(path)
	if fileInfo, err := db.getFileInfo(fn); err == nil {
		if err := fileInfo.GetLastModified().CheckValid(); err != nil {
			return err
		}
		fileInfoLastModified := fileInfo.LastModified.AsTime()
		if fileInfo.Size == fileSize && fileInfoLastModified.Equal(fileLastModified) {
			return index.AlreadyIndexedError
		}
	}

	return db.updateFilePath(path)
}

func (db *DB) updateFilePath(path string) error {
	var err error
	fileInfo := &schema.FileInfo{}

	fileInfo.Path, err = filepath.Abs(path)
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

	key, value, err := keyvalue.MarshalFileInfo(fileInfo, "")
	if err != nil {
		return err
	}

	return db.FileIndex.Update(func(txn *badger.Txn) error {
		return txn.Set(key, value)
	})
}

// write schedules a Record to be added to the DB via the batch channel.
func (db *DB) write(rec index.Record) {
	select {
	case <-db.done:
		// do nothing
	case db.batch <- rec:
		// added record to batch
	default:
		// batch channel is full so flush it before adding record to batch
		db.FlushBatch()
		db.batch <- rec
	}
}

// collectBatch returns a slice of all the records in the batch channel.
func (db *DB) collectBatch() (records []index.Record) {
	for {
		select {
		case record := <-db.batch:
			records = append(records, record)
		default:
			return
		}
	}
}

// FlushBatch collects all records in the batch channel and updates the id and cdx indices.
func (db *DB) FlushBatch() {
	records := db.collectBatch()
	if len(records) == 0 {
		return
	}

	// update id index

	if err := db.IdIndex.Update(set(records, marshalId)); err != nil {
		log.Error().Err(err).Msgf("Failed to update id index")
	}
	// update cdx index
	if err := db.CdxIndex.Update(set(records, marshalCdx)); err != nil {
		log.Error().Err(err).Msgf("Failed to update cdx index")
	}
}

func marshalId(r index.Record) ([]byte, []byte, error) {
	return keyvalue.MarshalId(r, "")
}

func marshalCdx(r index.Record) ([]byte, []byte, error) {
	return keyvalue.MarshalCdx(r)
}

func set(records []index.Record, m func(index.Record) ([]byte, []byte, error)) func(*badger.Txn) error {
	return func(txn *badger.Txn) error {
		for _, r := range records {
			key, value, err := m(r)
			if err != nil {
				return fmt.Errorf("failed to marshal '%s'-'%s': %w", key, r, err)
			}
			err = txn.Set(key, value)
			if err != nil {
				return fmt.Errorf("failed to set '%s'-'%s': %w", key, r, err)
			}
		}
		return nil
	}
}

func (db *DB) Write(rec index.Record) error {
	db.write(rec)
	return nil
}

func (db *DB) Index(path string) error {
	return db.addFile(path)
}

// Resolve looks up warcId in the id index of the database and returns corresponding storageRef, or an error if not found.
func (db *DB) Resolve(_ context.Context, warcId string) (storageRef string, err error) {
	key := keyvalue.Key(warcId)
	err = db.IdIndex.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			storageRef = string(val)
			return nil
		})
	})
	return
}

// ResolvePath looks up filename in file index and returns the path field.
func (db *DB) ResolvePath(filename string) (filePath string, err error) {
	fileInfo, err := db.getFileInfo(filename)
	return fileInfo.Path, err
}

func (db *DB) getFileInfo(fileName string) (*schema.FileInfo, error) {
	key := keyvalue.Key(fileName)
	val := new(schema.FileInfo)
	err := db.FileIndex.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}
		err = item.Value(func(v []byte) error {
			return proto.Unmarshal(v, val)
		})
		return err
	})
	return val, err
}

func (db *DB) listFileInfo(ctx context.Context, limit int, results chan<- index.FileInfoResponse) error {
	go func() {
		_ = db.FileIndex.View(func(txn *badger.Txn) error {
			opts := badger.DefaultIteratorOptions
			opts.PrefetchSize = limit
			iter := txn.NewIterator(opts)

			defer iter.Close()
			defer close(results)

			count := 0
			for iter.Seek(nil); iter.Valid(); iter.Next() {
				select {
				case <-ctx.Done():
					results <- keyvalue.FileInfoResponse{Error: ctx.Err()}
					return nil
				default:
				}

				if limit > 0 && count >= limit {
					return nil
				}
				count++

				err := iter.Item().Value(func(value []byte) error {
					fileInfo := new(schema.FileInfo)
					err := proto.Unmarshal(value, fileInfo)
					if err != nil {
						return err
					}
					results <- keyvalue.FileInfoResponse{FileInfo: fileInfo, Error: nil}
					return nil
				})
				if err != nil {
					results <- keyvalue.FileInfoResponse{Error: err}
					return nil
				}
			}
			return nil
		})
	}()
	return nil
}

func (db *DB) GetCdx(key string) (*schema.Cdx, error) {
	val := new(schema.Cdx)
	err := db.CdxIndex.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err != nil {
			return err
		}
		err = item.Value(func(v []byte) error {
			return proto.Unmarshal(v, val)
		})
		return err
	})
	return val, err
}

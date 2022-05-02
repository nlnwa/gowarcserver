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
	"errors"
	"fmt"
	"github.com/dgraph-io/badger/v3"
	"github.com/dgraph-io/badger/v3/options"
	"github.com/nlnwa/gowarcserver/internal/timestamp"
	"github.com/nlnwa/gowarcserver/schema"
	"github.com/rs/zerolog/log"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	"os"
	"path"
	"path/filepath"
	"sync"
	"time"
)

type DB struct {
	// IdIndex maps record id to storage ref
	IdIndex *badger.DB

	// FileIndex maps filepath to fileinfo
	FileIndex *badger.DB

	// CdxIndex maps cdx key to cdx record
	CdxIndex *badger.DB

	batch chan record

	done chan struct{}

	wg sync.WaitGroup
}

func NewDB(options ...DbOption) (db *DB, err error) {
	opts := defaultDbOptions()
	for _, opt := range options {
		opt(opts)
	}

	var idIndex *badger.DB
	var fileIndex *badger.DB
	var cdxIndex *badger.DB

	dir := path.Join(opts.Path, "warcdb")
	batch := make(chan record, opts.BatchMaxSize)
	done := make(chan struct{})

	if idIndex, err = newBadgerDB(path.Join(dir, "id-index"), opts.Compression, opts.ReadOnly); err != nil {
		return
	}
	if fileIndex, err = newBadgerDB(path.Join(dir, "file-index"), opts.Compression, opts.ReadOnly); err != nil {
		return
	}
	if cdxIndex, err = newBadgerDB(path.Join(dir, "cdx-index"), opts.Compression, opts.ReadOnly); err != nil {
		return
	}

	db = &DB{
		IdIndex:   idIndex,
		FileIndex: fileIndex,
		CdxIndex:  cdxIndex,
		batch:     batch,
		done:      done,
	}

	// If read-only return. We don't need to run batch and gc workers when read-only.
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
				db.flushBatch()
				return
			case <-ticker.C:
				db.flushBatch()
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

func (d *DB) runValueLogGC(discardRatio float64) {
	var wg sync.WaitGroup
	for _, m := range []*badger.DB{d.IdIndex, d.FileIndex, d.CdxIndex} {
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
func (d *DB) Close() {
	close(d.done)
	d.wg.Wait()
	_ = d.IdIndex.Close()
	_ = d.FileIndex.Close()
	_ = d.CdxIndex.Close()
}

// addFile checks if file is indexed or has not changed since indexing, and adds file to file index.
func (d *DB) addFile(filePath string) error {
	stat, err := os.Stat(filePath)
	if err != nil {
		return fmt.Errorf("failed to get file info: %s: %w", filePath, err)
	}

	fileSize := stat.Size()
	fileLastModified := stat.ModTime()
	fn := filepath.Base(filePath)
	if fileInfo, err := d.GetFileInfo(fn); err == nil {
		if err := fileInfo.GetLastModified().CheckValid(); err != nil {
			return err
		}
		fileInfoLastModified := fileInfo.LastModified.AsTime()
		if fileInfo.Size == fileSize && fileInfoLastModified.Equal(fileLastModified) {
			return errors.New("already indexed")
		}
	}

	return d.updateFilePath(filePath)
}

func (d *DB) updateFilePath(filePath string) error {
	var err error
	fileInfo := &schema.Fileinfo{}

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

	value, err := proto.Marshal(fileInfo)
	if err != nil {
		return err
	}

	return d.FileIndex.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(fileInfo.Name), value)
	})
}

// write schedules a Record to be added to the DB via the batch channel.
func (d *DB) write(rec record) {
	select {
	case <-d.done:
		// do nothing
	case d.batch <- rec:
		// added record to batch
	default:
		// batch channel is full so flush batch channel before adding record to batch
		d.flushBatch()
		d.batch <- rec
	}
}

// collectBatch returns a slice of all the records in the batch channel.
func (d *DB) collectBatch() (records []record) {
	for {
		select {
		case record := <-d.batch:
			records = append(records, record)
		default:
			return
		}
	}
}

// flushBatch collects all records in the batch channel and updates the id and cdx indices.
func (d *DB) flushBatch() {
	records := d.collectBatch()
	if len(records) == 0 {
		return
	}

	// update id index
	if err := d.IdIndex.Update(set(records, marshalIdKey)); err != nil {
		log.Error().Err(err).Msgf("Failed to update id index")
	}
	// update cdx index
	if err := d.CdxIndex.Update(set(records, marshalCdxKey)); err != nil {
		log.Error().Err(err).Msgf("Failed to update cdx index")
	}
}

func set(records []record, m func(record) ([]byte, []byte, error)) func(*badger.Txn) error {
	return func(txn *badger.Txn) error {
		for _, r := range records {
			key, value, err := m(r)
			if err != nil {
				return fmt.Errorf("failed to set '%s'-'%s': %w", key, r, err)
			}
			err = txn.Set(key, value)
			if err != nil {
				return fmt.Errorf("failed to set '%s'-'%s': %w", key, r, err)
			}
		}
		return nil
	}
}

// marshalIdKey takes a record and returns a key-value pair for the id index.
func marshalIdKey(r record) ([]byte, []byte, error) {
	return []byte(r.GetRid()), []byte(r.GetRef()), nil
}

// marshalCdxKey takes a record and returns a key-value pair for the cdx index.
func marshalCdxKey(r record) ([]byte, []byte, error) {
	ts := timestamp.TimeTo14(r.GetSts().AsTime())
	key := []byte(r.GetSsu() + " " + ts + " " + r.GetSrt())
	value, err := r.marshal()
	return key, value, err
}

func (d *DB) Write(rec record) error {
	d.write(rec)
	return nil
}

func (d *DB) Index(fileName string) error {
	err := d.addFile(fileName)
	if err != nil {
		return err
	}
	return indexFile(fileName, d)
}

// Resolve looks up warcId in the id index of the database and returns corresponding storageRef, or an error if not found.
func (d *DB) Resolve(warcId string) (storageRef string, err error) {
	err = d.IdIndex.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(warcId))
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
func (d *DB) ResolvePath(filename string) (filePath string, err error) {
	fileInfo, err := d.GetFileInfo(filename)
	return fileInfo.Path, err
}

func (d *DB) GetFileInfo(fileName string) (*schema.Fileinfo, error) {
	val := new(schema.Fileinfo)
	err := d.FileIndex.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(fileName))
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

func (d *DB) GetCdx(key string) (*schema.Cdx, error) {
	val := new(schema.Cdx)
	err := d.CdxIndex.View(func(txn *badger.Txn) error {
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

func defaultDbOptions() *dbOptions {
	return &dbOptions{
		Compression:  options.Snappy,
		BatchMaxSize: 10000,
		BatchMaxWait: 5 * time.Second,
		GcInterval:   15 * time.Second,
		Path:         ".",
		ReadOnly:     false,
	}
}

type dbOptions struct {
	Compression  options.CompressionType
	BatchMaxSize int
	BatchMaxWait time.Duration
	GcInterval   time.Duration
	Path         string
	ReadOnly     bool
}

type DbOption func(opts *dbOptions)

func WithCompression(c options.CompressionType) DbOption {
	return func(opts *dbOptions) {
		opts.Compression = c
	}
}

func WithDir(d string) DbOption {
	return func(opts *dbOptions) {
		opts.Path = d
	}
}

func WithBatchMaxSize(size int) DbOption {
	return func(opts *dbOptions) {
		opts.BatchMaxSize = size
	}
}

func WithBatchMaxWait(t time.Duration) DbOption {
	return func(opts *dbOptions) {
		opts.BatchMaxWait = t
	}
}

func WithGcInterval(t time.Duration) DbOption {
	return func(opts *dbOptions) {
		opts.GcInterval = t
	}
}

func WithReadOnly(readOnly bool) DbOption {
	return func(opts *dbOptions) {
		opts.ReadOnly = readOnly
	}
}

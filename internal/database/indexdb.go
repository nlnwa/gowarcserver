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

package database

import (
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v3"
	"github.com/nlnwa/gowarc"
	"github.com/nlnwa/gowarcserver/schema"
	log "github.com/sirupsen/logrus"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type record struct {
	id       string
	filePath string
	offset   int64
	cdx      *schema.Cdx
}

type CdxDbIndex struct {
	idIndex   *badger.DB
	fileIndex *badger.DB
	cdxIndex  *badger.DB
	batch     chan *record
	done      chan struct{}
}

func NewCdxIndexDb(options ...DbOption) (*CdxDbIndex, error) {
	opts := defaultOptions()
	for _, opt := range options {
		opt(opts)
	}

	var idIndex *badger.DB
	var fileIndex *badger.DB
	var cdxIndex *badger.DB

	dir := path.Join(opts.Path, "warcdb")
	batch := make(chan *record, opts.BatchMaxSize)
	done := make(chan struct{})

	var err error
	if idIndex, err = newBadgerDB(path.Join(dir, "id"), opts.Compression); err != nil {
		return nil, err
	}
	if fileIndex, err = newBadgerDB(path.Join(dir, "file"), opts.Compression); err != nil {
		return nil, err
	}
	if cdxIndex, err = newBadgerDB(path.Join(dir, "cdx"), opts.Compression); err != nil {
		return nil, err
	}

	d := &CdxDbIndex{
		idIndex:   idIndex,
		fileIndex: fileIndex,
		cdxIndex:  cdxIndex,
		batch:     batch,
		done:      done,
	}

	// batch worker
	go func() {
		ticker := time.NewTimer(opts.BatchMaxWait)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				d.flushBatch()
				return
			case <-ticker.C:
				d.flushBatch()
			}
		}
	}()

	// gc worker
	go func() {
		ticker := time.NewTimer(opts.GcInterval)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				d.runValueLogGC(0.3)
				return
			case <-ticker.C:
				d.runValueLogGC(0.5)
			}
		}
	}()

	return d, nil
}

func (d *CdxDbIndex) runValueLogGC(discardRatio float64) {
	var wg sync.WaitGroup
	for _, m := range []*badger.DB{d.idIndex, d.fileIndex, d.cdxIndex} {
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

func (d *CdxDbIndex) AddFile(fileName string) error {
	// Check if file is indexed and has not changed since indexing
	stat, err := os.Stat(fileName)
	if err != nil {
		return fmt.Errorf("failed to get file info: %s: %w", fileName, stat)
	}

	fileSize := stat.Size()
	fileLastModified := stat.ModTime()
	fn := filepath.Base(fileName)
	if fileInfo, err := d.GetFileInfo(fn); err == nil {
		if err := fileInfo.GetLastModified().CheckValid(); err != nil {
			return err
		}
		fileInfoLastModified := fileInfo.LastModified.AsTime()
		if fileInfo.Size == fileSize && fileInfoLastModified.Equal(fileLastModified) {
			return errors.New("already indexed")
		}
	}

	return d.updateFilePath(fileName)
}

func (d *CdxDbIndex) Close() {
	close(d.done)
	_ = d.idIndex.Close()
	_ = d.fileIndex.Close()
	_ = d.cdxIndex.Close()
}

func (d *CdxDbIndex) Write(warcRecord gowarc.WarcRecord, filePath string, offset int64) error {
	if warcRecord.Type() != gowarc.Response || warcRecord.Type() != gowarc.Revisit {
		return nil
	}

	rec := &record{
		id:       warcRecord.WarcHeader().Get(gowarc.WarcRecordID),
		filePath: filePath,
		offset:   offset,
		cdx:      schema.NewCdxRecord(warcRecord, filePath, offset),
	}

	select {
	case <-d.done:
	case d.batch <- rec:
	default:
		d.flushBatch()
		d.batch <- rec
	}
	return nil
}

func (d *CdxDbIndex) updateFilePath(filePath string) error {
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

	return d.fileIndex.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(fileInfo.Name), value)
	})
}

// collectBatch consumes all records from batch channel and returns a slice of the records.
func (d *CdxDbIndex) collectBatch() []*record {
	var records []*record
	for {
		select {
		case record := <-d.batch:
			records = append(records, record)
		default:
			return records
		}
	}
}

func (d *CdxDbIndex) flushBatch() {
	records := d.collectBatch()

	if len(records) == 0 {
		return
	}

	_ = d.idIndex.Update(func(txn *badger.Txn) error {
		for _, r := range records {
			fileName := filepath.Base(r.filePath)
			storageRef := fmt.Sprintf("warcfile:%s:%d", fileName, r.offset)
			err := txn.Set([]byte(r.id), []byte(storageRef))
			if err != nil {
				log.Errorf("Failed to save storage ref in id index: %s: %s: %v", r.id, storageRef, err)
			}
		}
		return nil
	})

	_ = d.cdxIndex.Update(func(txn *badger.Txn) error {
		for _, r := range records {
			if r.cdx != nil {
				key := r.cdx.Ssu + " " + r.cdx.Sts + " " + r.cdx.Srt
				value, err := proto.Marshal(r.cdx)
				if err != nil {
					log.Errorf("Failed to marshal cdx index value: %s, %v", key, err)
					continue
				}
				err = txn.Set([]byte(key), value)
				if err != nil {
					log.Errorf("Failed to save cdx entry to database: %s: %v", key, err)
				}
			}
		}
		return nil
	})
}

// Resolve looks up warcId in id index and returns a storage ref, or an empty string if not found.
func (d *CdxDbIndex) Resolve(warcId string) (string, error) {
	var val []byte
	err := d.idIndex.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(warcId))
		if err != nil {
			return err
		}
		val, err = item.ValueCopy(nil)
		return err
	})
	return string(val), err
}

func (d *CdxDbIndex) GetFileInfo(fileName string) (*schema.Fileinfo, error) {
	val := &schema.Fileinfo{}
	err := d.fileIndex.View(func(txn *badger.Txn) error {
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

type PerItemFunction func(*badger.Item) (stopIteration bool)
type AfterIterationFunction func(txn *badger.Txn) error

func (d *CdxDbIndex) Search(key string, reverse bool, f PerItemFunction, a AfterIterationFunction) error {
	return d.cdxIndex.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		opts.Prefix = []byte(key)
		opts.Reverse = reverse
		it := txn.NewIterator(opts)
		defer it.Close()

		seekKey := key
		if reverse {
			seekKey += string(rune(0xff))
		}

		for it.Seek([]byte(seekKey)); it.ValidForPrefix([]byte(key)); it.Next() {
			item := it.Item()
			if f(item) {
				break
			}
		}
		return a(txn)
	})
}

func (d *CdxDbIndex) ListFileNames(fn PerItemFunction) error {
	opts := badger.DefaultIteratorOptions
	opts.PrefetchValues = false
	return walk(d.fileIndex, opts, fn)
}

func (d *CdxDbIndex) ListIds(fn PerItemFunction) error {
	opts := badger.DefaultIteratorOptions
	return walk(d.idIndex, opts, fn)
}

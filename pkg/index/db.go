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

package index

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v2"
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	gowarcpb "github.com/nlnwa/gowarc/proto"
	"github.com/nlnwa/gowarc/warcrecord"
	log "github.com/sirupsen/logrus"
)

const (
	NONE_MASK    = 0b000
	ID_DB_MASK   = 0b001
	FILE_DB_MASK = 0b010
	CDX_DB_MASK  = 0b100
	ALL_MASK     = 0b111
)

type record struct {
	id       string
	filePath string
	offset   int64
	cdx      *gowarcpb.Cdx
}

type index struct {
	db *badger.DB
}

func newIndex(dbDir string, file string, mask int32, typeMask int32) (index, error) {
	if mask&typeMask <= 0 {
		return index{nil}, nil
	}

	indexDir := path.Join(dbDir, file)
	db, err := openIndex(indexDir)
	if err != nil {
		return index{nil}, err
	}
	return index{db}, nil
}

func (i index) close() error {
	if i.db == nil {
		return nil
	}

	return i.db.Close()
}

func (i index) runGC(discardRatio float64) {
	if i.db == nil {
		return
	}

	for {
		err := i.db.RunValueLogGC(discardRatio)
		// TODO: code smell using error as branching mechanism
		if err != nil {
			break
		}
	}
}

func (i index) update(fn func(txn *badger.Txn) error) error {
	if i.db != nil {
		return i.db.Update(fn)
	}
	return nil
}

func (i index) view(fn func(txn *badger.Txn) error) error {
	if i.db != nil {
		return i.db.View(fn)
	}
	return nil
}

type DB struct {
	dir        string
	idIndex    index
	fileIndex  index
	cdxIndex   index
	gcInterval *time.Ticker

	// batch settings
	batchMaxSize int
	batchMaxWait time.Duration
	batchItems   []*record
	batchMutex   *sync.RWMutex
	// notifier channel
	batchFlushChan chan []*record
}

func NewIndexDb(dbDir string, mask int32) (*DB, error) {
	dbDir = path.Join(dbDir, "warcdb")

	batchMaxSize := 10000
	batchMaxWait := 5 * time.Second

	d := &DB{
		dir:            dbDir,
		gcInterval:     time.NewTicker(15 * time.Second),
		batchMaxSize:   batchMaxSize,
		batchMaxWait:   batchMaxWait,
		batchItems:     make([]*record, 0, batchMaxSize),
		batchMutex:     &sync.RWMutex{},
		batchFlushChan: make(chan []*record, 1),
	}

	// Init batch routines
	go func(flushJobs <-chan []*record) {
		for j := range flushJobs {
			d.AddBatch(j)
		}
	}(d.batchFlushChan)

	go func() {
		for {
			select {
			case <-time.Tick(d.batchMaxWait):
				d.Flush()
			}
		}
	}()

	// Open db
	var err error

	d.idIndex, err = newIndex(dbDir, "id-index", mask, ID_DB_MASK)
	if err != nil {
		return nil, err
	}

	d.fileIndex, err = newIndex(dbDir, "file-index", mask, FILE_DB_MASK)
	if err != nil {
		return nil, err
	}

	d.cdxIndex, err = newIndex(dbDir, "cdx-index", mask, CDX_DB_MASK)
	if err != nil {
		return nil, err
	}

	go func() {
		for range d.gcInterval.C {
			d.runValueLogGC(0.5)
		}
	}()

	return d, nil
}

func openIndex(indexDir string) (db *badger.DB, err error) {
	if err := os.MkdirAll(indexDir, 0777); err != nil {
		return nil, err
	}
	opts := badger.DefaultOptions(indexDir)
	opts.Logger = log.StandardLogger()
	db, err = badger.Open(opts)
	return
}

func (d *DB) runValueLogGC(discardRatio float64) {
	d.idIndex.runGC(discardRatio)
	d.fileIndex.runGC(discardRatio)
	d.cdxIndex.runGC(discardRatio)
}

func (d *DB) DeleteDb() {
	if err := os.RemoveAll(d.dir); err != nil {
		log.Fatal(err)
	}
}

func (d *DB) Close() {
	d.Flush()
	d.gcInterval.Stop()
	d.runValueLogGC(0.3)
	_ = d.idIndex.close()
	_ = d.fileIndex.close()
	_ = d.cdxIndex.close()
}

func (d *DB) Add(warcRecord warcrecord.WarcRecord, filePath string, offset int64) error {
	rec := &record{
		id:       warcRecord.WarcHeader().Get(warcrecord.WarcRecordID),
		filePath: filePath,
		offset:   offset,
	}

	var err error
	if warcRecord.Type() == warcrecord.RESPONSE || warcRecord.Type() == warcrecord.REVISIT {
		rec.cdx = NewCdxRecord(warcRecord, filePath, offset)
	}
	if err != nil {
		return err
	}

	d.batchMutex.Lock()
	d.batchItems = append(d.batchItems, rec)
	d.batchMutex.Unlock()
	if len(d.batchItems) >= d.batchMaxSize {
		d.Flush()
	}

	return nil
}

func (d *DB) UpdateFilePath(filePath string) {
	fileInfo := &gowarcpb.Fileinfo{}
	var err error
	fileInfo.Path, err = filepath.Abs(filePath)
	if err != nil {
		log.Errorf("%v", err)
	}
	fileInfo.Name = filepath.Base(fileInfo.Path)
	stat, err := os.Stat(fileInfo.Path)
	if err != nil {
		log.Errorf("%v", err)
	}
	fileInfo.Size = stat.Size()
	fileInfo.LastModified, err = ptypes.TimestampProto(stat.ModTime())
	if err != nil {
		log.Errorf("%v", err)
	}

	value, err := proto.Marshal(fileInfo)
	if err != nil {
		log.Errorf("%v", err)
		return
	}

	err = d.fileIndex.update(func(txn *badger.Txn) error {
		return txn.Set([]byte(fileInfo.Name), value)
	})
	if err != nil {
		log.Errorf("%v", err)
	}
}

func (d *DB) AddBatch(records []*record) {
	log.Debug("flushing batch to DB")

	var err error
	err = d.idIndex.update(func(txn *badger.Txn) error {
		for _, r := range records {
			r.filePath, err = filepath.Abs(r.filePath)
			if err != nil {
				log.Errorf("%v", err)
			}
			fileName := filepath.Base(r.filePath)
			storageRef := fmt.Sprintf("warcfile:%s:%d", fileName, r.offset)
			err := txn.Set([]byte(r.id), []byte(storageRef))
			if err != nil {
				log.Errorf("%v", err)
			}
		}
		return nil
	})
	if err != nil {
		log.Errorf("%v", err)
	}

	err = d.cdxIndex.update(func(txn *badger.Txn) error {
		for _, r := range records {
			if r.cdx != nil {
				key := r.cdx.Ssu + " " + r.cdx.Sts + " " + r.cdx.Srt
				value, err := proto.Marshal(r.cdx)
				if err != nil {
					log.Errorf("%v", err)
					continue
				}
				err = txn.Set([]byte(key), value)
				if err != nil {
					log.Errorf("%v", err)
				}
			}
		}
		return nil
	})
	if err != nil {
		log.Errorf("%v", err)
	}
}

func (d *DB) Flush() {
	d.batchMutex.RLock()
	defer d.batchMutex.RUnlock()

	if len(d.batchItems) <= 0 {
		return
	}

	copiedItems := d.batchItems
	d.batchItems = d.batchItems[:0]
	d.batchFlushChan <- copiedItems
}

func (d *DB) GetStorageRef(id string) (string, error) {
	var val []byte
	err := d.idIndex.view(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(id))
		if err != nil {
			return err
		}
		val, err = item.ValueCopy(nil)
		return err
	})
	return string(val), err
}

func (d *DB) GetFilePath(fileName string) (*gowarcpb.Fileinfo, error) {
	val := &gowarcpb.Fileinfo{}
	err := d.fileIndex.view(func(txn *badger.Txn) error {
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

func (d *DB) ListFileNames() ([]string, error) {
	var result []string
	opt := badger.DefaultIteratorOptions
	opt.PrefetchSize = 10
	opt.PrefetchValues = false
	var count int
	err := d.fileIndex.view(func(txn *badger.Txn) error {
		it := txn.NewIterator(opt)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			count++
			result = append(result, string(it.Item().KeyCopy(nil)))
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	fmt.Printf("Counted %d elements\n", count)
	return result, err
}

type PerItemFunction func(*badger.Item) (stopIteration bool)
type AfterIterationFunction func(txn *badger.Txn) error

func (d *DB) Search(key string, reverse bool, f PerItemFunction, a AfterIterationFunction) error {
	log.Debugf("Searching for key '%s'", key)

	err := d.cdxIndex.view(func(txn *badger.Txn) error {
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
	return err
}

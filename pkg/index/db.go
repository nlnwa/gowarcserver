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

type record struct {
	id       string
	filePath string
	offset   int64
	cdx      *gowarcpb.Cdx
}

type Db struct {
	dbDir        string
	idIndex      *badger.DB
	fileIndex    *badger.DB
	cdxIndex     *badger.DB
	dbGcInterval *time.Ticker

	// batch settings
	batchMaxSize int
	batchMaxWait time.Duration
	batchItems   []*record
	batchMutex   *sync.RWMutex
	// notifier channel
	batchFlushChan chan []*record
}

func NewIndexDb(dbDir string) (*Db, error) {
	dbDir = path.Join(dbDir, "warcdb")
	idIndexDir := path.Join(dbDir, "id-index")
	fileIndexDir := path.Join(dbDir, "file-index")
	cdxIndexDir := path.Join(dbDir, "cdx-index")

	batchMaxSize := 10000
	batchMaxWait := 5 * time.Second

	d := &Db{
		dbDir:          dbDir,
		dbGcInterval:   time.NewTicker(15 * time.Second),
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

	d.idIndex, err = openIndex(idIndexDir)
	if err != nil {
		return nil, err
	}

	d.fileIndex, err = openIndex(fileIndexDir)
	if err != nil {
		return nil, err
	}

	d.cdxIndex, err = openIndex(cdxIndexDir)
	if err != nil {
		return nil, err
	}

	go func() {
		for range d.dbGcInterval.C {
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

func (d *Db) runValueLogGC(discardRatio float64) {
	for {
		err := d.idIndex.RunValueLogGC(discardRatio)
		if err != nil {
			break
		}
	}
	for {
		err := d.fileIndex.RunValueLogGC(discardRatio)
		if err != nil {
			break
		}
	}
	for {
		err := d.cdxIndex.RunValueLogGC(discardRatio)
		if err != nil {
			break
		}
	}
}

func (d *Db) DeleteDb() {
	if err := os.RemoveAll(d.dbDir); err != nil {
		log.Fatal(err)
	}
}

func (d *Db) Close() {
	d.Flush()
	d.dbGcInterval.Stop()
	d.runValueLogGC(0.3)
	_ = d.idIndex.Close()
	_ = d.fileIndex.Close()
	_ = d.cdxIndex.Close()
}

func (d *Db) Add(warcRecord warcrecord.WarcRecord, filePath string, offset int64) error {
	record := &record{
		id:       warcRecord.WarcHeader().Get(warcrecord.WarcRecordID),
		filePath: filePath,
		offset:   offset,
	}

	var err error
	if warcRecord.Type() == warcrecord.RESPONSE || warcRecord.Type() == warcrecord.REVISIT {
		record.cdx = NewCdxRecord(warcRecord, filePath, offset)
	}
	if err != nil {
		return err
	}

	d.batchMutex.Lock()
	d.batchItems = append(d.batchItems, record)
	d.batchMutex.Unlock()
	if len(d.batchItems) >= d.batchMaxSize {
		d.Flush()
	}

	return nil
}

func (d *Db) UpdateFilePath(filePath string) {
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

	err = d.fileIndex.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(fileInfo.Name), value)
	})
	if err != nil {
		log.Errorf("%v", err)
	}
}

func (d *Db) AddBatch(records []*record) {
	log.Debug("flushing batch to DB")

	var err error
	err = d.idIndex.Update(func(txn *badger.Txn) error {
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

	err = d.cdxIndex.Update(func(txn *badger.Txn) error {
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

func (d *Db) Flush() {
	d.batchMutex.RLock()
	defer d.batchMutex.RUnlock()

	if len(d.batchItems) <= 0 {
		return
	}

	copiedItems := d.batchItems
	d.batchItems = d.batchItems[:0]
	d.batchFlushChan <- copiedItems
}

func (d *Db) GetStorageRef(id string) (string, error) {
	var val []byte
	err := d.idIndex.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(id))
		if err != nil {
			return err
		}
		val, err = item.ValueCopy(nil)
		return err
	})
	return string(val), err
}

func (d *Db) GetFilePath(fileName string) (*gowarcpb.Fileinfo, error) {
	val := &gowarcpb.Fileinfo{}
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

func (d *Db) ListFileNames() ([]string, error) {
	var result []string
	opt := badger.DefaultIteratorOptions
	opt.PrefetchSize = 10
	opt.PrefetchValues = false
	var count int
	err := d.fileIndex.View(func(txn *badger.Txn) error {
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

func (d *Db) Search(key string, reverse bool, f PerItemFunction, a AfterIterationFunction) error {
	log.Debugf("Searching for key '%s'", key)

	err := d.cdxIndex.View(func(txn *badger.Txn) error {
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

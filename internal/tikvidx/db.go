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
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/nlnwa/gowarcserver/index"
	"github.com/nlnwa/gowarcserver/schema"
	"github.com/nlnwa/gowarcserver/timestamp"
	"github.com/rs/zerolog/log"
	"github.com/tikv/client-go/v2/rawkv"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// KV represents a Key-Value pair.
type KV struct {
	K, V []byte
}

func (kv KV) String() string {
	return fmt.Sprintf("%s => %s (%v)", kv.K, kv.V, kv.V)
}

func (kv KV) ts() int64 {
	b := bytes.Split(kv.K, []byte{32})[1]
	ts, _ := time.Parse(timestamp.CDX, string(b))
	return ts.Unix()
}

type cdxKey string

func (k cdxKey) ts() string {
	return strings.Split(string(k), " ")[1]
}

var (
	dbPrefix   = "d"
	idPrefix   = "i"
	filePrefix = "f"
	cdxPrefix  = "c"
)

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
	dbName := dbPrefix + opts.Database

	// prefix all keys with name of database
	idPrefix = dbName + idPrefix
	filePrefix = dbName + filePrefix
	cdxPrefix = dbName + cdxPrefix

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
				db.flushBatch()
				return
			case <-ticker.C:
				db.flushBatch()
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
	fileInfo := new(schema.Fileinfo)

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

func (db *DB) putFileInfo(fi *schema.Fileinfo) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	k := []byte(filePrefix + fi.Name)
	v, err := proto.Marshal(fi)
	if err != nil {
		log.Error().Err(err).Msg("")
		return err
	}
	err = db.client.Put(ctx, k, v)
	if err != nil {
		log.Error().Err(err).Msgf("Failed to put file info: %s", fi.Name)
		return err
	}
	return nil
}

func (db *DB) getFileInfo(fileName string) (*schema.Fileinfo, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	key := []byte(filePrefix + fileName)
	val, err := db.client.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	if val == nil {
		return nil, nil
	}
	fi := new(schema.Fileinfo)
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
		db.flushBatch()
		db.batch <- rec
	}
}

func (db *DB) collectBatch() ([][]byte, [][]byte) {
	var keys [][]byte
	var values [][]byte
	for {
		select {
		case r := <-db.batch:
			id := idKV(r)
			cdx, err := cdxKV(r)
			if err != nil {
				log.Error().Err(err).Msgf("failed to marshal record: %v", r)
				continue
			}
			keys = append(keys, id.K, cdx.K)
			values = append(values, id.V, cdx.V)
		default:
			return keys, values
		}
	}
}

// flushBatch collects all records in the batch channel and updates the id and cdx indices.
func (db *DB) flushBatch() {
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
func idKV(r index.Record) KV {
	return KV{
		K: []byte(idPrefix + r.GetRid()),
		V: []byte(r.GetRef()),
	}
}

// cdxKV takes a record and returns a key-value pair for the cdx index.
func cdxKV(r index.Record) (KV, error) {
	ts := timestamp.TimeTo14(r.GetSts().AsTime())
	k := []byte(cdxPrefix + r.GetSsu() + " " + ts + " " + r.GetSrt())
	v, err := r.Marshal()
	if err != nil {
		return KV{}, err
	}
	return KV{K: k, V: v}, nil
}

func (db *DB) Write(rec index.Record) error {
	db.write(rec)
	return nil
}

func (db *DB) Index(path string) error {
	return db.addFile(path)
}

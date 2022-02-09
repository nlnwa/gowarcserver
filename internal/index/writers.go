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
	"github.com/bits-and-blooms/bloom/v3"
	"sync"

	"github.com/nlnwa/gowarc"
	"github.com/nlnwa/gowarcserver/internal/database"
	"github.com/nlnwa/gowarcserver/internal/surt"
	"github.com/nlnwa/gowarcserver/schema"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

type CdxDb struct {
	*database.CdxDbIndex
}

func (c *CdxDb) Index(fileName string) error {
	err := c.AddFile(fileName)
	if err != nil {
		return err
	}
	return ReadFile(fileName, c, gowarc.WithNoValidation())
}

type CdxJ struct {
}

func (c *CdxJ) Write(wr gowarc.WarcRecord, fileName string, offset int64) error {
	if wr.Type() == gowarc.Response {
		rec := schema.NewCdxRecord(wr, fileName, offset)
		cdxj := protojson.Format(rec)
		fmt.Printf("%s %s %s %s\n", rec.Ssu, rec.Sts, rec.Srt, cdxj)
	}
	return nil
}

func (c *CdxJ) Index(fileName string) error {
	return ReadFile(fileName, c, gowarc.WithNoValidation())
}

type CdxPb struct {
}

func (c *CdxPb) Write(wr gowarc.WarcRecord, fileName string, offset int64) error {
	if wr.Type() != gowarc.Response {
		return nil
	}

	rec := schema.NewCdxRecord(wr, fileName, offset)
	cdxpb, err := proto.Marshal(rec)
	if err != nil {
		return err
	}
	fmt.Printf("%s %s %s %s\n", rec.Ssu, rec.Sts, rec.Srt, cdxpb)

	return nil
}

func (c *CdxPb) Index(fileName string) error {
	return ReadFile(fileName, c, gowarc.WithNoValidation())
}

func NewTocWithBloom(n uint, fp float64) *Toc {
	return &Toc {
		bf:  bloom.NewWithEstimates(n, fp),
		m: new(sync.Mutex),
	}
}

type Toc struct {
	bf *bloom.BloomFilter
	m  *sync.Mutex
}

func (t Toc) Write(wr gowarc.WarcRecord, fileName string, offset int64) error {
	if wr.Type() == gowarc.Response {
		return nil
	}

	uri := wr.WarcHeader().Get(gowarc.WarcTargetURI)
	surthost, err := surt.SsurtHostname(uri)
	if err != nil {
		return nil
	}

	hasSurt := false
	if t.bf != nil {
		t.m.Lock()
		hasSurt = t.bf.TestOrAddString(surthost)
		t.m.Unlock()
	}
	if !hasSurt {
		fmt.Println(surthost)
	}

	return nil
}

func (c *Toc) Index(fileName string) error {
	return ReadFile(fileName, c, gowarc.WithNoValidation())
}

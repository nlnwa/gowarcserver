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
	"github.com/nlnwa/gowarc"
	"github.com/nlnwa/gowarcserver/internal/cdx"
	"github.com/nlnwa/gowarcserver/internal/database"
	"github.com/nlnwa/gowarcserver/internal/surt"
	"github.com/rs/zerolog/log"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"strconv"
	"strings"
	"sync"
	"time"
)

func warcRecordFilter(wr gowarc.WarcRecord) bool {
	// only write response and revisit records
	if wr.Type() == gowarc.Response || wr.Type() == gowarc.Revisit {
		// of type application/http
		if strings.HasPrefix(wr.WarcHeader().Get(gowarc.ContentType), gowarc.ApplicationHttp) {
			return true
		}
	}
	return false
}

func indexFile(fileName string, r RecordWriter) error {
	start := time.Now()

	count, total, err := ReadFile(fileName, r, warcRecordFilter,
		gowarc.WithSyntaxErrorPolicy(gowarc.ErrIgnore),
		gowarc.WithSpecViolationPolicy(gowarc.ErrIgnore),
	)
	log.Debug().Msgf("Indexed %5d of %5d records in %10v: %s\n", count, total, time.Since(start), fileName)
	return err
}

type CdxDb struct {
	*database.CdxDbIndex
}

func (c CdxDb) Index(fileName string) error {
	err := c.AddFile(fileName)
	if err != nil {
		return err
	}
	return indexFile(fileName, c)
}

type CdxJ struct {
}

func (c CdxJ) Write(wr gowarc.WarcRecord, fileName string, offset int64) error {
	rec := cdx.New(wr, fileName, offset)
	cdxj := protojson.Format(rec)
	fmt.Printf("%s %s %s %s\n", rec.Ssu, rec.Sts, rec.Srt, cdxj)

	return nil
}

func (c CdxJ) Index(fileName string) error {
	return indexFile(fileName, c)
}

type CdxPb struct {
}

func (c CdxPb) Write(wr gowarc.WarcRecord, fileName string, offset int64) error {
	rec := cdx.New(wr, fileName, offset)
	cdxpb, err := proto.Marshal(rec)
	if err != nil {
		return err
	}
	fmt.Printf("%s %s %s %s\n", rec.Ssu, rec.Sts, rec.Srt, cdxpb)

	return nil
}

func (c CdxPb) Index(fileName string) error {
	return indexFile(fileName, c)
}

type Toc struct {
	m sync.Mutex
	*bloom.BloomFilter
}

func (t *Toc) Write(wr gowarc.WarcRecord, fileName string, offset int64) error {
	uri := wr.WarcHeader().Get(gowarc.WarcTargetURI)
	surthost, err := surt.SsurtHostname(uri)
	if err != nil {
		return nil
	}
	date, err := wr.WarcHeader().GetTime(gowarc.WarcDate)
	if err != nil {
		return err
	}
	key := surthost + " " + strconv.Itoa(date.Year())

	t.m.Lock()
	hasSurt := t.BloomFilter.TestOrAddString(key)
	t.m.Unlock()

	if !hasSurt {
		fmt.Println(key)
	}

	return nil
}

func (t *Toc) Index(fileName string) error {
	return indexFile(fileName, t)
}

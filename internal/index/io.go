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
	"github.com/nlnwa/gowarc"
	"github.com/rs/zerolog/log"
	"io"
	"path/filepath"
	"strings"
	"time"
)

type recordFilter func(gowarc.WarcRecord) bool

// readFile reads a file using the supplied config and writes with a IndexWriter.
func readFile(path string, writer recordWriter, filter recordFilter, opts ...gowarc.WarcRecordOption) (int, int, error) {
	wf, err := gowarc.NewWarcFileReader(path, 0, opts...)
	if err != nil {
		return 0, 0, err
	}
	defer func() {
		_ = wf.Close()
	}()

	filename := filepath.Base(path)

	var prevOffset int64
	var prevWr gowarc.WarcRecord
	// write record from the previous iteration because we need to calculate record length
	write := func(offset int64) error {
		r, err := newRecord(prevWr, filename, prevOffset, offset-prevOffset)
		if err != nil {
			return err
		}
		return writer.Write(r)
	}

	count := 0
	total := 0

	for {
		wr, offset, validation, err := wf.Next()
		if prevWr != nil {
			if err := write(offset); err != nil {
				log.Warn().Err(err).Str("path", path).Int64("offset", offset).Msgf("Error writing index record")
			} else {
				count++
			}
			prevWr = nil
		}
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return count, total, fmt.Errorf("failed to get record number %d in %s at offset %d: %w", count, path, offset, err)
		}
		if !validation.Valid() {
			log.Warn().Err(validation).Str("path", path).Int64("offset", offset).Msgf("Invalid %s record: %s", wr.Type(), wr.RecordId())
		}
		if filter(wr) {
			prevWr = wr
		}
		total++
		prevOffset = offset
	}
	return count, total, err
}

type recordWriter interface {
	Write(record) error
}

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

func indexFile(fileName string, r recordWriter) error {
	start := time.Now()

	count, total, err := readFile(fileName, r, warcRecordFilter)

	log.Info().Msgf("Indexed %5d of %5d records in %10v: %s\n", count, total, time.Since(start), fileName)
	return err
}

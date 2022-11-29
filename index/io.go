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
	"io"
	"path/filepath"

	"github.com/nlnwa/gowarc"
	"github.com/rs/zerolog/log"
)

type recordFilter func(gowarc.WarcRecord, *gowarc.Validation) bool

type RecordWriter interface {
	Write(Record) error
}

// readFile reads, filters and writes records of a warc file to a record writer
func readFile(path string, writer RecordWriter, filter recordFilter, opts ...gowarc.WarcRecordOption) (int, int, error) {
	filename := filepath.Base(path)

	wf, err := gowarc.NewWarcFileReader(path, 0, opts...)
	if err != nil {
		return 0, 0, err
	}
	defer func() {
		_ = wf.Close()
	}()

	var prevOffset int64
	var prevWr gowarc.WarcRecord

	count := 0
	total := 0

	for {
		wr, offset, validation, err := wf.Next()
		if prevWr != nil {
			if r, err := newRecord(prevWr, filename, prevOffset, offset-prevOffset); err != nil {
				log.Error().Err(err).Msgf("Failed to create index record %s#%d", filename, prevOffset)
			} else if err = writer.Write(r); err != nil {
				log.Error().Err(err).Msgf("Failed to index record: %s#%d", filename, prevOffset)
			} else {
				count++
			}
			prevWr = nil
		}
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return count, total, fmt.Errorf("failed to read record #%d at %s#%d: %w", total, filename, offset, err)
		}
		if filter(wr, validation) {
			prevWr = wr
		}
		total++
		prevOffset = offset
	}
	return count, total, err
}

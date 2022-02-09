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
	log "github.com/rs/zerolog/log"
	"io"
)

type RecordWriter interface {
	Write(wr gowarc.WarcRecord, fileName string, offset int64) error
}

type Filter func(gowarc.WarcRecord) bool

// ReadFile reads a file using the supplied config and writes with a IndexWriter.
func ReadFile(filename string, writer RecordWriter, filter Filter, opts ...gowarc.WarcRecordOption) (int, int, error) {
	wf, err := gowarc.NewWarcFileReader(filename, 0, opts...)
	if err != nil {
		return 0, 0, err
	}
	defer func() {
		_ = wf.Close()
	}()

	count := 0
	total := 0

	for {
		wr, offset, validation, err := wf.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return count, total, fmt.Errorf("failed to get record number %d in %s at offset %d: %w", count, filename, offset, err)
		}
		if !validation.Valid() {
			log.Warn().Msg(validation.String())
		}
		if filter(wr) {
			_ = writer.Write(wr, filename, offset)
			count++
		}
		total++
	}
	return count, total, err
}

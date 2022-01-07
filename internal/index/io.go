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

	"github.com/nlnwa/gowarc"
	log "github.com/sirupsen/logrus"
)

type RecordWriter interface {
	Write(wr gowarc.WarcRecord, fileName string, offset int64) error
}

// ReadFile reads a file using the supplied config and writes with a IndexWriter.
func ReadFile(filename string, writer RecordWriter, opts ...gowarc.WarcRecordOption) error {
	wf, err := gowarc.NewWarcFileReader(filename, 0, opts...)
	if err != nil {
		return err
	}
	defer func() {
		_ = wf.Close()
	}()

	count := 0

	for {
		record, offset, validation, err := wf.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to get record number %d in %s at offset %d: %w", count, filename, offset, err)
		}
		if !validation.Valid() {
			log.Warn(validation.String())
		}
		_ = writer.Write(record, filename, offset)
		count++
	}
	return nil
}

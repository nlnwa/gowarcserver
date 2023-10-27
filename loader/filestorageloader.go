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

package loader

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/nlnwa/gowarc"
	"github.com/rs/zerolog/log"
)

type FileStorageLoader struct {
	FilePathResolver
}

type FilePathResolver interface {
	ResolvePath(filename string) (path string, err error)
}

func (f FileStorageLoader) Load(ctx context.Context, storageRef string) (record gowarc.WarcRecord, err error) {
	filePath, offset, err := f.parseStorageRef(storageRef)
	if err != nil {
		return nil, err
	}
	log.Debug().Str("storageRef", storageRef).Msgf("Loading record from file: %s, offset: %v", filePath, offset)

	wf, err := gowarc.NewWarcFileReader(filePath, offset,
		gowarc.WithSyntaxErrorPolicy(gowarc.ErrIgnore),
		gowarc.WithSpecViolationPolicy(gowarc.ErrIgnore))
	if err != nil {
		return nil, fmt.Errorf("failed to initialize warc reader: %s#%d, %w", filePath, offset, err)
	}

	go func() {
		<-ctx.Done()
		_ = wf.Close()
	}()

	record, offset, _, err = wf.Next()
	if err != nil {
		log.Error().Msgf("%s, offset %v\n", err, offset)
		return nil, fmt.Errorf("failed to read record: %s#%d: %w", filePath, offset, err)
	}
	return
}

// parseStorageRef parses a storageRef (eg. warcfile:filename#offset) into parts.
func (f FileStorageLoader) parseStorageRef(storageRef string) (filename string, offset int64, err error) {
	n := strings.IndexRune(storageRef, ':')
	if n == -1 {
		err = fmt.Errorf("invalid storage ref, missing scheme delimiter ':'")
		return
	}
	scheme := storageRef[:n]
	if scheme != "warcfile" {
		err = fmt.Errorf("invalid storage ref, scheme must be \"warcfile\", was: %s", scheme)
		return
	}
	storageRef = storageRef[n+1:]
	n = strings.IndexRune(storageRef, '#')
	if n == -1 {
		err = fmt.Errorf("invalid storage ref, missing offset delimiter '#'")
		return
	}
	filename = storageRef[:n]
	offset, err = strconv.ParseInt(storageRef[n+1:], 0, 64)
	if err != nil {
		err = fmt.Errorf("invalid storage ref, invalid offset: %w", err)
	}
	if f.FilePathResolver != nil {
		filename, err = f.FilePathResolver.ResolvePath(filename)
	}
	return
}

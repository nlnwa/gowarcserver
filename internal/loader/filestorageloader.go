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
	"github.com/nlnwa/gowarc"
	"github.com/rs/zerolog/log"
	"strconv"
	"strings"
)

type FileStorageLoader struct {
	FilePathResolver func(fileName string) (filePath string, err error)
}

func (f *FileStorageLoader) Load(ctx context.Context, storageRef string) (record gowarc.WarcRecord, err error) {
	filePath, offset, err := f.parseStorageRef(storageRef)
	if err != nil {
		return nil, err
	}
	log.Debug().Msgf("Loading record from file: %s, offset: %v", filePath, offset)

	opts := gowarc.WithStrictValidation()
	wf, err := gowarc.NewWarcFileReader(filePath, offset, opts)
	if err != nil {
		return nil, err
	}

	go func() {
		<-ctx.Done()
		log.Trace().Msgf("file: %v closed\n", filePath)
		_ = wf.Close()
	}()

	record, offset, validation, err := wf.Next()
	if !validation.Valid() {
		log.Warn().Msg(validation.String())
		return nil, fmt.Errorf("validation error in warcfile at offset %d", offset)
	}
	if err != nil {
		log.Error().Msgf("%s, offset %v\n", err, offset)
		return nil, err
	}
	return
}

func (f *FileStorageLoader) parseStorageRef(storageRef string) (fileName string, offset int64, err error) {
	p := strings.SplitN(storageRef, ":", 3)
	if len(p) != 3 || p[0] != "warcfile" {
		err = fmt.Errorf("storage ref '%s' can't be handled by FileStorageLoader", storageRef)
		return
	}
	fileName = p[1]
	offset, err = strconv.ParseInt(p[2], 0, 64)

	if f.FilePathResolver != nil {
		fileName, err = f.FilePathResolver(fileName)
	}
	return
}

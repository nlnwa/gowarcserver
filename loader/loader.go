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

package loader

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/nlnwa/gowarc"
	"github.com/rs/zerolog/log"
)

type StorageRefResolver interface {
	Resolve(ctx context.Context, warcId string) (storageRef string, err error)
}

type RecordLoader interface {
	Load(ctx context.Context, storageRef string) (gowarc.WarcRecord, io.Closer, error)
}

type WarcLoader interface {
	LoadById(context.Context, string) (gowarc.WarcRecord, io.Closer, error)
	LoadByStorageRef(context.Context, string) (gowarc.WarcRecord, io.Closer, error)
}

type Loader struct {
	StorageRefResolver
	RecordLoader
	NoUnpack bool
}

type ErrResolveRevisit struct {
	Profile   string
	TargetURI string
	Date      string
}

func (e ErrResolveRevisit) Error() string {
	return fmt.Sprintf("Resolving via Warc-Refers-To-Date and Warc-Refers-To-Target-URI failed: %s", e.String())
}

func (e ErrResolveRevisit) String() string {
	return fmt.Sprintf("Warc-Refers-To-Date: %s, Warc-Refers-To-Target-URI: %s, Warc-Profile: %s", e.Date, e.TargetURI, e.Profile)
}

// LoadById loads a record by warcId and returns the record and a closer.
//
// The closer must be closed by the caller when the record is no longer needed.
func (l *Loader) LoadById(ctx context.Context, warcId string) (gowarc.WarcRecord, io.Closer, error) {
	storageRef, err := l.StorageRefResolver.Resolve(ctx, warcId)
	if err != nil {
		return nil, nil, err
	}
	return l.LoadByStorageRef(ctx, storageRef)
}

// LoadByStorageRef loads a record from storageRef and returns the record and a closer.
//
// The closer must be closed by the caller when the record is no longer needed.
//
// If the record is a revisit record, the referred record will be loaded and merged with the revisit record.
//
// If the loader is set to not unpack, the closer will be closed and an error will be returned.
func (l *Loader) LoadByStorageRef(ctx context.Context, storageRef string) (gowarc.WarcRecord, io.Closer, error) {
	record, closer, err := l.RecordLoader.Load(ctx, storageRef)
	if err != nil {
		return nil, nil, err
	}
	if l.NoUnpack {
		defer closer.Close()
		return nil, nil, errors.New("loader set to not unpack")
	}

	//nolint:exhaustive
	switch record.Type() {
	case gowarc.Revisit:
		log.Debug().Str("storageRef", storageRef).
			Str("warcRefersTo", record.WarcHeader().Get(gowarc.WarcRefersTo)).
			Str("warcRefersToTargetURI", record.WarcHeader().Get(gowarc.WarcRefersToTargetURI)).
			Str("warcRefersToDate", record.WarcHeader().Get(gowarc.WarcRefersToDate)).
			Msg("Loader found a revisit record")
		warcRefersTo := record.WarcHeader().GetId(gowarc.WarcRefersTo)
		if warcRefersTo == "" {
			defer closer.Close()

			warcRefersToTargetURI := record.WarcHeader().Get(gowarc.WarcRefersToTargetURI)
			warcRefersToDate := record.WarcHeader().Get(gowarc.WarcRefersToDate)
			if warcRefersToTargetURI == "" {
				return nil, nil, fmt.Errorf("failed to resolve revisit record: neither WARC-Refers-To nor Warc-Refers-To-Target-URI is set")
			}
			if warcRefersToDate == "" {
				warcRefersToDate = record.WarcHeader().Get(gowarc.WarcDate)
			}
			return nil, nil, ErrResolveRevisit{
				Profile:   record.WarcHeader().Get(gowarc.WarcProfile),
				TargetURI: warcRefersToTargetURI,
				Date:      warcRefersToDate,
			}
		}

		// We can safely defer the closer to the end of this function
		// because we will return a new record that is a merge of the revisit record
		// and the referred record. The merge operation substitutes the block of the referred
		// record with the block of the revisit record so no more operations will be
		// performed on the warc file containing the revisit record.
		defer closer.Close()

		// revisitOf is the referred record that the revisit record refers to.
		var revisitOf gowarc.WarcRecord

		// Resolve the storage ref of the referred record
		storageRef, err = l.Resolve(ctx, warcRefersTo)
		if err != nil {
			return nil, nil, fmt.Errorf("unable to resolve referred Warc-Record-ID [%s]: %w", warcRefersTo, err)
		}
		// Load the referred record
		revisitOf, closer, err = l.RecordLoader.Load(ctx, storageRef)
		if err != nil {
			return nil, nil, err
		}
		// Merge the revisit record with the referred record
		record, err = record.Merge(revisitOf)
		if err != nil {
			// Close the closer of the referred record before returning the error
			defer closer.Close()
			return nil, nil, err
		}

	case gowarc.Continuation:
		log.Warn().Msg("Not implemented: storage ref resolved to a continuation record")
	}

	return record, closer, nil
}

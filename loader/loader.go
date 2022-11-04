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

	"github.com/nlnwa/gowarc"
	"github.com/rs/zerolog/log"
)

type StorageRefResolver interface {
	Resolve(warcId string) (storageRef string, err error)
}

type RecordLoader interface {
	Load(ctx context.Context, storageRef string) (wr gowarc.WarcRecord, err error)
}

type WarcLoader interface {
	LoadById(context.Context, string) (gowarc.WarcRecord, error)
	LoadByStorageRef(context.Context, string) (gowarc.WarcRecord, error)
}

type Loader struct {
	StorageRefResolver
	FileStorageLoader
	NoUnpack bool
}

type ErrResolveRevisit struct {
	Profile   string
	TargetURI string
	Date      string
}

func (e ErrResolveRevisit) Error() string {
	return fmt.Sprintf("Resolving via Warc-Refers-To-Date and Warc-Refers-To-Target-URI is not implemented: %s", e.String())
}

func (e ErrResolveRevisit) String() string {
	return fmt.Sprintf("Warc-Refers-To-Date: %s, Warc-Refers-To-Target-URI: %s, Warc-Profile: %s", e.Date, e.TargetURI, e.Profile)
}

func (l *Loader) LoadById(ctx context.Context, warcId string) (gowarc.WarcRecord, error) {
	storageRef, err := l.StorageRefResolver.Resolve(warcId)
	if err != nil {
		return nil, err
	}
	return l.LoadByStorageRef(ctx, storageRef)
}

func (l *Loader) LoadByStorageRef(ctx context.Context, storageRef string) (gowarc.WarcRecord, error) {
	record, err := l.FileStorageLoader.Load(ctx, storageRef)
	if err != nil {
		return nil, err
	}
	if l.NoUnpack {
		return nil, errors.New("loader set to not unpack")
	}

	var rtrRecord gowarc.WarcRecord

	//nolint:exhaustive
	switch record.Type() {
	case gowarc.Revisit:
		log.Debug().Msgf("Resolving revisit  %v -> %v", record.RecordId(), record.WarcHeader().Get(gowarc.WarcRefersTo))
		warcRefersTo := record.WarcHeader().GetId(gowarc.WarcRefersTo)
		if warcRefersTo == "" {
			return nil, ErrResolveRevisit{
				Profile:   record.WarcHeader().Get(gowarc.WarcProfile),
				TargetURI: record.WarcHeader().Get(gowarc.WarcRefersToTargetURI),
				Date:      record.WarcHeader().Get(gowarc.WarcRefersToDate),
			}
		}

		var revisitOf gowarc.WarcRecord
		storageRef, err = l.Resolve(warcRefersTo)
		if err != nil {
			return nil, fmt.Errorf("unable to resolve referred Warc-Record-ID [%s]: %w", warcRefersTo, err)
		}
		revisitOf, err = l.FileStorageLoader.Load(ctx, storageRef)
		if err != nil {
			return nil, err
		}

		rtrRecord, err = record.Merge(revisitOf)
		if err != nil {
			return nil, err
		}
	case gowarc.Continuation:
		// TODO continuation not implemented
		fallthrough
	default:
		rtrRecord = record
	}

	return rtrRecord, nil
}
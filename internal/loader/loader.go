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
	"errors"
	"fmt"
	"github.com/nlnwa/gowarc"
	"github.com/rs/zerolog/log"
)

type StorageRefResolver interface {
	Resolve(warcId string) (storageRef string, err error)
}

type RecordLoader interface {
	Load(context.Context, string) (gowarc.WarcRecord, error)
}

type Loader struct {
	Resolver StorageRefResolver
	Loader   RecordLoader
	NoUnpack bool
}

func (l *Loader) Load(ctx context.Context, warcId string) (gowarc.WarcRecord, error) {
	storageRef, err := l.Resolver.Resolve(warcId)
	if err != nil {
		return nil, err
	}
	record, err := l.Loader.Load(ctx, storageRef)
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
		log.Debug().Msgf("Resolving revisit  %v -> %v", record.WarcHeader().Get(gowarc.WarcRecordID), record.WarcHeader().Get(gowarc.WarcRefersTo))
		warcRefersTo := record.WarcHeader().Get(gowarc.WarcRefersTo)
		if warcRefersTo == "" {
			warcRefersToTargetURI := record.WarcHeader().Get(gowarc.WarcRefersToTargetURI)
			warcRefersToDate := record.WarcHeader().Get(gowarc.WarcRefersToDate)
			return nil, fmt.Errorf("revisit record is missing Warc-Refers-To header. Resolving via Warc-Refers-To-Target-URI [%s] and Warc-Refers-To-Date [%s] is not implemented", warcRefersToTargetURI, warcRefersToDate)
		}
		storageRef, err = l.Resolver.Resolve(warcRefersTo)
		if err != nil {
			return nil, fmt.Errorf("unable to resolve referred Warc-Record-ID [%s]: %w", warcRefersTo, err)
		}
		revisitOf, err := l.Loader.Load(ctx, storageRef)
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

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
	"bufio"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"path"

	"github.com/dgraph-io/badger/v3"
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
	ProxyUrl *url.URL
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

func (l *Loader) Load(ctx context.Context, warcId string) (gowarc.WarcRecord, error) {
	log.Debug().Msg("loader load")
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
			return nil, ErrResolveRevisit{
				Profile:   record.WarcHeader().Get(gowarc.WarcProfile),
				TargetURI: record.WarcHeader().Get(gowarc.WarcRefersToTargetURI),
				Date:      record.WarcHeader().Get(gowarc.WarcRefersToDate),
			}
		}

		var revisitOf gowarc.WarcRecord
		storageRef, err = l.Resolver.Resolve(warcRefersTo)
		// if the record is missing from out DB and a proxy is configured, then we should
		// ask the proxy to get the revisitOf record for us
		if errors.Is(err, badger.ErrKeyNotFound) && l.ProxyUrl != nil {
			reqUrl := *l.ProxyUrl
			reqUrl.Path = path.Join(reqUrl.Path, "id", warcRefersTo)

			log.Debug().Msgf("attempt to get record from proxy url %s", reqUrl.String())
			req, err := http.NewRequestWithContext(ctx, "GET", reqUrl.String(), nil)
			if err != nil {
				return nil, err
			}

			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				return nil, err
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				return nil, fmt.Errorf("resolve revisit from proxy expected %d got %d", http.StatusOK, resp.StatusCode)
			}

			warcUnmarshaler := gowarc.NewUnmarshaler(
				gowarc.WithSyntaxErrorPolicy(gowarc.ErrIgnore),
				gowarc.WithSpecViolationPolicy(gowarc.ErrIgnore),
			)
			bodyIoReader := bufio.NewReader(resp.Body)
			revisitOf, _, _, err = warcUnmarshaler.Unmarshal(bodyIoReader)
			if err != nil {
				return nil, err
			}
		} else if err != nil {
			return nil, fmt.Errorf("unable to resolve referred Warc-Record-ID [%s]: %w", warcRefersTo, err)
		} else {
			// in the event that it managed to load record locally we do that instead
			revisitOf, err = l.Loader.Load(ctx, storageRef)
			if err != nil {
				return nil, err
			}
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

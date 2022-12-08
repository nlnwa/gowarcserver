/*
 * Copyright 2020 National Library of Norway.
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
	"fmt"
	"sync"

	"github.com/bits-and-blooms/bloom/v3"
	"github.com/nlnwa/gowarcserver/surt"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

type Cdx struct{}

func (c Cdx) Write(rec Record) error {
	cdxj := protojson.Format(rec)
	fmt.Printf("%s %s %s %s\n", rec.Ssu, rec.Sts, rec.Srt, cdxj)

	return nil
}

type CdxJ struct{}

func (c CdxJ) Write(rec Record) error {
	cdxj := protojson.Format(rec)
	fmt.Printf("%s %s %s %s\n", rec.Ssu, rec.Sts, rec.Srt, cdxj)

	return nil
}

type CdxPb struct{}

func (c CdxPb) Write(rec Record) error {
	cdxpb, err := proto.Marshal(rec)
	if err != nil {
		return err
	}
	fmt.Printf("%s %s %s %s\n", rec.Ssu, rec.Sts, rec.Srt, cdxpb)

	return nil
}

type Toc struct {
	m sync.Mutex
	*bloom.BloomFilter
}

func (t *Toc) Write(rec Record) error {
	uri := rec.Uri
	surthost, err := surt.UrlToSsurtHostname(uri)
	if err != nil {
		return nil
	}

	t.m.Lock()
	hasSurt := t.BloomFilter.TestOrAddString(surthost)
	t.m.Unlock()

	if !hasSurt {
		fmt.Println(surthost)
	}

	return nil
}

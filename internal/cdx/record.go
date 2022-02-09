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

package cdx

import (
	"bufio"
	"bytes"
	"errors"
	"github.com/nlnwa/gowarc"
	"github.com/nlnwa/gowarcserver/internal/surt"
	"github.com/nlnwa/gowarcserver/internal/timestamp"
	"github.com/nlnwa/gowarcserver/schema"
	"github.com/rs/zerolog/log"
	"io"
	"net/http"
	"strconv"
)

// New parses a WARC record to a Cdx record
func New(wr gowarc.WarcRecord, fileName string, offset int64) (cdx *schema.Cdx) {
	cdx = &schema.Cdx{
		Uri: wr.WarcHeader().Get(gowarc.WarcTargetURI),
		Sha: wr.WarcHeader().Get(gowarc.WarcPayloadDigest),
		Dig: wr.WarcHeader().Get(gowarc.WarcPayloadDigest),
		Ref: "warcfile:" + fileName + "#" + strconv.FormatInt(offset, 10),
		Rid: wr.WarcHeader().Get(gowarc.WarcRecordID),
		Cle: wr.WarcHeader().Get(gowarc.ContentLength),
		Rct: wr.WarcHeader().Get(gowarc.WarcConcurrentTo),
		Rou: wr.WarcHeader().Get(gowarc.WarcRefersToTargetURI),
		Rod: wr.WarcHeader().Get(gowarc.WarcRefersToDate),
		Roi: wr.WarcHeader().Get(gowarc.WarcRefersTo),
	}
	if ssu, err := surt.SsurtString(wr.WarcHeader().Get(gowarc.WarcTargetURI), true); err == nil {
		cdx.Ssu = ssu
	}
	cdx.Sts, _ = timestamp.To14(wr.WarcHeader().Get(gowarc.WarcDate))
	cdx.Srt = wr.Type().String()

	// nolint:exhaustive
	switch wr.Type() {
	case gowarc.Response:
		if block, ok := wr.Block().(gowarc.HttpResponseBlock); ok {
			header := block.HttpHeader()
			cdx.Mct = header.Get("Content-Type")
			cdx.Ple = header.Get("Content-Length")
			cdx.Hsc = strconv.Itoa(block.HttpStatusCode())
		}
	case gowarc.Revisit:
		r, err := wr.Block().RawBytes()
		if err != nil {
			log.Warn().Msgf("Failed to get raw bytes of revisit block: %v", err)
			return
		}
		b, err := io.ReadAll(r)
		if err != nil {
			log.Warn().Msgf("Failed to read bytes of revisit block: %v", err)
			return
		}
		amended := false
		for {
			// try to read revisit block as HTTP headers
			resp, err := http.ReadResponse(bufio.NewReader(bytes.NewReader(b)), nil)
			if err != nil {
				if !amended && errors.Is(err, io.ErrUnexpectedEOF) {
					b = append(b, '\r', '\n')
					amended = true
					continue
				}
				log.Warn().Msgf("Failed to parse revisit block as HTTP headers: %v", err)
				return
			}
			defer resp.Body.Close()
			cdx.Mct = resp.Header.Get("Content-Type")
			cdx.Cle = resp.Header.Get("Content-Length")
			cdx.Hsc = strconv.Itoa(resp.StatusCode)
			return
		}
	}
	return
}

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
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/nlnwa/gowarc"
	"github.com/nlnwa/gowarcserver/schema"
	"github.com/nlnwa/gowarcserver/surt"
	"github.com/rs/zerolog/log"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Record struct {
	*schema.Cdx
}

func (r Record) String() string {
	return fmt.Sprintf("%s %s", r.Ref, r.Uri)
}

// newRecord constructs a Record from wr, filename, offset and length.
func newRecord(wr gowarc.WarcRecord, filename string, offset int64, length int64) (rec Record, err error) {
	cle, err := wr.WarcHeader().GetInt64(gowarc.ContentLength)
	if err != nil {
		return rec, fmt.Errorf("failed to parse WARC header field '%s': %w", gowarc.ContentLength, err)
	}
	rec.Cdx = &schema.Cdx{
		Uri: wr.WarcHeader().Get(gowarc.WarcTargetURI),
		// Sha: wr.WarcHeader().Get(gowarc.WarcPayloadDigest),
		Dig: wr.WarcHeader().Get(gowarc.WarcPayloadDigest),
		Ref: "warcfile" + ":" + filename + "#" + strconv.FormatInt(offset, 10),
		Rid: wr.RecordId(),
		Cle: cle,
		Rle: length,
		Rct: wr.WarcHeader().GetId(gowarc.WarcConcurrentTo),
	}
	if ssu, err := surt.StringToSsurt(rec.Uri); err != nil {
		return rec, fmt.Errorf("failed to convert url '%s' to ssurt: %w", rec.Uri, err)
	} else {
		rec.Ssu = ssu
	}
	t, err := wr.WarcHeader().GetTime(gowarc.WarcDate)
	if err != nil {
		return rec, fmt.Errorf("failed to parse WARC header field '%s': %w", gowarc.WarcDate, err)
	}
	rec.Sts = timestamppb.New(t)

	rec.Srt = wr.Type().String()

	// nolint:exhaustive
	switch wr.Type() {
	case gowarc.Response:
		if block, ok := wr.Block().(gowarc.HttpResponseBlock); ok {
			header := block.HttpHeader()
			if header == nil {
				return
			}
			rec.Hsc = int32(block.HttpStatusCode())
			rec.Mct = header.Get("Content-Type")
			cl := header.Get("Content-Length")
			if len(cl) > 0 {
				var err error
				rec.Ple, err = strconv.ParseInt(cl, 10, 64)
				if err != nil {
					log.Warn().Msgf("Failed to parse HTTP header field 'Content-Length' as int64: %v", err)
				}
			}
		}
	case gowarc.Revisit:
		rec.Rou = wr.WarcHeader().Get(gowarc.WarcRefersToTargetURI)
		if t, err := wr.WarcHeader().GetTime(gowarc.WarcRefersToDate); err == nil {
			rec.Rod = timestamppb.New(t)
		}
		rec.Roi = wr.WarcHeader().GetId(gowarc.WarcRefersTo)

		r, bErr := wr.Block().RawBytes()
		if bErr != nil {
			log.Warn().Msgf("Failed to get raw bytes of revisit block: %v", bErr)
			return
		}
		b, rErr := io.ReadAll(r)
		if rErr != nil {
			log.Warn().Msgf("Failed to read bytes of revisit block: %v", rErr)
			return
		}
		amended := false
		var resp *http.Response
		defer func() {
			if resp != nil {
				_ = resp.Body.Close()
			}
		}()
		for {
			// try to read revisit block as HTTP headers
			resp, err = http.ReadResponse(bufio.NewReader(bytes.NewReader(b)), nil)
			if err != nil {
				// make one attempt at fixing missing CRLF after HTTP headers
				if !amended && errors.Is(err, io.ErrUnexpectedEOF) {
					b = append(b, '\r', '\n')
					amended = true
					continue
				}
				log.Warn().Msgf("Failed to parse revisit block as HTTP headers: %v", err)
				break
			}
			rec.Hsc = int32(resp.StatusCode)
			rec.Mct = resp.Header.Get("Content-Type")
			cl := resp.Header.Get("Content-Length")
			if len(cl) > 0 {
				var err error
				rec.Ple, err = strconv.ParseInt(cl, 10, 64)
				if err != nil {
					log.Warn().Msgf("Failed to parse HTTP header field 'Content-Length' as int64: %v", err)
				}
			}
			break
		}
		// fallback in case the revisit record payload is empty
		if rec.Mct == "" {
			rec.Mct = "warc/revisit"
		}
	}
	return
}

// Marshal is a wrapper around proto.Marshal that can handle invalid UTF-8 runes in the MIME type.
func (r Record) Marshal() ([]byte, error) {
	value, err := proto.Marshal(r)
	if err != nil && strings.HasSuffix(err.Error(), "contains invalid UTF-8") {
		// sanitize MIME type
		r.Mct = HandleInvalidUtf8String(r.GetMct())
		// and retry
		value, err = proto.Marshal(r)
	}
	return value, err
}

// HandleInvalidUtf8String removes invalid utf-8 runes from a string.
func HandleInvalidUtf8String(s string) string {
	if !utf8.ValidString(s) {
		v := make([]rune, 0, len(s))
		for i, r := range s {
			if r == utf8.RuneError {
				_, size := utf8.DecodeRuneInString(s[i:])
				if size == 1 {
					continue
				}
			}
			v = append(v, r)
		}
		return string(v)
	}
	return s
}

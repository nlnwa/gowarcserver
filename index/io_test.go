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
	"os"
	"path"
	"testing"

	"github.com/nlnwa/gowarc"
)

func TestReadFile(t *testing.T) {
	// same as testdata/example.warc except removed gzip content because of illegal go str characters
	testFileContent := []byte(`WARC/1.0
WARC-Date: 2017-03-06T04:03:53Z
WARC-Record-ID: <urn:uuid:e9a0cecc-0221-11e7-adb1-0242ac120008>
WARC-Type: warcinfo
Content-Length: 0`)

	filepath := path.Join(t.TempDir(), "test.warc")
	file, err := os.Create(filepath)
	if err != nil {
		t.Fatalf("Failed to create testfile at '%s'", filepath)
	}
	defer file.Close()

	_, err = file.Write(testFileContent)
	if err != nil {
		t.Fatalf("Failed to write to testfile at '%s'", filepath)
	}

	err = file.Sync()
	if err != nil {
		t.Fatalf("Failed to sync testfile at '%s'", filepath)
	}

	tests := []struct {
		format string
		writer recordWriter
	}{
		{
			"cdxj",
			new(CdxJ),
		},
		{

			"cdxpb",
			new(CdxPb),
		},
		{
			"toc",
			new(Toc),
		},
	}

	for _, tt := range tests {
		t.Run(tt.format, func(t *testing.T) {
			_, _, err = readFile(filepath, tt.writer, func(gowarc.WarcRecord) bool { return true })
			if err != nil {
				t.Errorf("Unexpected error: %v", err)
			}
		})
	}
}

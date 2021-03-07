package index

import (
	"fmt"
	"os"
	"path"
	"reflect"
	"testing"

	"github.com/nlnwa/gowarcserver/pkg/index"
	log "github.com/sirupsen/logrus"
)

func TestParseFormat(t *testing.T) {
	tests := []struct {
		name       string
		format     string
		expected   reflect.Type
		errorState bool
	}{
		{
			"'cdx' results in CdxLegacy writer",
			"cdx",
			reflect.TypeOf((*index.CdxLegacy)(nil)),
			false,
		},
		{
			"'cdxj' results in CdxJ writer",
			"cdxj",
			reflect.TypeOf((*index.CdxJ)(nil)),
			false,
		},
		{
			"'db' results in CdxDb writer",
			"db",
			reflect.TypeOf((*index.CdxDb)(nil)),
			false,
		},
		{
			"'cdxpb' results in CdxPd writer",
			"cdxpb",
			reflect.TypeOf((*index.CdxPb)(nil)),
			false,
		},
		{
			"'cd' results in error",
			"cd",
			nil,
			true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseFormat(tt.format)
			if err != nil && !tt.errorState {
				t.Errorf("Unexpected failure: %v", err)
			} else if err == nil && tt.errorState {
				t.Errorf("Expected error parsing '%v', got type %T", tt.format, got)
			}

			if reflect.TypeOf(got) != tt.expected {
				t.Errorf("Expected %v, got %v", tt.expected, got)
			}
		})
	}
}

// TODO: this was hard to write tests for and therefore ReadFile
//		 should probably be refactored
func TestReadFile(t *testing.T) {
	log.SetLevel(log.WarnLevel)
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
	// This is not strictly needed because of tmp, but to be platform agnostic it might be a good idea
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
		writerFormat string
		writer       index.CdxWriter
	}{
		{
			"cdx",
			&index.CdxLegacy{},
		},
		{
			"cdxj",
			&index.CdxJ{},
		},
		{

			"cdxpd",
			&index.CdxPb{},
		},
		{
			"db",
			&index.CdxDb{},
		},
	}

	for _, tt := range tests {
		testName := fmt.Sprintf("Readfile: %T successfully indexes", tt.writer)
		t.Run(testName, func(t *testing.T) {
			c := &conf{
				filepath,
				tt.writerFormat,
				tt.writer,
			}
			c.writer.Init()
			defer c.writer.Close()

			err := ReadFile(c)
			if err != nil {
				t.Errorf("Unexpected failure: %v", err)
			}

		})
	}
}

package index

import (
	"github.com/nlnwa/gowarc"
	"os"
	"path"
	"testing"

	"github.com/nlnwa/gowarcserver/internal/database"

	"github.com/dgraph-io/badger/v3/options"
	log "github.com/sirupsen/logrus"
)

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
		writer RecordWriter
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
			"cdxdb",
			func() RecordWriter {
				db, err := database.NewCdxIndexDb(database.WithDir(t.TempDir()), database.WithCompression(options.None))
				if err != nil {
					t.Fatal(err)
				}
				return &CdxDb{CdxDbIndex: db}
			}(),
		},
		{
			"toc",
			new(Toc),
		},
	}

	for _, tt := range tests {
		t.Run(tt.format, func(t *testing.T) {
			if t, ok := tt.writer.(*CdxDb); ok {
				defer t.Close()
			}
			err = ReadFile(filepath, tt.writer, gowarc.WithNoValidation())
			if err != nil {
				t.Errorf("Unexpected error: %v", err)
			}
		})
	}
}

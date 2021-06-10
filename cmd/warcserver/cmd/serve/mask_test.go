package serve_test

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/nlnwa/gowarcserver/cmd/warcserver/cmd/serve"
	"github.com/nlnwa/gowarcserver/pkg/index"
)

func TestConfigToDBMask(t *testing.T) {
	tests := []struct {
		noIdDB   bool
		noFileDB bool
		noCdxDb  bool
		expected int32
	}{
		{
			false,
			false,
			false,
			index.ALL_MASK,
		},
		{
			true,
			false,
			false,
			index.FILE_DB_MASK | index.CDX_DB_MASK,
		},
		{
			false,
			true,
			false,
			index.ID_DB_MASK | index.CDX_DB_MASK,
		},
		{
			false,
			false,
			true,
			index.ID_DB_MASK | index.FILE_DB_MASK,
		},
		{
			true,
			true,
			false,
			index.CDX_DB_MASK,
		},
		{
			true,
			false,
			true,
			index.FILE_DB_MASK,
		},
		{
			false,
			true,
			true,
			index.ID_DB_MASK,
		},
		{
			true,
			true,
			true,
			index.NONE_MASK,
		},
	}

	for _, tt := range tests {
		bits := strconv.FormatInt(int64(tt.expected), 2)
		testName := fmt.Sprintf("%t, %t, %t results in 0b%s", tt.noIdDB, tt.noFileDB, tt.noCdxDb, bits)
		t.Run(testName, func(t *testing.T) {
			mask := serve.ConfigToDBMask(tt.noIdDB, tt.noFileDB, tt.noCdxDb)
			if mask != tt.expected {
				bitMask := strconv.FormatInt(int64(mask), 2)
				t.Errorf("Expected 0b%s got 0b%s", bits, bitMask)
			}
		})
	}
}

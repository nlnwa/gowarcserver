package serve_test

import (
	"fmt"
	"github.com/nlnwa/gowarcserver/pkg/index"
	"net/url"
	"strconv"
	"testing"

	"github.com/nlnwa/gowarcserver/cmd/warcserver/cmd/serve"
)

func TestBuildUrlSlice(t *testing.T) {
	tests := []struct {
		name           string
		urlStrs        []string
		expectedUrls   []url.URL
		expectedErrors int
	}{
		{
			"parsing empty slice",
			[]string{},
			[]url.URL{},
			0,
		},
		{
			"parsing multiple urls",
			[]string{
				"http://192.148.38.150:8888",
				"http://192.148.38.150:7777",
			},
			[]url.URL{
				{
					Scheme: "http",
					Host:   "192.148.38.150:8888",
				},
				{
					Scheme: "http",
					Host:   "192.148.38.150:7777",
				},
			},
			0,
		},
		{
			"parsing invalid url",
			[]string{
				"1.22.333:4444",
			},
			[]url.URL{},
			1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if len(tt.urlStrs)-tt.expectedErrors != len(tt.expectedUrls) {
				t.Errorf("test url slice and string slice does not have the same length")
				return
			}

			urls := serve.BuildUrlSlice(tt.urlStrs)
			println(urls)
			for i, url := range urls {
				expected := tt.expectedUrls[i]
				if url != expected {
					t.Errorf("Expected %v got %v", expected, url)
				}
			}
		})
	}
}


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

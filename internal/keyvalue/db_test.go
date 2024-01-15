package keyvalue

import (
	"testing"

	"github.com/nlnwa/gowarcserver/timestamp"
)

// Test CdxKey.Domain
func TestCdxKey(t *testing.T) {
	tests := []struct {
		key    CdxKey
		domain string
		path   string
		scheme string
		ts     string
	}{
		{
			key:    []byte("test,example,/path 20200101000000 :http: response"),
			domain: "test,example,",
			path:   "/path",
			scheme: "http",
			ts:     "20200101000000",
		},
		{
			key:    []byte("test,example,/ 20200101000000"),
			domain: "test,example,",
			path:   "/",
			scheme: "",
			ts:     "20200101000000",
		},
	}

	for _, test := range tests {
		t.Run(test.key.String(), func(t *testing.T) {
			if string(test.key.Domain()) != test.domain {
				t.Errorf("Domain() = %v, want %v", test.key.Domain(), test.domain)
			}
			if string(test.key.Path()) != test.path {
				t.Errorf("Path() = %v, want %v", test.key.Path(), test.path)
			}
			if string(test.key.Scheme()) != test.scheme {
				t.Errorf("Scheme() = %v, want %v", test.key.Scheme(), test.scheme)
			}
			ts, _ := timestamp.Parse(test.ts)
			if test.key.Time() != ts {
				t.Errorf("Timestamp() = %v, want %v", ts, test.ts)
			}
		})
	}
}

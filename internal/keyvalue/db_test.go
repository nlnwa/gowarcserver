package keyvalue

import (
	"testing"

	"github.com/nlnwa/gowarcserver/timestamp"
)

// Test CdxKey.Domain
func TestCdxKey(t *testing.T) {
	tests := []struct {
		key        CdxKey
		domain     string
		path       string
		ts         string
		port       string
		scheme     string
		userinfo   string
		recordType string
	}{
		{
			key:        []byte("test,example,/path 20200101000000 :http: response"),
			domain:     "test,example,",
			path:       "/path",
			port:       "",
			scheme:     "http",
			userinfo:   "",
			ts:         "20200101000000",
			recordType: "response",
		},
		{
			key:        []byte("test,example,/ 20200101000000"),
			domain:     "test,example,",
			path:       "/",
			port:       "",
			scheme:     "",
			userinfo:   "",
			ts:         "20200101000000",
			recordType: "",
		},
		{
			key:        []byte("test,example,/path 20200101000000 8080:http@gammalost: response"),
			domain:     "test,example,",
			path:       "/path",
			port:       "8080",
			scheme:     "http",
			userinfo:   "gammalost",
			ts:         "20200101000000",
			recordType: "response",
		},
		{
			key:        []byte("test,example,/path 20200101000000 8080:http@user:password: response"),
			domain:     "test,example,",
			path:       "/path",
			port:       "8080",
			scheme:     "http",
			userinfo:   "user:password",
			ts:         "20200101000000",
			recordType: "response",
		},
		{
			key:        []byte("test,example,/path 20200101000000 8080:http@user:pass:word: response"),
			domain:     "test,example,",
			path:       "/path",
			port:       "8080",
			scheme:     "http",
			userinfo:   "user:pass:word",
			ts:         "20200101000000",
			recordType: "response",
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
			ts, _ := timestamp.Parse(test.ts)
			if test.key.Time() != ts {
				t.Errorf("Timestamp() = %v, want %v", ts, test.ts)
			}
			if string(test.key.Port()) != test.port {
				t.Errorf("Port() = %v, want %v", test.key.Port(), test.port)
			}
			if string(test.key.Scheme()) != test.scheme {
				t.Errorf("Scheme() = %v, want %v", test.key.Scheme(), test.scheme)
			}
			if string(test.key.UserInfo()) != test.userinfo {
				t.Errorf("UserInfo() = %v, want %v", test.key.UserInfo(), test.userinfo)
			}
			if string(test.key.ResponseType()) != test.recordType {
				t.Errorf("ResponseType() = %v, want %v", test.key.ResponseType(), test.recordType)
			}
		})
	}
}

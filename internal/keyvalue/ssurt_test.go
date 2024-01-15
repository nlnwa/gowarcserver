package keyvalue

import (
	"testing"
)

func TestSplitSurt(t *testing.T) {
	tests := []struct {
		ssurt             string
		host              string
		schemeAndUserinfo string
		path              string
	}{
		{
			"no,nb,//8080:http@user:password:/path?query#fragment",
			"no,nb,",
			"8080:http@user:password:",
			"/path?query#fragment",
		},
		{
			"no,nb,//:http@user:password:/path?query#fragment",
			"no,nb,",
			":http@user:password:",
			"/path?query#fragment",
		},
		{
			"no,nb,//:http:/path?query#fragment",
			"no,nb,",
			":http:",
			"/path?query#fragment",
		},
		{
			"no,nb,//:http:/path",
			"no,nb,",
			":http:",
			"/path",
		},
		{
			"no,nb,//:http:/",
			"no,nb,",
			":http:",
			"/",
		},
		{
			"no,nb,",
			"no,nb,",
			"",
			"",
		},
		{
			"no,nb,//",
			"no,nb,",
			"",
			"/",
		},
	}

	for _, test := range tests {
		t.Run(test.ssurt, func(t *testing.T) {
			host, schemeAndUserinfo, path := SplitSSURT(test.ssurt)
			if host != test.host {
				t.Errorf("want '%s', got '%s'", test.host, host)
			}
			if schemeAndUserinfo != test.schemeAndUserinfo {
				t.Errorf("want '%s', got '%s'", test.schemeAndUserinfo, schemeAndUserinfo)
			}
			if path != test.path {
				t.Errorf("want '%s', got '%s'", test.path, path)
			}
		})
	}
}

// Test DeSurtDomain
func TestDeSurtDomain(t *testing.T) {
	tests := []struct {
		surtDomain string
		domain     string
	}{
		{
			"test,example,",
			"example.test",
		},
		{
			"test,example,www,",
			"www.example.test",
		},
	}
	for _, test := range tests {
		t.Run(test.surtDomain, func(t *testing.T) {
			domain := deSurtDomain(test.surtDomain)
			if domain != test.domain {
				t.Errorf("want '%s', got '%s'", test.domain, domain)
			}
		})
	}
}

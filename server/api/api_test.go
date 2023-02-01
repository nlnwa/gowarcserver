package api

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/nlnwa/gowarcserver/surt"
)

func TestParse(t *testing.T) {
	domains := []string{
		"no",
		"kommune.no",
		"nb.no",
	}

	for _, domain := range domains {
		u, _ := url.Parse("http://example.test/")
		values := u.Query()
		values.Set("url", domain)
		values.Set("matchType", "domain")
		u.RawQuery = values.Encode()

		r := &http.Request{URL: u}

		a, err := Parse(r)
		if err != nil {
			t.Error(err)
		}

		got := SearchAPI{a}.Key()
		want := MatchType(surt.UrlToSsurt(a.Urls[0]), MatchTypeDomain)
		if got != want {
			t.Errorf("Got: '%s', Want: '%s'", got, want)
		}
	}

}

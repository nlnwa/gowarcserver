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
		"http://kommune.no",
		"http://nb.no",
	}

	for _, domain := range domains {
		u, err := url.Parse("http://example.test/")
		if err != nil {
			t.Fatal(err)
		}
		values := u.Query()
		values.Set("url", domain)
		values.Set("matchType", "domain")
		u.RawQuery = values.Encode()

		t.Log(u.String())
		r := &http.Request{URL: u}

		a, err := Parse(r)
		if err != nil {
			t.Error(err)
			continue
		}

		got := SearchAPI{CoreAPI: a}.Key()
		want := MatchType(surt.UrlToSsurt(a.Urls[0]), MatchTypeDomain)
		if got != want {
			t.Errorf("Got: '%s', Want: '%s'", got, want)
		}
	}

}

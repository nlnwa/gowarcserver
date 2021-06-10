package serve_test

import (
	"net/url"
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

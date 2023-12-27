package api

import (
	"errors"
	"net/http"
	"net/url"
	"reflect"
	"testing"
)

type searchParams struct {
	url       string
	matchType string
	limit     string
	from      string
	to        string
	closest   string
	sort      string
}

func TestParse(t *testing.T) {
	tests := []struct {
		query searchParams
		want  *CoreAPI
		err   error
	}{
		// invalid matchType should error
		{
			query: searchParams{
				url:       "http://example.test/01",
				matchType: "not a matchtype",
			},
			want: &CoreAPI{},
			err:  errors.New("invalid matchtype"),
		},
		// invalid sort should error
		{
			query: searchParams{
				url:  "http://example.test/02",
				sort: "not a sort",
			},
			want: &CoreAPI{},
			err:  errors.New("invalid sort"),
		},
		// invalid URL should error
		{
			query: searchParams{
				url: "htttp://\\example02",
			},
			want: &CoreAPI{},
			err:  errors.New("invalid url"),
		},
		// limit should be set
		{
			query: searchParams{
				url:   "http://www.nb.no",
				limit: "100",
			},
			want: &CoreAPI{
				Limit: 100,
			},
		},
		// parse does not set default limit
		{
			query: searchParams{
				url:   "http://example.com/?q=1",
				limit: "0",
			},
			want: &CoreAPI{
				Limit: 0,
			},
		},
		// to cannot be before from and should error
		{
			query: searchParams{
				url:  "http://example.com/",
				from: "20020101000000",
				to:   "2001",
			},
			want: &CoreAPI{},
			err:  errors.New("to should not be allowed before from"),
		},
		// test closest and sort closest
		{
			query: searchParams{
				url:     "http://example.com/",
				closest: "20020101000000",
				sort:    "closest",
			},
			want: &CoreAPI{
				Closest: "20020101000000",
				Sort:    SortClosest,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.query.url, func(t *testing.T) {
			reqUrl, _ := url.Parse("http://example.test/")
			query := reqUrl.Query()
			if test.query.url != "" {
				query.Set("url", test.query.url)
			}
			if test.query.matchType != "" {
				query.Set("matchType", test.query.matchType)
			}
			if test.query.limit != "" {
				query.Set("limit", test.query.limit)
			}
			if test.query.sort != "" {
				query.Set("sort", test.query.sort)
			}
			if test.query.from != "" {
				query.Set("from", test.query.from)
			}
			if test.query.to != "" {
				query.Set("to", test.query.to)
			}
			if test.query.closest != "" {
				query.Set("closest", test.query.closest)
			}
			reqUrl.RawQuery = query.Encode()

			testRequest := &http.Request{URL: reqUrl}

			got, err := Parse(testRequest)
			if err != nil {
				if test.err == nil {
					t.Errorf("unexpected error: %s", err)
				}
				return
			}
			if test.err != nil {
				t.Errorf("expected error: %v", test.err)
				return
			}

			// set want URL to got URL
			test.want.Url = got.Url

			if !reflect.DeepEqual(got, test.want) {
				t.Errorf("Got: '%+v', Want: '%+v'", got, test.want)
			}
		})
	}
}

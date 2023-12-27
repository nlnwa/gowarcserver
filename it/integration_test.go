package it

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/nlnwa/gowarcserver/index"
	"github.com/nlnwa/gowarcserver/schema"
	"github.com/nlnwa/gowarcserver/server/api"
	"github.com/nlnwa/gowarcserver/surt"
	"github.com/nlnwa/whatwg-url/url"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type test struct {
	req  index.Request
	want []string
}

var records []index.Record

var tests []test

var samples = []struct {
	id     string
	uri    string
	record index.Record
	time   time.Time
}{
	{
		id:   "a",
		uri:  "http://www.example.com",
		time: time.Date(2020, time.April, 1, 22, 22, 0, 0, time.UTC),
	},
	{
		id:   "b",
		uri:  "https://www.example.com/",
		time: time.Date(2020, time.April, 1, 22, 22, 0, 0, time.UTC),
	},
	{
		id:   "c",
		uri:  "http://user:password@www.example.com:8080/",
		time: time.Date(2020, time.April, 1, 22, 22, 0, 0, time.UTC),
	},
	{
		id:   "d",
		uri:  "http://www.example.com/",
		time: time.Date(2020, time.April, 1, 22, 21, 0, 0, time.UTC),
	},
	{
		id:   "e",
		uri:  "http://www.example.com/",
		time: time.Date(2020, time.April, 1, 22, 20, 0, 0, time.UTC),
	},
	{
		id:   "f",
		uri:  "http://www.example.com/path",
		time: time.Date(2020, time.April, 1, 22, 20, 0, 0, time.UTC),
	},
	{
		id:   "g",
		uri:  "http://example.com/",
		time: time.Date(2020, time.April, 1, 22, 22, 0, 0, time.UTC),
	},
	{
		id:   "h",
		uri:  "http://example.com/",
		time: time.Date(2020, time.April, 1, 22, 23, 0, 0, time.UTC),
	},
	{
		id:   "i",
		uri:  "http://example.com/path?query=2#fragment",
		time: time.Date(2020, time.April, 1, 22, 23, 0, 0, time.UTC),
	},
	{
		id:   "j",
		uri:  "http://example.com/path",
		time: time.Date(2020, time.April, 1, 22, 23, 0, 0, time.UTC),
	},
}

func init() {
	wwwExampleCom, err := url.Parse("http://www.example.com/")
	if err != nil {
		panic(err)
	}
	wwwExampleComPath, err := url.Parse("http://www.example.com/path")
	if err != nil {
		panic(err)
	}
	exampleCom, err := url.Parse("http://example.com/")
	if err != nil {
		panic(err)
	}
	exampleComPath, err := url.Parse("http://example.com/path")
	if err != nil {
		panic(err)
	}
	exampleComPathQuery, err := url.Parse("http://example.com/path?query=2#fragment")
	if err != nil {
		panic(err)
	}
	tests = []test{
		{
			req: api.SearchRequest{
				CoreAPI: &api.CoreAPI{
					Url:       wwwExampleCom,
					MatchType: api.MatchTypeExact,
				},
			},
			want: []string{"e", "d", "c", "a", "b"},
		},
		{
			req: api.SearchRequest{
				CoreAPI: &api.CoreAPI{
					Url:       wwwExampleCom,
					MatchType: api.MatchTypeExact,
					Sort:      api.SortReverse,
				},
			},
			want: []string{"b", "a", "c", "d", "e"},
		},
		{
			req: api.SearchRequest{
				CoreAPI: &api.CoreAPI{
					Url:       wwwExampleCom,
					MatchType: api.MatchTypePrefix,
				},
			},
			want: []string{"e", "d", "c", "a", "b", "f"},
		},
		{
			req: api.SearchRequest{
				CoreAPI: &api.CoreAPI{
					Url:       wwwExampleCom,
					Closest:   "202004012221",
					Sort:      api.SortClosest,
					MatchType: api.MatchTypeExact,
				},
			},
			want: []string{"d", "e", "c", "a", "b"},
		},
		{
			req: api.SearchRequest{
				CoreAPI: &api.CoreAPI{
					Url:       wwwExampleCom,
					Closest:   "202004012221",
					Sort:      api.SortClosest,
					MatchType: api.MatchTypeVerbatim,
				},
			},
			want: []string{"d", "e", "a"},
		},
		{
			req: api.SearchRequest{
				CoreAPI: &api.CoreAPI{
					Url:    wwwExampleCom,
					Filter: []string{"!hsc:200"},
				},
			},
			want: nil,
		},
		{
			req: api.SearchRequest{
				CoreAPI: &api.CoreAPI{
					Url: exampleCom,
				},
			},
			want: []string{"g", "h"},
		},
		{
			req: api.SearchRequest{
				CoreAPI: &api.CoreAPI{
					Url:       exampleCom,
					MatchType: api.MatchTypeDomain,
				},
			},
			want: []string{"g", "h", "j", "i", "e", "d", "c", "a", "b", "f"},
		},
		{
			req: api.SearchRequest{
				CoreAPI: &api.CoreAPI{
					Url: wwwExampleComPath,
				},
			},
			want: []string{"f"},
		},
		{
			req: api.SearchRequest{
				CoreAPI: &api.CoreAPI{
					Url:       exampleComPath,
					MatchType: api.MatchTypePrefix,
				},
			},
			want: []string{"j", "i"},
		},
		{
			req: api.SearchRequest{
				CoreAPI: &api.CoreAPI{
					Url: exampleComPathQuery,
				},
			},
			want: []string{"i"},
		},
	}

	for _, sample := range samples {
		uri, err := url.Parse(sample.uri)
		if err != nil {
			panic(err)
		}
		ssu := surt.UrlToSsurt(uri)
		record := index.Record{
			Cdx: &schema.Cdx{
				Rid: sample.id,
				Ssu: ssu,
				Sts: timestamppb.New(sample.time),
				Hsc: 200,
			},
		}
		records = append(records, record)
	}

}

func writeRecords(recordWriter index.RecordWriter) error {
	for _, record := range records {
		err := recordWriter.Write(record)
		if err != nil {
			return err
		}
	}
	return nil
}

func runIntegrationTest(t *testing.T, cdxAPI index.CdxAPI) {
	for i, test := range tests {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			responses := make(chan index.CdxResponse)
			err := cdxAPI.Search(context.Background(), test.req, responses)
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}

			j := 0
			for r := range responses {
				if r == (index.CdxResponse{}) {
					continue
				}
				t.Logf("[%d.%d] %+v", i, j, r)

				if j >= len(test.want) {
					t.Errorf("[%d]: got more results than we want (%d)", i, len(test.want))
					break
				}
				if r.Error != nil && r.Error == nil {
					t.Errorf("unexpected error: %v", r.Error)
					continue
				}
				if r.Cdx.GetRid() != test.want[j] {
					t.Errorf("[%d.%d]: got %s, want %s", i, j, r.Cdx.GetRid(), test.want[j])
				}
				j++ // count results
			}
			// check if number of results is equal to number of expected results
			if j != len(test.want) {
				t.Errorf("[%d] got %d results, want %d", i, j, len(test.want))
			}
		})

	}
}

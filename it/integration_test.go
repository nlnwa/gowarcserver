package it

import (
	"context"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/nlnwa/gowarcserver/index"
	"github.com/nlnwa/gowarcserver/schema"
	"github.com/nlnwa/gowarcserver/server/api"
	"github.com/nlnwa/gowarcserver/surt"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type test struct {
	req  url.Values
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
	wwwExampleCom := "http://www.example.com/"
	wwwExampleComPath := "http://www.example.com/path"
	exampleCom := "http://example.com/"
	exampleComPath := "http://example.com/path"
	exampleComPathQuery := "http://example.com/path?query=2#fragment"
	tests = []test{
		{
			req: url.Values{
				"matchType": {api.MatchTypeExact},
				"url":       {wwwExampleCom},
			},
			want: []string{"e", "d", "c", "a", "b"},
		},
		{
			req: url.Values{
				"matchType": {api.MatchTypeExact},
				"url":       {wwwExampleCom},
				"sort":      {api.SortReverse},
			},
			want: []string{"b", "a", "c", "d", "e"},
		},
		{
			req: url.Values{
				"matchType": {api.MatchTypePrefix},
				"url":       {wwwExampleCom},
			},
			want: []string{"e", "d", "c", "a", "b", "f"},
		},
		{
			req: url.Values{
				"matchType": {api.MatchTypeExact},
				"url":       {wwwExampleCom},
				"closest":   {"202004012221"},
				"sort":      {api.SortClosest},
			},
			want: []string{"d", "e", "c", "a", "b"},
		},
		{
			req: url.Values{
				"matchType": {api.MatchTypeVerbatim},
				"url":       {wwwExampleCom},
				"closest":   {"202004012221"},
				"sort":      {api.SortClosest},
			},
			want: []string{"d", "e", "a"},
		},
		{
			req: url.Values{
				"matchType": {api.MatchTypeExact},
				"url":       {wwwExampleCom},
				"filter":    {"!hsc:200"},
			},
			want: nil,
		},
		{
			req: url.Values{
				"url": {exampleCom},
			},
			want: []string{"g", "h"},
		},
		{
			req: url.Values{
				"matchType": {api.MatchTypeDomain},
				"url":       {exampleCom},
			},
			want: []string{"g", "h", "j", "i", "e", "d", "c", "a", "b", "f"},
		},
		{
			req: url.Values{
				"url": {wwwExampleComPath},
			},
			want: []string{"f"},
		},
		{
			req: url.Values{
				"matchType": {api.MatchTypePrefix},
				"url":       {exampleComPath},
			},
			want: []string{"j", "i"},
		},
		{
			req: url.Values{
				"url": {exampleComPathQuery},
			},
			want: []string{"i"},
		},
	}

	for _, sample := range samples {
		ssu, err := surt.StringToSsurt(sample.uri)
		if err != nil {
			panic(err)
		}
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
			req, err := api.Parse(test.req)
			if err != nil {
				t.Fatal(err)
			}
			responses := make(chan index.CdxResponse)
			err = cdxAPI.Search(context.Background(), req, responses)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			j := 0
			for r := range responses {
				if r == nil {
					continue
				}
				t.Logf("[%d.%d] %+v", i, j, r)

				if j >= len(test.want) {
					t.Errorf("[%d]: got more results than we want (%d)", i, len(test.want))
					break
				}
				if r.GetError() != nil && r.GetError() == nil {
					t.Errorf("unexpected error: %v", r.GetError())
					continue
				}
				cdx := r.GetCdx()
				if cdx.GetRid() != test.want[j] {
					t.Errorf("[%d.%d]: got %s, want %s", i, j, cdx.GetRid(), test.want[j])
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

package handlers_test

import (
	"log"
	"net/url"
	"testing"

	"github.com/nlnwa/gowarcserver/pkg/server/handlers"
)

func TestConfigToDBMask(t *testing.T) {
	tests := []struct {
		name        string
		nodeUrlStr  string
		childUrlStr string
		expectedUrl string
	}{
		{
			"simple nop",
			"http://190.165.33.152:8080",
			"http://190.165.33.152:9999",
			"http://190.165.33.152:9999",
		},
		{
			"dont include child non-host",
			"http://190.165.33.152:8080",
			"http://190.165.33.152:9999/yes?foo=bar#ez",
			"http://190.165.33.152:9999",
		},
		{
			"path",
			"http://190.165.33.152:8080/foo",
			"http://190.165.33.152:9999",
			"http://190.165.33.152:9999/foo",
		},
		{
			"query",
			"http://190.165.33.152:8080?foo=bar",
			"http://190.165.33.152:9999",
			"http://190.165.33.152:9999?foo=bar",
		},
		{
			"fragment",
			"http://190.165.33.152:8080#foo",
			"http://190.165.33.152:9999",
			"http://190.165.33.152:9999#foo",
		},
		{
			"path&query",
			"http://190.165.33.152:8080/path?foo=bar",
			"http://190.165.33.152:9999",
			"http://190.165.33.152:9999/path?foo=bar",
		},
		{
			"path&query&fragment",
			"http://190.165.33.152:8080/path?foo=bar#f",
			"http://190.165.33.152:9999",
			"http://190.165.33.152:9999/path?foo=bar#f",
		},
	}

	for _, tt := range tests {
		var err error
		childUrl, err := url.Parse(tt.childUrlStr)
		if err != nil {
			log.Fatal("illegal child url test string, msg: ", err)
		}
		nodeUrl, err := url.Parse(tt.nodeUrlStr)
		if err != nil {
			log.Fatal("illegal node url test string, msg: ", err)
		}
		t.Run(tt.name, func(t *testing.T) {
			result := handlers.BuildChildURLString(childUrl, nodeUrl).String()
			if result != tt.expectedUrl {
				t.Errorf("Expected %s got %s", tt.expectedUrl, result)
			}
		})
	}
}

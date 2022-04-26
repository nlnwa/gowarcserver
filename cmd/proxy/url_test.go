/*
 * Copyright 2021 National Library of Norway.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package proxy

import (
	"net/url"
	"testing"
)

func TestParseUrls(t *testing.T) {
	tests := []struct {
		name           string
		urlStrs        []string
		expectedUrls   []*url.URL
		expectedErrors int
	}{
		{
			"parsing empty slice",
			[]string{},
			[]*url.URL{},
			0,
		},
		{
			"parsing multiple urls",
			[]string{
				"http://192.148.38.150:8888",
				"http://192.148.38.150:7777",
			},
			[]*url.URL{
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
			[]*url.URL{},
			1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if len(tt.urlStrs)-tt.expectedErrors != len(tt.expectedUrls) {
				t.Errorf("test url slice and string slice does not have the same length")
				return
			}

			urls := ParseUrls(tt.urlStrs)
			for i, url := range urls {
				expected := tt.expectedUrls[i]
				if *url != *expected {
					t.Errorf("Expected %v got %v", expected, url)
				}
			}
		})
	}
}

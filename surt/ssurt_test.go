/*
 * Copyright 2020 National Library of Norway.
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

package surt

import (
	"testing"
)

func TestSsurtS(t *testing.T) {
	tests := []struct {
		url     string
		want    string
		wantErr bool
	}{
		{"http://www.example.com", "com,example,www,//:http:/", false},
		{"http://www.example.com:80", "com,example,www,//:http:/", false},
		{"http://www.example.com/foo/bar", "com,example,www,//:http:/foo/bar", false},
		{"http://127.0.0.1/foo/bar", "127.0.0.1//:http:/foo/bar", false},
		{"http://[::1]/foo/bar", "[::1]//:http:/foo/bar", false},
		{"http://example.com/foo/bar?query#fragment", "com,example,//:http:/foo/bar?query#fragment", false},
		{"http://example.com:8080/foo/bar?query#fragment", "com,example,//8080:http:/foo/bar?query#fragment", false},
		{"http://user:pass@foo.example.org:81/path?query#frag", "org,example,foo,//81:http@user:pass:/path?query#frag", false},
		{"http://user@foo.example.org:81/path?query#frag", "org,example,foo,//81:http@user:/path?query#frag", false},
		{"http://foo.example.org:81/path?query#frag", "org,example,foo,//81:http:/path?query#frag", false},
		{"http://foo.example.org/path?query#frag", "org,example,foo,//:http:/path?query#frag", false},
		{"http://81.foo.example.org/path?query#frag", "org,example,foo,81,//:http:/path?query#frag", false},
		{"scheme://user:pass@foo.example.org:81/path?query#frag", "org,example,foo,//81:scheme@user:pass:/path?query#frag", false},
		{"scheme://user@foo.example.org:81/path?query#frag", "org,example,foo,//81:scheme@user:/path?query#frag", false},
		{"scheme://foo.example.org:81/path?query#frag", "org,example,foo,//81:scheme:/path?query#frag", false},
		{"scheme://foo.example.org/path?query#frag", "org,example,foo,//:scheme:/path?query#frag", false},
		{"scheme://81.foo.example.org/path?query#frag", "org,example,foo,81,//:scheme:/path?query#frag", false},
		// TODO the url parser does not by default provide access to part after scheme when there is no authority
		// {"screenshot:http://example.com/", "com,example,//screenshot:", false},
	}
	for _, tt := range tests {
		t.Run(tt.url, func(t *testing.T) {
			got, err := StringToSsurt(tt.url)
			if err != nil {
				if tt.wantErr {
					return
				}
				t.Errorf("SsurtS() error = %v, wantErr %v", err, tt.wantErr)
			}
			if got != tt.want {
				t.Errorf("SsurtS() got = %v, want %v", got, tt.want)
			}
		})
	}
}

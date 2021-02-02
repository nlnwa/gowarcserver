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

func TestSurtS(t *testing.T) {
	tests := []struct {
		name          string
		u             string
		includeScheme bool
		want          string
		wantErr       bool
	}{
		{"1", "http://www.example.com", false, "(com,example,www,)/", false},
		{"2", "http://www.example.com:80", false, "(com,example,www,)/", false},
		{"3", "http://www.example.com/foo/bar", false, "(com,example,www,)/foo/bar", false},
		{"4", "http://127.0.0.1/foo/bar", false, "(1,0,0,127,)/foo/bar", false},
		{"5", "http://[::1]/foo/bar", false, "([::1],)/foo/bar", false},
		{"6", "http://example.com/foo/bar?query#fragment", false, "(com,example,)/foo/bar?query", false},
		{"7", "http://www.example.com", true, "http://(com,example,www,)/", false},
		{"8", "http://www.example.com:80", true, "http://(com,example,www,)/", false},
		{"9", "http://www.example.com/foo/bar", true, "http://(com,example,www,)/foo/bar", false},
		{"10", "http://127.0.0.1/foo/bar", true, "http://(1,0,0,127,)/foo/bar", false},
		{"11", "http://[::1]/foo/bar", true, "http://([::1],)/foo/bar", false},
		{"12", "http://example.com/foo/bar?query=foo#fragment", true, "http://(com,example,)/foo/bar?query=foo", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := SurtS(tt.u, tt.includeScheme)
			if (err != nil) != tt.wantErr {
				t.Errorf("SurtS() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("SurtS() got = %v, want %v", got, tt.want)
			}
		})
	}
}

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

package warcserver

import (
	"strings"

	"github.com/nlnwa/gowarcserver/pkg/surt"
)

func parseKey(uri string, matchType string) (string, error) {
	key, err := surt.SsurtString(uri, true)
	if err != nil {
		return "", err
	}

	switch matchType {
	case MatchTypeExact:
		key += " "
	case MatchTypePrefix:
		i := strings.IndexAny(key, "?#")
		if i > 0 {
			key = key[:i]
		}
	case MatchTypeHost:
		i := strings.Index(key, "//")
		if i > 0 {
			key = key[:i+2]
		}
	case MatchTypeDomain:
		i := strings.Index(key, "//")
		if i > 0 {
			key = key[:i]
		}
	}

	return key, nil
}

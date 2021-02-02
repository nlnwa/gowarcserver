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
	"github.com/nlnwa/whatwg-url/url"
	"strings"
)

func SurtU(u *url.Url, includeScheme bool) (string, error) {
	u.SearchParams().Sort()

	var result strings.Builder
	if includeScheme {
		result.WriteString(u.Protocol() + "//")
	}

	result.WriteByte('(')
	t := strings.Split(u.Hostname(), ".")
	for i := len(t) - 1; i >= 0; i-- {
		result.WriteString(t[i])
		result.WriteByte(',')
	}
	result.WriteByte(')')
	result.WriteString(u.Pathname())
	result.WriteString(u.Search())
	return result.String(), nil
}

func SurtS(u string, includeScheme bool) (string, error) {
	u2, err := url.Parse(u)
	if err != nil {
		return "", err
	}
	return SurtU(u2, includeScheme)
}

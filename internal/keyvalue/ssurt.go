/*
 * Copyright 2023 National Library of Norway.
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

package keyvalue

import (
	"strings"
)

func SplitSSURT(ssurt string) (surtHost string, schemeAndUserinfo string, path string) {
	i := strings.Index(ssurt, "//")
	if i == -1 {
		return ssurt, "", ""
	}
	surtHost = ssurt[:i]

	if len(ssurt) < i+2 {
		return surtHost, "", ""
	}
	if len(ssurt) == i+2 {
		return surtHost, "", "/"
	}

	j := strings.Index(ssurt[i+2:], "/")
	if j == -1 {
		return surtHost, ssurt[i+2:], ""
	}
	schemeAndUserinfo = ssurt[i+2 : i+2+j]

	path = ssurt[i+2+j:]

	return
}

func deSurtDomain(domain string) string {
	var sb strings.Builder
	sb.Grow(len(domain) - 1)
	t := strings.Split(domain, ",")
	for i := len(t) - 1; i >= 0; i-- {
		if t[i] == "" {
			continue
		}
		sb.WriteString(t[i])
		if i > 0 {
			sb.WriteByte('.')
		}
	}
	return sb.String()
}

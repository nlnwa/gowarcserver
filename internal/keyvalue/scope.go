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

package keyvalue

import (
	"strings"

	"github.com/nlnwa/gowarcserver/index"
)

func scope(ssurt string, matchType index.MatchType) string {
	if ssurt == "" {
		return ""
	}
	switch matchType {
	case index.MatchTypeExact, index.MatchTypeVerbatim:
		return ssurt + " "
	case index.MatchTypePrefix:
		i := strings.IndexAny(ssurt, "?#")
		if i > 0 {
			return ssurt[:i]
		}
	case index.MatchTypeHost:
		i := strings.Index(ssurt, "//")
		if i > 0 {
			return ssurt[:i+2]
		}
	case index.MatchTypeDomain:
		i := strings.Index(ssurt, "//")
		if i > 0 {
			return ssurt[:i]
		}
	}
	return ssurt
}

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
	"fmt"
	"strings"
)

type dateRange struct {
	from string
	to   string
}

func parseDateRange(from, to string) *dateRange {
	from = fmt.Sprintf("%s%0*d", from, 14-len(from), 0)
	to = fmt.Sprintf("%s%.*s", to, 14-len(to), "99999999999999")
	d := &dateRange{from: from, to: to}
	return d
}

func (d *dateRange) eval(key []byte) bool {
	ts := strings.Split(string(key), " ")[1]
	if ts < d.from || ts > d.to {
		return false
	}
	return true
}

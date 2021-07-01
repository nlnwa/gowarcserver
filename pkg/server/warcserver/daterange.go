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
)

type DateRange struct {
	from string
	to   string
}

// contains returns true if the timestamp ts contained by the bounds defined by the DateRange d.
func (d DateRange) contains(ts string) bool {
	return ts >= d.from && ts <= d.to
}

// From pads the timestamp f with 0's on the right until the string is 14 characters in length.
func From(f string) string {
	return fmt.Sprintf("%s%0*d", f, 14-len(f), 0)
}

// To pads the timestamp t with 9's on the right until the string is 14 characters in length.
func To(t string) string {
	return fmt.Sprintf("%s%.*s", t, 14-len(t), "99999999999999")
}

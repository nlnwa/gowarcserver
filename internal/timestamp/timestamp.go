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

package timestamp

import (
	"time"
)

const CDX = "20060102150405"

func TimeTo14(t time.Time) string {
	return t.Format(CDX)
}

func To14(s string) (string, error) {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return "", err
	}

	return t.Format(CDX), nil
}

func Parse(s string) (time.Time, error) {
	return time.Parse(CDX, s)
}

// AbsInt64 returns the absolute value of an int64
//
// Source: https://github.com/cavaliercoder/go-abs
func AbsInt64(n int64) int64 {
	y := n >> 63
	return (n ^ y) - y
}

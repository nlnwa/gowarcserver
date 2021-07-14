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
	"math"
	"strings"
	"time"
)

type DateRange struct {
	from int64
	to   int64
}

const timeLayout = "20060102150405"

func NewDateRange(fromstr string, tostr string) (DateRange, error) {
	from, err := From(fromstr)
	if err != nil {
		return DateRange{0, 0}, err
	}
	to, err := To(tostr)
	if err != nil {
		return DateRange{0, 0}, err
	}

	return DateRange{
		from,
		to,
	}, nil
}

// contains returns true if the timestamp ts contained by the bounds defined by the DateRange d.
// input 'ts' is 'trusted' and does not have the same parsing complexity as a From or To string
func (d DateRange) contains(ts string) (bool, error) {
	timestamp, err := time.Parse(timeLayout, ts)
	if err != nil {
		return false, fmt.Errorf("failed to parse ts: %w", err)
	}
	unixTs := timestamp.Unix()

	return unixTs >= d.from && unixTs <= d.to, nil
}

// Implemented according to https://pywb.readthedocs.io/en/latest/manual/cdxserver_api.html#from-to
func From(f string) (int64, error) {
	fLen := len(f)
	if fLen%2 != 0 {
		return 0, fmt.Errorf("'from' string was not an odd number, len: %d", fLen)
	}
	if fLen > 14 {
		return 0, fmt.Errorf("expected from string len less than 14, len: %d", fLen)
	}

	// No specified from date
	if fLen < 4 {
		return math.MinInt64, nil
	}

	builder := strings.Builder{}
	builder.Grow(14)
	builder.WriteString(f)
	if builder.Len() <= 4 {
		builder.WriteString("01")
	}
	if builder.Len() <= 6 {
		builder.WriteString("01")
	}
	if builder.Len() <= 8 {
		builder.WriteString("00")
	}
	if builder.Len() <= 10 {
		builder.WriteString("00")
	}
	if builder.Len() <= 12 {
		builder.WriteString("00")
	}

	date := builder.String()
	from, err := time.Parse(timeLayout, date)
	if err != nil {
		return 0, fmt.Errorf("failed to parse 'from' date %s, %w", date, err)
	}

	return from.Unix(), nil
}

// Implemented according to https://pywb.readthedocs.io/en/latest/manual/cdxserver_api.html#from-to:
func To(t string) (int64, error) {
	fLen := len(t)
	if fLen%2 != 0 {
		return 0, fmt.Errorf("'to' string was not an odd number, len: %d", fLen)
	}
	if fLen > 14 {
		return 0, fmt.Errorf("expected from string len less than 14, len: %d", fLen)
	}

	// No specified from date
	if fLen < 4 {
		return math.MaxInt64, nil
	}

	builder := strings.Builder{}
	builder.Grow(14)
	builder.WriteString(t)
	if builder.Len() <= 4 {
		builder.WriteString("12")
	}
	// Assumption: there is no harm in having a timestamp with a month with less than 31 days
	// 			   be assigned 31 days
	if builder.Len() <= 6 {
		builder.WriteString("31")
	}
	if builder.Len() <= 8 {
		builder.WriteString("23")
	}
	if builder.Len() <= 10 {
		builder.WriteString("59")
	}
	if builder.Len() <= 12 {
		builder.WriteString("59")
	}

	date := builder.String()
	to, err := time.Parse(timeLayout, date)
	if err != nil {
		return 0, fmt.Errorf("failed to parse 'to' date %s, %w", date, err)
	}

	return to.Unix(), nil
}

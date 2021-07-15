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
		return DateRange{}, err
	}
	to, err := To(tostr)
	if err != nil {
		return DateRange{}, err
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

// Implemented according to https://pywb.readthedocs.io/en/latest/manual/cdxserver_api.html#from-to:
func From(f string) (int64, error) {
	fLen := len(f)
	if fLen%2 != 0 {
		return 0, fmt.Errorf("'from' string was an odd number, len: %d", fLen)
	}
	if fLen > 14 {
		return 0, fmt.Errorf("expected 'from' string len less than 14, len: %d", fLen)
	}

	// No specified from date
	if fLen < 4 {
		return time.Time{}.Unix(), nil
	}

	from, err := time.Parse(timeLayout[:fLen], f)
	if err != nil {
		return 0, fmt.Errorf("failed to parse 'from' date %s, %w", f, err)
	}

	return from.Unix(), nil
}

// Implemented according to https://pywb.readthedocs.io/en/latest/manual/cdxserver_api.html#from-to:
func To(t string) (int64, error) {
	tLen := len(t)
	if tLen%2 != 0 {
		return 0, fmt.Errorf("'to' string was an odd number, len: %d", tLen)
	}
	if tLen > 14 {
		return 0, fmt.Errorf("expected 'to' string len less than 14, len: %d", tLen)
	}

	// No specified from date
	if tLen < 4 {
		return math.MaxInt64, nil
	}

	to, err := time.Parse(timeLayout[:tLen], t)
	if err != nil {
		return 0, fmt.Errorf("failed to parse 'to' date %s, %w", t, err)
	}

	switch tLen {
	case 4:
		to = to.AddDate(0, 12, -1) // 31 days in decemeber
		to = to.Add(time.Hour*23 + time.Minute*59 + time.Second*59)
	case 6:
		// add one month - one day, i.e: user supplies january, we add 29 - 1
		to = to.AddDate(0, 1, -1)
		to = to.Add(time.Hour*23 + time.Minute*59 + time.Second*59)
	case 8:
		to = to.Add(time.Hour*23 + time.Minute*59 + time.Second*59)
	case 10:
		to = to.Add(time.Minute*59 + time.Second*59)
	case 12:
		to = to.Add(time.Second * 59)
	}

	return to.Unix(), nil
}

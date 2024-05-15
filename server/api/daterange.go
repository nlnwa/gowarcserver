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

package api

import (
	"fmt"
	"math"
	"time"

	"github.com/nlnwa/gowarcserver/timestamp"
)

type DateRange struct {
	from int64 // unix time
	to   int64 // unix time
}

func NewDateRange(fromstr string, tostr string) (*DateRange, error) {
	if fromstr == "" && tostr == "" {
		return nil, nil
	}

	from, err := from(fromstr)
	if err != nil {
		return nil, err
	}

	to, err := to(tostr)
	if err != nil {
		return nil, err
	}

	if from > to {
		return nil, fmt.Errorf("from date %s is after to date %s", fromstr, tostr)
	}
	return &DateRange{from, to}, nil
}

func (d *DateRange) Contains(ts int64) bool {
	if d == nil {
		return true
	}
	return ts >= d.from && ts <= d.to
}

// from parses string f to unix time according to https://pywb.readthedocs.io/en/latest/manual/cdxserver_api.html#from-to:
func from(f string) (int64, error) {
	l := len(f)
	if l == 0 {
		return time.Time{}.Unix(), nil
	}
	t, err := timestamp.Parse(f)
	if err != nil {
		return 0, fmt.Errorf("failed to parse 'from' date %s, %w", f, err)
	}
	return t.Unix(), nil
}

// to parses string t to unix time according to https://pywb.readthedocs.io/en/latest/manual/cdxserver_api.html#from-to:
func to(t string) (int64, error) {
	l := len(t)
	if l == 0 {
		return math.MaxInt64, nil
	}

	to, err := timestamp.Parse(t)
	if err != nil {
		return 0, fmt.Errorf("failed to parse 'to' date %s, %w", t, err)
	}

	switch l {
	case 4:
		// adjust to last second of year
		to = to.AddDate(0, 12, -1).Add(time.Hour*23 + time.Minute*59 + time.Second*59)
	case 6:
		// adjust to last second of month
		// add one month - one day, i.e: user supplies january, we add 29 - 1
		to = to.AddDate(0, 1, -1).Add(time.Hour*23 + time.Minute*59 + time.Second*59)
	case 8:
		// adjust to last second of day
		to = to.Add(time.Hour*23 + time.Minute*59 + time.Second*59)
	case 10:
		// adjust to last second of hour
		to = to.Add(time.Minute*59 + time.Second*59)
	case 12:
		// adjust to last second of minute
		to = to.Add(time.Second * 59)
	}

	return to.Unix(), nil
}

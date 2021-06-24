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
	"regexp"
	"strings"

	cdx "github.com/nlnwa/gowarcserver/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

type Op int

const (
	OpContains Op = iota
	OpExact
	OpRegexp
)

type Filter []filter

func (f Filter) eval(c *cdx.Cdx) bool {
	for _, ff := range f {
		if !ff.eval(c) {
			return false
		}
	}
	return true
}

func parseFilter(filterStrings []string) Filter {
	var filters Filter

	for _, f := range filterStrings {
		not := false
		if f[0] == '!' {
			f = f[1:]
			not = true
		}
		var op Op
		switch f[0] {
		case '=':
			f = f[1:]
			op = OpExact
		case '~':
			f = f[1:]
			op = OpRegexp
		default:
			op = OpContains
		}

		t := strings.SplitN(f, ":", 2)
		filter := filter{
			field:       t[0],
			filterValue: t[1],
			invert:      not,
		}

		switch op {
		case OpContains:
			filter.matcher = func(filterValue, fieldValue string) bool {
				return strings.Contains(fieldValue, filterValue)
			}
		case OpExact:
			filter.matcher = func(filterValue, fieldValue string) bool {
				return fieldValue == filterValue
			}
		case OpRegexp:
			filter.matcher = func(filterValue, fieldValue string) bool {
				return regexp.MustCompile(filterValue).MatchString(fieldValue)
			}
		}
		filters = append(filters, filter)
	}

	return filters
}

type filter struct {
	field       string
	filterValue string
	invert      bool
	matcher     func(filterValue, fieldValue string) bool
}

func (f filter) eval(c *cdx.Cdx) bool {
	result := false
	if fieldValue, found := f.findFieldValue(c); found {
		result = f.matcher(f.filterValue, fieldValue)
	}
	if f.invert {
		return !result
	} else {
		return result
	}
}

func (f filter) findFieldValue(c *cdx.Cdx) (fieldValue string, found bool) {
	c.ProtoReflect().Range(func(descriptor protoreflect.FieldDescriptor, value protoreflect.Value) bool {
		if string(descriptor.Name()) == f.field {
			found = true
			fieldValue = value.String()
			return false
		}
		return true
	})
	return
}

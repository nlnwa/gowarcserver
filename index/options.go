/*
 * Copyright 2021 National Library of Norway.
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

package index

import (
	"regexp"

	"github.com/nlnwa/gowarc"
)

type Options struct {
	Includes         []*regexp.Regexp
	Excludes         []*regexp.Regexp
	warcRecordOption []gowarc.WarcRecordOption
}

type Option func(*Options)

func (o *Options) filter(name string) bool {
	return o.isIncluded(name) && !o.isExcluded(name)
}

func (o *Options) isExcluded(name string) bool {
	for _, re := range o.Excludes {
		if re.MatchString(name) {
			return true
		}
	}
	return false
}

func (o *Options) isIncluded(name string) bool {
	if len(o.Includes) == 0 {
		return true
	}
	for _, re := range o.Includes {
		if re.MatchString(name) {
			return true
		}
	}
	return false
}

func WithIncludes(res ...*regexp.Regexp) Option {
	return func(opts *Options) {
		opts.Includes = res
	}
}

func WithExcludes(res ...*regexp.Regexp) Option {
	return func(opts *Options) {
		opts.Excludes = res
	}
}

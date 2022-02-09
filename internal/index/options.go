package index

import (
	"regexp"
)

type Options struct {
	Watch    bool
	Includes []*regexp.Regexp
	Excludes []*regexp.Regexp
	MaxDepth int
}

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

func defaultOptions() *Options {
	return &Options{
		Watch:    false,
		MaxDepth: 4,
	}
}

type Option func(*Options)

func WithMaxDepth(depth int) Option {
	return func(opts *Options) {
		opts.MaxDepth = depth
	}
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

func WithWatch(watch bool) Option {
	return func(opts *Options) {
		opts.Watch = watch
	}
}

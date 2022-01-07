package index

import "strings"

type Options struct {
	Watch    bool
	Suffixes []string
	MaxDepth int
}

func (o *Options) isWhitelisted(name string) bool {
	if len(o.Suffixes) == 0 {
		return true
	}
	for _, suffix := range o.Suffixes {
		if strings.HasSuffix(name, suffix) {
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

func WithSuffixes(suffixes ...string) Option {
	return func(opts *Options) {
		opts.Suffixes = suffixes
	}
}

func WithWatch(watch bool) Option {
	return func(opts *Options) {
		opts.Watch = watch
	}
}

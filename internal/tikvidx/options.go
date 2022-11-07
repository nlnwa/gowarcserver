package tikvidx

import "time"

func defaultOptions() *Options {
	return &Options{
		BatchMaxSize:    255,
		BatchMaxWait:    5 * time.Second,
		BatchMaxRetries: 3,
	}
}

type Options struct {
	BatchMaxSize    int
	BatchMaxWait    time.Duration
	BatchMaxRetries int
	ReadOnly        bool
	PdAddr          []string
	Database        string
}

type Option func(opts *Options)

func WithPDAddress(pdAddr []string) Option {
	return func(opts *Options) {
		opts.PdAddr = pdAddr
	}
}

func WithReadOnly(readOnly bool) Option {
	return func(opts *Options) {
		opts.ReadOnly = readOnly
	}
}

func WithBatchMaxSize(size int) Option {
	return func(opts *Options) {
		opts.BatchMaxSize = size
	}
}

func WithBatchMaxWait(t time.Duration) Option {
	return func(opts *Options) {
		opts.BatchMaxWait = t
	}
}

func WithDatabase(db string) Option {
	return func(opts *Options) {
		opts.Database = db
	}
}

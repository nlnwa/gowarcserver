package database

import (
	"time"

	"github.com/dgraph-io/badger/v3/options"
)

func defaultOptions() *dbOptions {
	return &dbOptions{
		Compression:  options.Snappy,
		BatchMaxSize: 10000,
		BatchMaxWait: 5 * time.Second,
		GcInterval:   15 * time.Second,
		Path:         ".",
	}
}

type dbOptions struct {
	Compression  options.CompressionType
	BatchMaxSize int
	BatchMaxWait time.Duration
	GcInterval   time.Duration
	Path         string
}

type DbOption func(opts *dbOptions)

func WithCompression(c options.CompressionType) DbOption {
	return func(opts *dbOptions) {
		opts.Compression = c
	}
}

func WithDir(d string) DbOption {
	return func(opts *dbOptions) {
		opts.Path = d
	}
}

func WithBatchMaxSize(size int) DbOption {
	return func(opts *dbOptions) {
		opts.BatchMaxSize = size
	}
}

func WithBatchMaxWait(t time.Duration) DbOption {
	return func(opts *dbOptions) {
		opts.BatchMaxWait = t
	}
}

func WithGcInterval(t time.Duration) DbOption {
	return func(opts *dbOptions) {
		opts.GcInterval = t
	}
}

package config

import (
	"fmt"
	"github.com/mitchellh/mapstructure"
	"reflect"
	"strings"

	"github.com/dgraph-io/badger/v3/options"
)

type Compression string

const (
	SnappyCompression = "snappy"
	NoCompression     = "none"
	ZstdCompression   = "zstd"
)

func parseCompression(value string) (options.CompressionType, error) {
	switch strings.ToLower(value) {
	case NoCompression:
		return options.None, nil
	case SnappyCompression:
		return options.Snappy, nil
	case ZstdCompression:
		return options.ZSTD, nil
	default:
		return options.None, fmt.Errorf("supported compression types is: 'snappy', 'zstd' or 'none', was: %s", strings.ToLower(value))
	}
}

// CompressionDecodeHookFunc helps decode a string to a badger compression type using viper.
//
// Example:
// var c options.CompressionType
// _ = viper.UnmarshalKey("compression", &c, viper.DecodeHook(CompressionDecodeHookFunc())
func CompressionDecodeHookFunc() mapstructure.DecodeHookFuncType {
	return func(
		f reflect.Type,
		t reflect.Type,
		data interface{},
	) (interface{}, error) {
		// Check that the data is string
		if f.Kind() != reflect.String {
			return data, nil
		}
		if s, ok := data.(string); ok {
			return parseCompression(s)
		} else {
			return parseCompression("")
		}
	}
}

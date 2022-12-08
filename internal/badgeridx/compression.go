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

package badgeridx

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/mitchellh/mapstructure"

	"github.com/dgraph-io/badger/v3/options"
)

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

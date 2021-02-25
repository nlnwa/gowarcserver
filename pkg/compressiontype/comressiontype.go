package compressiontype

import (
	"fmt"
	"strings"

	"github.com/dgraph-io/badger/v2/options"
)

type CompressionType options.CompressionType

// should be const, since it is internal to this file it should be ok as var...
var stringRepresentation = [...]string{"none", "snappy", "zstd"}

func (c CompressionType) String() (string, error) {
	if c != CompressionType(options.None) && c != CompressionType(options.Snappy) && c != CompressionType(options.ZSTD) {
		return "", fmt.Errorf("CompressionType can only have value %v, %v or %v", options.None, options.Snappy, options.ZSTD)
	}
	return stringRepresentation[c], nil
}

func FromString(value string) (CompressionType, error) {
	lowered := strings.ToLower(value)

	// for now we manually check for each type and return if we find it
	if lowered == "none" {
		return CompressionType(options.None), nil
	} else if lowered == "snappy" {
		return CompressionType(options.Snappy), nil
	} else if lowered == "zstd" {
		return CompressionType(options.ZSTD), nil
	}

	return CompressionType(0), fmt.Errorf("unexpected value '%v', expected any of listed: '%v'", lowered, strings.Join(stringRepresentation[:], ", "))
}

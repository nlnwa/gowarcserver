package compressiontype_test

import (
	"testing"

	"github.com/dgraph-io/badger/v3/options"
	ct "github.com/nlnwa/gowarcserver/pkg/compressiontype"
)

func TestCompressionType_String(t *testing.T) {
	tests := []struct {
		name       string
		cType      options.CompressionType
		expected   string
		errorState bool
	}{
		{
			"options.None to string none",
			options.None,
			"none",
			false,
		},
		{
			"options.Snappy to string snappy",
			options.Snappy,
			"snappy",
			false,
		},
		{
			"options.ZSTD to string zstd",
			options.ZSTD,
			"zstd",
			false,
		},
		{
			"Illegal to error",
			999,
			"",
			true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ct.CompressionType(tt.cType).String()
			if err != nil && !tt.errorState {
				t.Errorf("Unexpected failure: %v", err)
			} else if err == nil && tt.errorState {
				t.Errorf("Expected failure with %v, but got '%v'", tt.cType, got)
			}

			if tt.expected != got {
				t.Errorf("Exptected %v got %v", tt.expected, got)
			}
		})
	}
}

func TestCompressionType_FromString(t *testing.T) {
	tests := []struct {
		name        string
		stringValue string
		expected    ct.CompressionType
		errorState  bool
	}{
		{
			"string none to options.None",
			"none",
			ct.CompressionType(options.None),
			false,
		},
		{
			"string snappy to options.Snappy",
			"snappy",
			ct.CompressionType(options.Snappy),
			false,
		},
		{
			"string zstd to options.ZSTD",
			"zstd",
			ct.CompressionType(options.ZSTD),
			false,
		},
		{
			"string ZstD to options.ZSTD",
			"ZstD",
			ct.CompressionType(options.ZSTD),
			false,
		},
		{
			"string garbage to error",
			"garbage",
			0,
			true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ct.FromString(tt.stringValue)
			if err != nil && !tt.errorState {
				t.Errorf("Unexpected failure: %v", err)
			} else if err == nil && tt.errorState {
				t.Errorf("Expected failure with %v, but got %v", tt.stringValue, got)
			}

			if tt.expected != got {
				t.Errorf("Exptected %v got %v", tt.expected, got)
			}
		})
	}
}

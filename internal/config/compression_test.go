package config

import (
	"testing"

	"github.com/dgraph-io/badger/v3/options"
)

func TestParseCompression(t *testing.T) {
	tests := []struct {
		name      string
		t         options.CompressionType
		expectErr bool
	}{
		{
			"none",
			options.None,
			false,
		},
		{
			"snappy",
			options.Snappy,
			false,
		},
		{
			"zstd",
			options.ZSTD,
			false,
		},
		{
			"foobar",
			options.None,
			true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := parseCompression(tt.name)
			if err != nil && !tt.expectErr {
				t.Errorf("Unexpected error: %v", err)
			} else if err == nil && tt.expectErr {
				t.Errorf("Expected error: %s", tt.name)
			}
			if tt.t != got {
				t.Errorf("Exptected %d, got %v", tt.t, got)
			}
		})
	}
}

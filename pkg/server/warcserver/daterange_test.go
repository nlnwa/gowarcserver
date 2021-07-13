package warcserver

import (
	"testing"
)

type dateRangeTestData struct {
	name      string
	daterange DateRange
	timestamp string
	expect    bool
}

func TestInValidDateRangeContains(t *testing.T) {
	tests := []dateRangeTestData{
		{
			"invalid 'to' value fails",
			DateRange{
				from: "123",
				to:   "a",
			},
			"100",
			false,
		},
		{
			"invalid 'from' value fails",
			DateRange{
				from: "a",
				to:   "123",
			},
			"100",
			false,
		},
		{
			"invalid 'timestamp' value fails",
			DateRange{
				from: "100",
				to:   "123",
			},
			"a",
			false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			contains, err := tt.daterange.contains(tt.timestamp)
			if err == nil {
				t.Errorf("Expected error, got %t", contains)
			}
		})
	}
}

func TestValidDateRangeContains(t *testing.T) {
	tests := []dateRangeTestData{
		{
			"'timestamp' in range returns true",
			DateRange{
				from: "100",
				to:   "123",
			},
			"110",
			true,
		},
		{
			"'timestamp' same as 'to' returns true",
			DateRange{
				from: "100",
				to:   "123",
			},
			"100",
			true,
		},
		{
			"'timestamp' same as 'from' returns true",
			DateRange{
				from: "100",
				to:   "123",
			},
			"123",
			true,
		},
		{
			"'timestamp' below range returns false",
			DateRange{
				from: "100",
				to:   "123",
			},
			"99",
			false,
		},
		{
			"'timestamp' above range returns false",
			DateRange{
				from: "100",
				to:   "123",
			},
			"124",
			false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			contains, err := tt.daterange.contains(tt.timestamp)
			if err != nil {
				t.Errorf("Expected error, got %t", contains)
			}
			if contains != tt.expect {
				t.Errorf("Expected %t, got %t", tt.expect, contains)
			}
		})
	}
}

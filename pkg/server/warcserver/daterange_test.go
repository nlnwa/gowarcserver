package warcserver

import (
	"math"
	"testing"
)

type dateRangeTestData struct {
	name      string
	daterange DateRange
	timestamp string
	expect    bool
}

func TestValidDateRangeContains(t *testing.T) {
	tests := []dateRangeTestData{
		{
			"'timestamp' in range returns true",
			DateRange{
				from: 0,
				to:   60,
			},
			"19700101000010",
			true,
		},
		{
			"'timestamp' same as 'from' returns true",
			DateRange{
				from: 0,
				to:   60,
			},
			"19700101000000",
			true,
		},
		{
			"'timestamp' same as 'to' returns true",
			DateRange{
				from: 0,
				to:   60,
			},
			"19700101000100",
			true,
		},
		{
			"'timestamp' below range returns false",
			DateRange{
				from: 59,
				to:   60,
			},
			"19700101000000",
			false,
		},
		{
			"'timestamp' above range returns false",
			DateRange{
				from: 0,
				to:   1,
			},
			"19700101000200",
			false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			contains, err := tt.daterange.contains(tt.timestamp)
			if err != nil {
				t.Errorf("Unexpected error: %s", err)
			}
			if contains != tt.expect {
				t.Errorf("Expected %t, got %t", tt.expect, contains)
			}
		})
	}
}

func TestInvalidContainData(t *testing.T) {
	test := dateRangeTestData{
		"invalid 'to' value fails",
		DateRange{
			from: 0,
			to:   60,
		},
		"invalid",
		false,
	}

	t.Run(test.name, func(t *testing.T) {
		contains, err := test.daterange.contains(test.timestamp)
		if err == nil {
			t.Errorf("Expected error, got %v", err)
		}

		if contains == true {
			t.Error("Expected result to be false in event of error")
		}
	})
}

func TestInFromAndToParsing(t *testing.T) {
	tests := []struct {
		name         string
		fromAndTo    string
		expectedFrom int64
		expectedTo   int64
		expectError  bool
	}{
		{
			"valid full date string succeeds",
			"19700101000000",
			0,
			0,
			false,
		},
		{
			"valid partial date string succeeds",
			"197001010000",
			0,
			59,
			false,
		},
		{
			"empty date string succeeds",
			"",
			math.MinInt64,
			math.MaxInt64,
			false,
		},
		{
			"odd number date string fails",
			"19975",
			0,
			0,
			true,
		},
		{
			"more than 14 number date string fails",
			"200801122359590",
			0,
			0,
			true,
		},
		{
			"invalid char date string fails",
			"199A",
			0,
			0,
			true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			from, err := From(tt.fromAndTo)
			if err != nil && !tt.expectError {
				t.Errorf("Unexpected 'from' error: %s", err)
			}
			if from != tt.expectedFrom {
				t.Errorf("Expected %d, got %d", tt.expectedFrom, from)
			}
			to, err := To(tt.fromAndTo)
			if err != nil && !tt.expectError {
				t.Errorf("Unexpected 'to' error: %s", err)
			}
			if to != tt.expectedTo {
				t.Errorf("Expected %d, got %d", tt.expectedTo, to)
			}
		})
	}
}

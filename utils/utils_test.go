package utils

import "testing"

func TestRangeFromString(t *testing.T) {
	cases := map[string]struct {
		input  string
		output *Range
	}{
		"empty": {
			input:  "",
			output: nil,
		},
		"no dash": {
			input:  "test",
			output: nil,
		},
		"two dashes": {
			input:  "0-1-test",
			output: nil,
		},
		"begin is not int": {
			input:  "test-0",
			output: nil,
		},
		"end is not int": {
			input:  "0-test",
			output: nil,
		},
		"begin is greater than end": {
			input:  "1-0",
			output: nil,
		},

		"begin is equal to end": {
			input:  "0-0",
			output: &Range{Offset: 0, Limit: 1},
		},
		"begin is less than end": {
			input:  "0-1",
			output: &Range{Offset: 0, Limit: 2},
		},
	}

	for name, test := range cases {
		t.Run(name, func(t *testing.T) {
			got := RangeFromString(&test.input)
			failed := false

			if test.output != nil {
				if *got != *test.output {
					failed = true
				}
			} else {
				if got != test.output {
					failed = true
				}
			}

			if failed {
				t.Errorf("got %#v, want %#v", got, test.output)
			}
		})
	}
}

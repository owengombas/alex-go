package tests

import (
	"fmt"
	"testing"
)

func TestSequentialInserts1kto1m(t *testing.T) {
	for i := 1_000; i <= 10_000_000; i *= 10 {
		t.Run(fmt.Sprintf("SequentialInserts%d", i), func(t *testing.T) {
			keys := GenerateRandomKeys(i)
			SaveKeysToCSV(keys)
			alex, keys, err := SequentialInserts(keys)
			if err != nil {
				t.Error(err)
			}
			err = SequentialLookups(alex, keys)
			if err != nil {
				t.Error(err)
			}
		})
	}
}

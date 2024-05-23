package tests

import (
	"fmt"
	"testing"
)

func BenchmarkSequentialInserts1kTo1m(b *testing.B) {
	b.Run("SequentialInserts", func(b *testing.B) {
		keys := GenerateRandomKeys(b.N)
		b.ResetTimer()

		_, _, err := SequentialInserts(keys)
		if err != nil {
			b.Error(err)
		}
	})
}

func BenchmarkSequentialLookups(b *testing.B) {
	b.Run(fmt.Sprintf("SequentialLookups"), func(b *testing.B) {
		keys := GenerateRandomKeys(b.N)
		index, _, err := SequentialInserts(keys)
		if err != nil {
			b.Error(err)
		}

		b.ResetTimer()
		err = SequentialLookups(index, keys)
		if err != nil {
			b.Error(err)
		}
	})
}

package benchmarks

import (
	"alex_go/tests"
	"fmt"
	"testing"
)

func BenchmarkSequentialInserts1kTo1m(b *testing.B) {
	for i := 1_000; i <= 1_000_000; i *= 10 {
		keys := tests.GenerateRandomKeys(i)
		b.Run(fmt.Sprintf("SequentialInserts_%d", i), func(b *testing.B) {
			b.StartTimer()
			_, _, err := tests.SequentialInserts(keys)
			if err != nil {
				b.Error(err)
			}
			b.StopTimer()
		})
	}
}

func BenchmarkSequentialLookup1kTo1m(b *testing.B) {
	for i := 1_000; i <= 1_000_000; i *= 10 {
		b.Run(fmt.Sprintf("SequentialInserts_%d", i), func(b *testing.B) {
			keys := tests.GenerateRandomKeys(i)
			index, _, err := tests.SequentialInserts(keys)
			if err != nil {
				b.Error(err)
			}

			b.ResetTimer()
			err = tests.SequentialLookups(index, keys)
			if err != nil {
				b.Error(err)
			}
		})
	}
}

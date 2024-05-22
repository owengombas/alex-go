package tests

import (
	"testing"
)

func BenchmarkSequentialInserts1k(b *testing.B) {
	keys := GenerateRandomKeys(1_000)
	b.ResetTimer()
	_, _, err := SequentialInserts(keys)
	if err != nil {
		b.Error(err)
	}
}

func BenchmarkSequentialInserts10k(b *testing.B) {
	keys := GenerateRandomKeys(10_000)
	b.ResetTimer()
	_, _, err := SequentialInserts(keys)
	if err != nil {
		b.Error(err)
	}
}

func BenchmarkSequentialInserts100k(b *testing.B) {
	keys := GenerateRandomKeys(100_000)
	b.ResetTimer()
	_, _, err := SequentialInserts(keys)
	if err != nil {
		b.Error(err)
	}
}

func BenchmarkSequentialInserts1m(b *testing.B) {
	keys := GenerateRandomKeys(1_000_000)
	b.ResetTimer()
	_, _, err := SequentialInserts(keys)
	if err != nil {
		b.Error(err)
	}
}

func BenchmarkSequentialLookups1k(b *testing.B) {
	keys := GenerateRandomKeys(1_000)
	index, _, err := SequentialInserts(keys)
	if err != nil {
		b.Error(err)
	}

	b.ResetTimer()
	err = SequentialLookups(index, keys)
	if err != nil {
		b.Error(err)
	}

}

func BenchmarkSequentialLookups10k(b *testing.B) {
	keys := GenerateRandomKeys(10_000)
	index, _, err := SequentialInserts(keys)
	if err != nil {
		b.Error(err)
	}

	b.ResetTimer()
	err = SequentialLookups(index, keys)
	if err != nil {
		b.Error(err)
	}
}

func BenchmarkSequentialLookups100k(b *testing.B) {
	keys := GenerateRandomKeys(100_000)
	index, _, err := SequentialInserts(keys)
	if err != nil {
		b.Error(err)
	}

	b.ResetTimer()
	err = SequentialLookups(index, keys)
	if err != nil {
		b.Error(err)
	}
}

func BenchmarkSequentialLookups1m(b *testing.B) {
	keys := GenerateRandomKeys(1_000_000)
	index, _, err := SequentialInserts(keys)
	if err != nil {
		b.Error(err)
	}

	b.ResetTimer()
	err = SequentialLookups(index, keys)
	if err != nil {
		b.Error(err)
	}
}

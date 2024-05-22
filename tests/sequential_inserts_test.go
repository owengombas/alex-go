package tests

import (
	"testing"
)

func TestSequentialInserts1k(t *testing.T) {
	keys := GenerateRandomKeys(1_000)
	SaveKeysToCSV(keys)
	alex, keys, err := SequentialInserts(keys)
	if err != nil {
		t.Error(err)
	}
	err = SequentialLookups(alex, keys)
	if err != nil {
		t.Error(err)
	}
}

func TestSequentialInserts10k(t *testing.T) {
	keys := GenerateRandomKeys(10_000)
	SaveKeysToCSV(keys)
	alex, keys, err := SequentialInserts(keys)
	if err != nil {
		t.Error(err)
	}
	err = SequentialLookups(alex, keys)
	if err != nil {
		t.Error(err)
	}
}

func TestSequentialInserts100k(t *testing.T) {
	keys := GenerateRandomKeys(100_000)
	SaveKeysToCSV(keys)
	alex, keys, err := SequentialInserts(keys)
	if err != nil {
		t.Error(err)
	}
	err = SequentialLookups(alex, keys)
	if err != nil {
		t.Error(err)
	}
}

func TestSequentialInserts1m(t *testing.T) {
	keys := GenerateRandomKeys(1_000_000)
	SaveKeysToCSV(keys)
	alex, keys, err := SequentialInserts(keys)
	if err != nil {
		t.Error(err)
	}
	err = SequentialLookups(alex, keys)
	if err != nil {
		t.Error(err)
	}
}

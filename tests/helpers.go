package tests

import (
	"alex_go/index"
	"alex_go/shared"
	"bufio"
	"errors"
	"fmt"
	"math/rand"
	"os"
)

func RandomNotInSet(rng *rand.Rand, max int, set map[int]struct{}) int {
	for {
		value := rng.Intn(max)
		if _, ok := set[value]; !ok {
			return value
		}
	}
}

func ReadValuesFromFile(filePath string) ([]int, []int) {
	file, err := os.Open(filePath)
	if err != nil {
		panic(err)
	}

	keys := make([]int, 0)
	values := make([]int, 0)
	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		key, value := 0, 0
		fmt.Sscanf(scanner.Text(), "%d %d", &key, &value)
		keys = append(keys, key)
		values = append(values, value)
	}

	if err := scanner.Err(); err != nil {
		panic(err)
	}

	return keys, values
}

func GenerateRandomKeys(N int) []shared.KeyType {
	source := rand.NewSource(42)
	rng := rand.New(source)
	keys := make([]shared.KeyType, N)
	existingKeys := map[shared.KeyType]bool{}
	for i := 0; i < N; i++ {
		for {
			key := shared.KeyType(rng.Intn(N * 2))
			if _, ok := existingKeys[key]; !ok {
				keys[i] = key
				existingKeys[key] = true
				break
			}
		}
	}
	return keys
}

func SaveKeysToCSV(keys []shared.KeyType) error {
	file, err := os.Create(fmt.Sprintf("keys_%d.csv", len(keys)))
	if err != nil {
		return err
	}
	defer file.Close()

	writer := bufio.NewWriter(file)
	defer writer.Flush()

	for i := 0; i < len(keys); i++ {
		_, err := writer.WriteString(fmt.Sprintf("%d\n", keys[i]))
		if err != nil {
			return err
		}
	}

	return nil
}

func SequentialInserts(keys []shared.KeyType) (*index.Index, []shared.KeyType, error) {
	alex := index.NewIndex()

	for i := 0; i < len(keys); i++ {
		key := keys[i]
		err := alex.Insert(key, i)
		if err != nil {
			return alex, keys, err
		}
	}

	return alex, keys, nil
}

func SequentialLookups(alex *index.Index, keys []shared.KeyType) error {
	for i := 0; i < len(keys); i++ {
		payload, err := alex.Find(keys[i])
		if err != nil {
			return err
		} else {
			if i != *payload {
				return errors.New(fmt.Sprintf("retrieval error for key %d expected %d got %d", keys[i], i, *payload))
			}
		}
	}
	return nil
}

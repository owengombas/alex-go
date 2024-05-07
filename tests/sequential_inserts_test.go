package tests

import (
	"alex_go/index"
	"bufio"
	"fmt"
	"os"
	"testing"
)

func readValuesFromFile(filePath string) ([]int, []int) {
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

func TestSequentialInserts(t *testing.T) {
	alex := index.NewIndex()
	keys, values := readValuesFromFile("values.txt")

	for i := 0; i < len(keys); i++ {
		err := alex.Insert(keys[i], values[i])
		if err != nil {
			fmt.Println(err)
			t.Error(err)
		}
	}

	for i := 0; i < len(keys); i++ {
		payload, err := alex.Find(keys[i])
		if err != nil {
			fmt.Println(err)
			t.Error(err)
		} else {
			if values[i] != *payload {
				t.Errorf("Retrieval error for key %d expected %d go %d", keys[i], values[i], *payload)
			}
		}
	}
}

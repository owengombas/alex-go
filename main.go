package main

import (
	"alex_go/index"
	"bufio"
	"fmt"
	"math/rand"
	"os"
)

func randomNotInSet(rng *rand.Rand, max int, set map[int]struct{}) int {
	for {
		value := rng.Intn(max)
		if _, ok := set[value]; !ok {
			return value
		}
	}
}

func readValuesFromFile(filePath string) ([]int, []int) {
	file, err := os.Open(filePath)
	if err != nil {
		panic(err)
	}

	keys := make([]int, 0)
	values := make([]int, 0)
	scanner := bufio.NewScanner(file)
	// optionally, resize scanner's capacity for lines over 64K, see next example
	for scanner.Scan() {
		fmt.Println(scanner.Text())
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

func main() {
	// Create a new index and insert some values.
	index := index.NewIndex()

	// source := rand.NewSource(42)
	// rng := rand.New(source)

	// existing := make(map[int]struct{})
	// N := 1000
	// for i := 0; i < N; i++ {
	// 	random := randomNotInSet(rng, N*10, existing)
	// 	existing[random] = struct{}{}
	// 	index.Insert(random, i)
	// }

	keys, values := readValuesFromFile("values.txt")

	for i := 0; i < len(keys); i++ {
		index.Insert(keys[i], values[i])
	}

	for i := 0; i < len(keys); i++ {
		payload, err := index.Find(keys[i])
		if err != nil {
			fmt.Println(err)
		} else {
			fmt.Printf("✅ Key: %d, Value: %d, Payload: %d\n", keys[i], values[i], *payload)
		}
	}

	fmt.Println("Hello")
}

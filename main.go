package main

import (
	"alex_go/index"
	"alex_go/shared"
	"fmt"
)

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

	keys, values := shared.ReadValuesFromFile("values.txt")

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
}

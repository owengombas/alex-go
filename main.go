package main

import (
	"alex_go/index"
	"fmt"
	"math/rand"
)

func main() {
	source := rand.NewSource(42)
	rng := rand.New(source)

	// Create a new index and insert some values.
	index := index.NewIndex()

	for i := 0; i < 1000; i++ {
		index.Insert(i, rng.Int())
	}

	fmt.Println(index)
}

package shared

import (
	"bufio"
	"fmt"
	"math/rand"
	"os"
)

func Pow2RoundUp(x int) int {
	x--
	x |= x >> 1
	x |= x >> 2
	x |= x >> 4
	x |= x >> 8
	x |= x >> 16
	x |= x >> 32
	x++
	return x
}

func Log2RoundDown(x int) int {
	var result int
	for x > 1 {
		x >>= 1
		result++
	}
	return result
}

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

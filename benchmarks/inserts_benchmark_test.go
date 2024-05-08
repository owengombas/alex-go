package benchmarks

import (
	"alex_go/index"
	"alex_go/shared"
	"fmt"
	"testing"
)

func BenchmarkSequentialInserts(b *testing.B) {
	alex := index.NewIndex()
	keys, values := shared.ReadValuesFromFile("../values.txt")

	b.StartTimer()
	for i := 0; i < len(keys); i++ {
		err := alex.Insert(keys[i], values[i])
		if err != nil {
			fmt.Println(err)
			b.FailNow()
		}
	}
	b.StopTimer()
}

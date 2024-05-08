package benchmarks

import (
	"alex_go/index"
	"alex_go/utils"
	"fmt"
	"testing"
)

func BenchmarkSequentialInserts(b *testing.B) {
	alex := index.NewIndex()
	keys, values := utils.ReadValuesFromFile("../values.txt")

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

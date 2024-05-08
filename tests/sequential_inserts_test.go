package tests

import (
	"alex_go/index"
	"alex_go/utils"
	"fmt"
	"testing"
)

func TestSequentialInserts(t *testing.T) {
	alex := index.NewIndex()
	keys, values := utils.ReadValuesFromFile("../values.txt")

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

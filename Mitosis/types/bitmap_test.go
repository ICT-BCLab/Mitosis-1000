package types

import (
	"fmt"
	"testing"
)

func TestBitMap(t *testing.T) {
	bitmap := NewBitmap(16)
	bitmap.SetKey(8)
	bitmap.SetKey(10)

	bitmap1 := NewBitmap(16)
	bitmap1.SetKey(11)
	bitmap1.SetKey(0)
	bitmap1.SetKey(10)
	fmt.Printf("%v\n", bitmap.Merge(bitmap1))
	println(bitmap.String())
	for i := uint32(0); i < 16; i++ {
		println(i, bitmap.HasKey(i))
	}

}

//10000

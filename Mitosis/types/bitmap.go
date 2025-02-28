package types

import (
	"github.com/emirpasic/gods/utils"
)

type Bitmap []byte

func NewBitmap(len uint32) Bitmap {
	size := (len + 7) / 8
	b := make(Bitmap, size)
	for i := uint32(0); i < size; i++ {
		b[i] = 0
	}
	return b
}

func (b Bitmap) HasKey(x uint32) bool {
	return b[x>>3]&(1<<(x%8)) != 0
}

func (b Bitmap) SetKey(x uint32) {
	b[x>>3] |= 1 << (x % 8)
}

func (b Bitmap) Merge(x Bitmap) []uint32 {
	if len(b) != len(x) {
		return nil
	}
	var NewKeys []uint32
	for i := 0; i < len(b); i++ {
		diff := (b[i] & x[i]) ^ x[i]
		b[i] |= x[i]
		for j := uint32(i) * 8; diff > 0; j++ {
			if diff&1 == 1 {
				NewKeys = append(NewKeys, j)
			}
			diff = diff >> 1
		}

	}
	return NewKeys
}

func (b Bitmap) GetElement() []uint32 {
	var Elements []uint32
	for i := 0; i < len(b); i++ {
		t := b[i]
		for j := uint32(i) * 8; t > 0; j++ {
			if t&1 == 1 {
				Elements = append(Elements, j)
			}
			t = t >> 1
		}

	}
	return Elements
}

func (b Bitmap) GetSize() uint32 {
	var ans uint32 = 0
	for i := 0; i < len(b); i++ {
		t := b[i]
		for j := uint32(i) * 8; t > 0; j++ {
			if t&1 == 1 {
				ans += 1
			}
			t = t >> 1
		}

	}
	return ans
}

func (b Bitmap) String() string {
	var ans string
	ans += "["
	for i := 0; i < len(b); i++ {
		if i != 0 {
			ans += ","
		}
		ans += utils.ToString(b[i])
	}
	ans += "]"
	return ans
}
func (b Bitmap) Copy() Bitmap {
	ans := make(Bitmap, len(b))
	copy(ans[:], b[:])
	return ans
}

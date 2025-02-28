// Copyright 2014 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package trie

// Trie keys are dealt with in three distinct encodings:
//
// KEYBYTES encoding contains the actual key and nothing else. This encoding is the
// input to most API functions.
//
// HEX encoding contains one byte for each nibble of the key and an optional trailing
// 'terminator' byte of value 0x10 which indicates whether or not the node at the key
// contains a value. Hex key encoding is used for nodes loaded in memory because it's
// convenient to access.
//
// COMPACT encoding is defined by the Ethereum Yellow Paper (it's called "hex prefix
// encoding" there) and contains the bytes of the key and a flag. The high nibble of the
// first byte contains the flag; the lowest bit encoding the oddness of the length and
// the second-lowest encoding whether the node at the key is a value node. The low nibble
// of the first byte is zero in the case of an even number of nibbles and the first nibble
// in the case of an odd number. All remaining nibbles (now an even number) fit properly
// into the remaining bytes. Compact encoding is used for nodes stored on disk.

func keybytesToBinary(str []byte) []byte {
	l := len(str)*8 + 1
	var nibbles = make([]byte, l)
	for i, b := range str {
		nibbles[i*8] = b / 128
		b = b % 128
		nibbles[i*8+1] = b / 64
		b = b % 64
		nibbles[i*8+2] = b / 32
		b = b % 32
		nibbles[i*8+3] = b / 16
		b = b % 16
		nibbles[i*8+4] = b / 8
		b = b % 8
		nibbles[i*8+5] = b / 4
		b = b % 4
		nibbles[i*8+6] = b / 2
		b = b % 2
		nibbles[i*8+7] = b
	}
	nibbles[l-1] = 2

	return nibbles
}

func binaryToKeybytes(bin []byte) []byte {
	if hasTermBin(bin) {
		bin = bin[:len(bin)-1]
	}
	if len(bin)%8 != 0 {
		panic("can't convert hex key of odd length")
	}
	key := make([]byte, len(bin)/8)
	decodeNibblesBin(bin, key)
	return key
}

func compactToBinary(compact []byte) []byte {
	if len(compact) == 0 {
		return compact
	}
	base := keybytesToBinary(compact)
	if base[0] == 0 {
		base = base[:len(base)-1]
	}
	binodd := base[1]<<2 + base[2]<<1 + base[3]
	// apply odd flag
	chop := uint8(4)
	if binodd > 4 {
		chop = chop + 4 + 8 - binodd
	} else {
		chop = chop + 4 - binodd
	}
	return base[chop:]
}

func binaryToCompact(binary []byte) []byte {
	terminator := byte(0)
	if hasTermBin(binary) {
		terminator = 1
		binary = binary[:len(binary)-1]
	}
	buf := make([]byte, (len(binary)+3)/8+1)
	buf[0] = terminator << 7 // the flag byte
	binodd := uint8(len(binary) % 8)
	buf[0] = buf[0] | binodd<<4
	if binodd > 4 {
		for i, ni := uint8(0), binodd-1; i < binodd; i, ni = i+1, ni-1 {
			buf[1] |= binary[i] << ni
		}
		// first nibble is contained in the first byte
		binary = binary[binodd:]
		decodeNibblesBin(binary, buf[2:])
	} else {
		for i, ni := uint8(0), binodd-1; i < binodd; i, ni = i+1, ni-1 {
			buf[0] |= binary[i] << ni
		}
		// first nibble is contained in the first byte
		binary = binary[binodd:]
		decodeNibblesBin(binary, buf[1:])
	}
	return buf
}

func binaryToCompactInPlace(binary []byte) int {
	var (
		binaryLen  = len(binary) // length of the hex input
		firstByte  = byte(0)
		secondByte = byte(0)
	)
	// Check if we have a terminator there
	if binaryLen > 0 && binary[binaryLen-1] == 2 {
		firstByte = 1 << 7
		binaryLen-- // last part was the terminator, ignore that
	}
	binaryodd := binaryLen % 8
	firstByte = firstByte | (byte(binaryodd) << 4)
	var (
		binLen = (binaryLen+3)/8 + 1
		ni     = 0 // index in binary
		bi     = 1 // index in bin (compact)
	)
	if binaryodd > 4 {
		for i := binaryodd - 1; ni < binaryodd; ni, i = ni+1, i-1 {
			secondByte = secondByte | binary[ni]<<i
		}
		binary[1] = secondByte
		bi++
	} else {
		for i := binaryodd - 1; ni < binaryodd; ni, i = ni+1, i-1 {
			firstByte = firstByte | binary[ni]<<i
		}
	}
	for ; ni < binaryLen; bi, ni = bi+1, ni+8 {
		binary[bi] = binary[ni]<<7 | binary[ni+1]<<6 | binary[ni+2]<<5 | binary[ni+3]<<4 | binary[ni+4]<<3 | binary[ni+5]<<2 | binary[ni+6]<<1 | binary[ni+7]
	}
	binary[0] = firstByte

	return binLen
}

func decodeNibblesBin(nibbles []byte, bytes []byte) {
	for bi, ni := 0, 0; ni < len(nibbles); bi, ni = bi+1, ni+8 {
		bytes[bi] = nibbles[ni]<<7 | nibbles[ni+1]<<6 | nibbles[ni+2]<<5 | nibbles[ni+3]<<4 | nibbles[ni+4]<<3 | nibbles[ni+5]<<2 | nibbles[ni+6]<<1 | nibbles[ni+7]
	}
}

// prefixLen returns the length of the common prefix of a and b.
func prefixLen(a, b []byte) int {
	var i, length = 0, len(a)
	if len(b) < length {
		length = len(b)
	}
	for ; i < length; i++ {
		if a[i] != b[i] {
			break
		}
	}
	return i
}

// hasTerm returns whether a hex key has the terminator flag.

func hasTermBin(s []byte) bool {
	return len(s) > 0 && s[len(s)-1] == 2
}

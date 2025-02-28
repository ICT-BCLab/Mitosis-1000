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

package types

import (
	"bytes"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/rlp"
)

// Hasher is the tool used to calculate the hash of derivable list.
type Hasher interface {
	Reset()
	Update([]byte, []byte)
	Hash() common.Hash
}

func GetChunkRoot(txs []Transaction, hasher Hasher) common.Hash {
	hasher.Reset()
	keybuf := new(bytes.Buffer)

	// StackTrie requires values to be inserted in increasing
	// hash order, which is not the order that `list` provides
	// hashes in. This insertion sequence ensures that the
	// order is correct.
	for i := 1; i <= len(txs); i++ {
		keybuf.Reset()
		rlp.Encode(keybuf, uint(i))
		hash := txs[i-1].GetHash()
		hasher.Update(keybuf.Bytes(), hash.Bytes())
	}
	return hasher.Hash()
}

func GetBlockTxRoot(chunks []OutboundChunk, hasher Hasher) common.Hash {
	hasher.Reset()
	keybuf := new(bytes.Buffer)

	// StackTrie requires values to be inserted in increasing
	// hash order, which is not the order that `list` provides
	// hashes in. This insertion sequence ensures that the
	// order is correct.
	for i := 0; i < len(chunks); i++ {
		keybuf.Reset()
		rlp.Encode(keybuf, uint(i+1001))
		hash := chunks[i].Root()
		hasher.Update(keybuf.Bytes(), hash.Bytes())
	}
	return hasher.Hash()
}

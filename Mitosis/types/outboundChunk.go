package types

import (
	"github.com/KyrinCode/Mitosis/message/payload"
	"github.com/KyrinCode/Mitosis/trie"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/rlp"
)

// shardstate 需要有一个[map[blockHash][inboundChunk],...]
// 收到跨片消息后先检查本地是否有从中继分片拉取过源分片区块头
// 有的话直接验证，没有的话放到上述数据结构
// 收到以本分片为目的分片的区块头后，检查上述数据结构中是否有等待处理的chunk

type OutboundChunk struct {
	BlockHash common.Hash   `json:"BlockHash"` // inbound block hash (to get corresponding block tx root)
	Txs       []Transaction `json:"Txs"`       // --> chunk root --> + chunk proof --> block tx root
	// MerkleRoot common.Hash // chunk root
	ChunkProof [][]byte `json:"ChunkProof"` // chunk proof
}

// before invoke: first compute tx chunk root
// next compute block tx root and chunk proof with other chunks
func NewOutboundChunk(blockHash common.Hash, txs []Transaction, proof [][]byte) *OutboundChunk {
	return &OutboundChunk{
		BlockHash:  blockHash,
		Txs:        txs,
		ChunkProof: proof,
	}
}

func (c OutboundChunk) MarshalBinary() []byte {
	cBytes, _ := rlp.EncodeToBytes(c)
	return cBytes
}

func (c *OutboundChunk) UnmarshalBinary(data []byte) error {
	return rlp.DecodeBytes(data, c)
}

func (c OutboundChunk) Copy() payload.Safe {
	ChunkProof := make([][]byte, len(c.ChunkProof))
	for i := range c.ChunkProof {
		ChunkProof[i] = make([]byte, len(c.ChunkProof[i]))
		copy(ChunkProof[i][:], c.ChunkProof[i][:])
	}

	var Txs []Transaction
	for _, tx := range c.Txs {
		newTx := tx.Copy().(Transaction)
		Txs = append(Txs, newTx)
	}
	return OutboundChunk{
		BlockHash:  c.BlockHash,
		Txs:        Txs,
		ChunkProof: ChunkProof,
	}
}

func (c *OutboundChunk) Root() common.Hash {
	txTrie := new(trie.Trie)
	return GetChunkRoot(c.Txs, txTrie)
}

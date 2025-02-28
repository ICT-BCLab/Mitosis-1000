package types

import (
	"encoding/json"
	"errors"

	"github.com/KyrinCode/Mitosis/message/payload"

	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/rlp"
)

type Block struct {
	Header

	Transactions  []Transaction   `json:"Transactions"` // 作为发送分片的在这里
	InboundChunks []OutboundChunk `json:"InboundChunk"` // 作为目的分片的在这里
}

func NewBlock(h Header, txs []Transaction, inboundChunks []OutboundChunk) *Block {
	return &Block{
		Header:        h,
		Transactions:  txs,
		InboundChunks: inboundChunks,
	}
}

func (blk Block) MarshalJson() []byte {
	b, err := json.Marshal(blk)
	if err != nil {
		return nil
	}
	return b
}

func NewBlockFromJson(data []byte) (*Block, error) {
	if len(data) == 0 {
		return nil, errors.New("Empty input")
	}

	var blk Block
	if err := json.Unmarshal(data, &blk); err != nil {
		return nil, err
	}

	return &blk, nil
}

func (blk Block) MarshalBinary() []byte {
	blkBytes, err := rlp.EncodeToBytes(blk)
	if err != nil {
		log.Error("Block Encode To Bytes error", err)
	}
	return blkBytes
}

func (blk *Block) UnmarshalBinary(data []byte) error {
	err := rlp.DecodeBytes(data, blk)
	return err
}

func (blk Block) Copy() payload.Safe {
	H := blk.Header.Copy().(Header)
	Txs := make([]Transaction, 0)
	InboundChunks := make([]OutboundChunk, 0)

	for _, tx := range blk.Transactions {
		newTx := tx.Copy().(Transaction)
		Txs = append(Txs, newTx)
	}

	for _, inboundChunk := range blk.InboundChunks {
		newInboundChunk := inboundChunk.Copy().(OutboundChunk)
		InboundChunks = append(InboundChunks, newInboundChunk)
	}

	return Block{
		Header:        H,
		Transactions:  Txs,
		InboundChunks: InboundChunks,
	}
}

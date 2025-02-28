package types

import (
	"github.com/KyrinCode/Mitosis/message/payload"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/rlp"
)

type HeaderForHash struct {
	ShardId           uint32
	PrevBlockHash     common.Hash
	Height            uint32
	OutboundDstBitmap Bitmap // for relay to broadcast
	// InBoundBlocks []InBoundBlock
	TxRoot common.Hash
	// PrevStateRoot     common.Hash
	StateRoot common.Hash
	// ValidateProofHash common.Hash
	Timestamp uint64 `json:"TimeStamp"`
}
type Header struct {
	HeaderForHash

	Hash       common.Hash
	SignBitMap Bitmap
	Signature  []byte
}

func NewHeaderForHash(h *Header) *HeaderForHash {
	header := &HeaderForHash{
		ShardId:           h.ShardId,
		PrevBlockHash:     h.PrevBlockHash,
		Height:            h.Height,
		OutboundDstBitmap: h.OutboundDstBitmap,
		// InBoundBlocks:     inBoundBlocks,
		TxRoot: h.TxRoot,
		// PrevStateRoot:     h.PrevStateRoot,
		StateRoot: h.StateRoot,
		// ValidateProofHash: h.ValidateProofHash,
		Timestamp: h.Timestamp,
	}
	return header
}
func (h HeaderForHash) ComputeHash() common.Hash {
	data := h.MarshalBinary()
	return crypto.Keccak256Hash(data)
}

func (h HeaderForHash) MarshalBinary() []byte {
	hBytes, _ := rlp.EncodeToBytes(h)
	return hBytes
}

func (h *HeaderForHash) UnmarshalBinary(data []byte) error {
	return rlp.DecodeBytes(data, h)
}

func NewHeader(shardId, height uint32, prevBlockHash, stateRoot, txRoot common.Hash, outboundBitmap Bitmap, timestamp uint64) *Header {
	header := &Header{
		HeaderForHash: HeaderForHash{
			ShardId:           shardId,
			PrevBlockHash:     prevBlockHash,
			Height:            height,
			OutboundDstBitmap: outboundBitmap,
			// InBoundBlocks:     inBoundBlocks,
			TxRoot: txRoot,
			// PrevStateRoot:     prevStateRoot,
			StateRoot: stateRoot,
			// ValidateProofHash: validateProofHash,
			Timestamp: timestamp,
		},
	}
	header.Hash = NewHeaderForHash(header).ComputeHash()
	return header
}

func (h Header) MarshalBinary() []byte {
	hBytes, err := rlp.EncodeToBytes(h)
	if err != nil {
		log.Error("Head Encode To Bytes error", err)
	}
	return hBytes
}

func (h *Header) UnmarshalBinary(data []byte) error {
	return rlp.DecodeBytes(data, h)
}

func (h Header) Copy() payload.Safe {

	var hash [32]byte
	var prevBlockHash [32]byte
	var stateRoot [32]byte
	var txRoot [32]byte
	signature := make([]byte, len(h.Signature))

	copy(hash[:], h.Hash[:])
	copy(prevBlockHash[:], h.PrevBlockHash[:])
	copy(stateRoot[:], h.StateRoot[:])
	copy(txRoot[:], h.TxRoot[:])
	copy(signature, h.Signature)
	outboundBitmap := h.OutboundDstBitmap.Copy()

	return Header{
		HeaderForHash: HeaderForHash{
			ShardId:           h.ShardId,
			PrevBlockHash:     prevBlockHash,
			Height:            h.Height,
			OutboundDstBitmap: outboundBitmap,
			// InBoundBlocks:     inBoundBlocks,
			TxRoot: txRoot,
			// PrevStateRoot:     prevStateRoot,
			StateRoot: stateRoot,
			Timestamp: h.Timestamp,
			// ValidateProofHash: validateProofHash,
		},
		Hash:      hash,
		Signature: signature,
	}
}

func (h *Header) GetHash() common.Hash {
	return NewHeaderForHash(h).ComputeHash()
}

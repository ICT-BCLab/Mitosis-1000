package types

import (
	"github.com/KyrinCode/Mitosis/message/payload"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/rlp"
)

type MessageType byte

const (
	MessageType_PREPARE MessageType = iota
	MessageType_PREPAREVOTE
	MessageType_PRECOMMIT
	MessageType_PRECOMMITVOTE
	MessageType_COMMIT
	MessageType_COMMITVOTE
)

type BFTMessage struct {
	MessageType MessageType

	// ViewID    uint32
	BlockNum  uint32
	BlockHash common.Hash

	Block              Block
	SenderSig          []byte // 还是直接叫Sig？
	SenderPubkeyBitmap Bitmap
}

func (msg BFTMessage) MarshalBinary() []byte {
	msgBytes, err := rlp.EncodeToBytes(msg)
	if err != nil {
		log.Error("BFT Encode To Bytes error", err)
	}
	return msgBytes
}

func (msg *BFTMessage) UnmarshalBinary(data []byte) error {
	return rlp.DecodeBytes(data, msg)
}

func (msg BFTMessage) Copy() payload.Safe {
	sign := make([]byte, len(msg.SenderSig))
	copy(sign[:], msg.SenderSig[:])
	bitmap := make([]byte, len(msg.SenderPubkeyBitmap))
	copy(bitmap[:], msg.SenderPubkeyBitmap[:])
	var blockHash [32]byte
	copy(blockHash[:], msg.BlockHash[:])

	newB := msg.Block.Copy().(Block)
	return BFTMessage{
		msg.MessageType,
		// msg.ViewID,
		msg.BlockNum,
		blockHash,
		newB,
		sign,
		bitmap,
	}

}

func NewBFTMsg(messageType MessageType, block Block, sign []byte, bitmap Bitmap) *BFTMessage {
	return &BFTMessage{
		messageType,
		// viewID,
		block.Height,
		block.Hash,
		block,
		sign,
		bitmap,
	}
}

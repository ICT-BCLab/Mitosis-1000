package types

import (
	"encoding/json"
	"errors"

	"github.com/KyrinCode/Mitosis/message/payload"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/rlp"
)

// 针对不同的协议层应用，在此注册Data解析结构

type DataTemplate struct {
	Parameter1 uint32 `json:"parameter1"`
	Parameter2 string `json:"parameter2"`
	Parameter3 []byte `json:"parameter3"`
}

func NewDataTemplate(p1 uint32, p2 string, p3 []byte) *DataTemplate {
	dataTemplate := DataTemplate{
		Parameter1: p1,
		Parameter2: p2,
		Parameter3: p3,
	}
	return &dataTemplate
}

func (d DataTemplate) MarshalBinary() []byte {
	tBytes, err := rlp.EncodeToBytes(d)
	if err != nil {
		log.Error("Tx Encode To Bytes Error", err)
	}
	return tBytes
}

func (d *DataTemplate) UnmarshalBinary(data []byte) error {
	return rlp.DecodeBytes(data, d)
}

// FromAddr and Signature are exclusive
type Transaction struct {
	FromShard uint32         `json:"FromShard"`
	FromAddr  common.Address `json:"FromAddr"`
	ToShard   uint32         `json:"ToShard"`
	ToAddr    common.Address `json:"ToAddr"`
	Value     uint64         `json:"Value"`
	DataBytes []byte         `json:"Data"`
	// Nonce     uint64         `json:"Nonce"`
	Hash common.Hash `json:"Hash"`
	// Signature []byte         `json:"Signature"`
}

func NewTransaction(fromShard, toShard uint32, fromAddr, toAddr [20]byte, value uint64, dataBytes []byte) *Transaction {
	tx := Transaction{
		FromShard: fromShard,
		FromAddr:  fromAddr,
		ToShard:   toShard,
		ToAddr:    toAddr,
		Value:     value,
		DataBytes: dataBytes,
		// Nonce:     nonce,
	}
	tx.Hash = tx.ComputeHash()
	// tx.Signature = make([]byte, 0)
	return &tx
}

func (t Transaction) ComputeHash() common.Hash {
	var fromAddr [20]byte
	var toAddr [20]byte
	var dataBytes []byte

	copy(fromAddr[:], t.FromAddr[:])
	copy(toAddr[:], t.ToAddr[:])
	copy(dataBytes[:], t.DataBytes[:])

	txForHash := Transaction{
		FromShard: t.FromShard,
		FromAddr:  fromAddr,
		ToShard:   t.ToShard,
		ToAddr:    toAddr,
		Value:     t.Value,
		DataBytes: dataBytes,
		// Nonce:     t.Nonce,
	}
	tBytes, err := rlp.EncodeToBytes(txForHash)
	if err != nil {
		log.Error("Tx Encode To Bytes error", err)
	}
	return crypto.Keccak256Hash(tBytes)
}

func (t Transaction) GetHash() common.Hash {
	return t.Hash
}

func (t Transaction) MarshalBinary() []byte {
	tBytes, err := rlp.EncodeToBytes(t)
	if err != nil {
		log.Error("Tx Encode To Bytes Error", err)
	}
	return tBytes
}

func (t *Transaction) UnmarshalBinary(data []byte) error {
	return rlp.DecodeBytes(data, t)
}

func (t Transaction) Copy() payload.Safe {
	var fromAddr [20]byte
	var toAddr [20]byte
	var dataBytes = make([]byte, len(t.DataBytes))
	var hash [32]byte
	// var signature []byte

	copy(fromAddr[:], t.FromAddr[:])
	copy(toAddr[:], t.ToAddr[:])
	copy(dataBytes, t.DataBytes)
	copy(hash[:], t.Hash[:])
	// copy(signature[:], t.Signature[:])

	return Transaction{
		FromShard: t.FromShard,
		FromAddr:  fromAddr,
		ToShard:   t.ToShard,
		ToAddr:    toAddr,
		Value:     t.Value,
		DataBytes: dataBytes,
		// Nonce:     t.Nonce,
		Hash: hash,
		// Signature: signature,
	}
}

func (t Transaction) MarshalJson() []byte {
	b, err := json.Marshal(t)
	if err != nil {
		return nil
	}
	return b
}

func NewTransactionFromJson(data []byte) (*Transaction, error) {
	if len(data) == 0 {
		return nil, errors.New("Empty input")
	}

	var t Transaction
	if err := json.Unmarshal(data, &t); err != nil {
		return nil, err
	}

	return &t, nil
}

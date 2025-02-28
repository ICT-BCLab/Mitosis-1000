package core

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/KyrinCode/Mitosis/config"
	"github.com/KyrinCode/Mitosis/eventbus"
	"github.com/KyrinCode/Mitosis/p2p"
	"github.com/KyrinCode/Mitosis/trie"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethdb/memorydb"
	"github.com/ethereum/go-ethereum/rlp"
)

func NewConfig(nodeId uint32) *config.Config {
	topo := config.Topology{
		RShardIds: []uint32{1, 2},
		PShardIds: map[uint32][]uint32{
			1: {1001, 1002},
			2: {1003, 1004},
		},
	}

	accounts := make([]common.Address, 10)
	for i := 0; i < len(accounts); i++ {
		data := int64(i)
		bytebuf := bytes.NewBuffer([]byte{})
		binary.Write(bytebuf, binary.BigEndian, data)
		a := crypto.Keccak256Hash(bytebuf.Bytes()).String()
		accounts[i] = common.HexToAddress(a)
	}

	bootnode := "/ip4/127.0.0.1/tcp/50467/p2p/12D3KooWJLHcagKsPPf6nWtXizRTmAgsZJGrDAWP1xFFmUCT7bvx"

	return config.NewConfig("test", topo, 4, nodeId, accounts, bootnode)
}

func NewBlockChainWithId(nodeId uint32) *Blockchain {
	conf := NewConfig(nodeId)
	eb := eventbus.New()
	p2pNode := p2p.NewProtocol(eb, conf)
	bc := NewBlockchain(p2pNode, conf, eb)
	p2pNode.Start()
	return bc
}

func TestNewBlockchain(t *testing.T) {
	blockchains := []*Blockchain{}
	for nodeId := uint32(1); nodeId <= 24; nodeId++ {
		blockchains = append(blockchains, NewBlockChainWithId(nodeId))
	}
	for _, bc := range blockchains {
		bc.Server()
	}

	for {
		time.Sleep(10 * time.Second)
	}
}

func TestBlockAndChunkProof(t *testing.T) {
	bc := NewBlockChainWithId(12) // leader of 1001

	genesis := bc.CreateNewBlock()
	// fmt.Printf("%+v\n", genesis)
	fmt.Println(prettyPrint(genesis))
	fmt.Println("OutboundDstBitmap:", genesis.OutboundDstBitmap)

	for _, acc := range bc.config.Accounts {
		fmt.Println("Account ", acc.String(), " balance: ", bc.StateDB.GetBalance(acc))
	}
	fmt.Println()

	block1 := bc.CreateNewBlock()
	fmt.Println(prettyPrint(block1))
	fmt.Println("OutboundDstBitmap:", block1.OutboundDstBitmap)
	fmt.Println()

	block2 := bc.CreateNewBlock()
	fmt.Println(prettyPrint(block2))
	fmt.Println("OutboundDstBitmap:", block2.OutboundDstBitmap)
	fmt.Println()

	fmt.Println(prettyPrint(bc.OutboundChunkDB))
	fmt.Println()

	for _, acc := range bc.config.Accounts {
		fmt.Println("Account ", acc.String(), " balance: ", bc.StateDB.GetBalance(acc))
	}
	fmt.Println()

	// check outboundChunk proof
	outboundChunk := bc.OutboundChunkDB[block2.Hash][3]
	key, _ := rlp.EncodeToBytes(outboundChunk.Txs[0].ToShard)
	chunkRoot := outboundChunk.Root()
	db := memorydb.New()
	for j := 0; j < len(outboundChunk.ChunkProof); j++ {
		db.Put(crypto.Keccak256(outboundChunk.ChunkProof[j]), outboundChunk.ChunkProof[j])
	}
	// corresponding header
	header := block2.Header
	hash, err := trie.VerifyProof(header.TxRoot, key, db)
	if err != nil {
		fmt.Println(err)
	}
	if common.BytesToHash(hash) != chunkRoot {
		fmt.Printf("Error: chunk proof from shard-%d block-%s is incorrect.", outboundChunk.Txs[0].FromShard, outboundChunk.BlockHash)
	} else {
		fmt.Println("Chunk proof pass validation.")
	}
}

func TestRollBack(t *testing.T) {
	bc := NewBlockChainWithId(12) // leader of 1001

	bc.CreateNewBlock() // genesis
	bc.CreateNewBlock() // block1 empty
	block2 := bc.CreateNewBlock()

	for _, acc := range bc.config.Accounts {
		fmt.Println("Account ", acc.String(), " balance: ", bc.StateDB.GetBalance(acc))
	}
	fmt.Println()

	block3 := bc.CreateNewBlock()
	for _, acc := range bc.config.Accounts {
		fmt.Println("Account ", acc.String(), " balance: ", bc.StateDB.GetBalance(acc))
	}
	fmt.Println()

	fmt.Println("Roll back to block2")
	bc.RollBack(block2.StateRoot)
	for _, acc := range bc.config.Accounts {
		fmt.Println("Account ", acc.String(), " balance: ", bc.StateDB.GetBalance(acc))
	}
	fmt.Println()

	fmt.Println("Roll back to block3")
	bc.RollBack(block3.StateRoot)
	for _, acc := range bc.config.Accounts {
		fmt.Println("Account ", acc.String(), " balance: ", bc.StateDB.GetBalance(acc))
	}
}

func TestExecute(t *testing.T) {
	bc0 := NewBlockChainWithId(12) // leader of 1001
	bc1 := NewBlockChainWithId(11) // validator of 1001

	genesis := bc0.CreateNewBlock()
	err := bc1.CheckBlock(*genesis)
	if err != nil {
		t.Error("Genesis validation failed")
	} else {
		fmt.Println("Genesis validation passed")
	}
	bc1.Commit(genesis)
	bc0.Commit(genesis)

	block1 := bc0.CreateNewBlock()
	err = bc1.CheckBlock(*block1)
	if err != nil {
		t.Error("Block1 validation failed")
	} else {
		fmt.Println("Block1 validation passed")
	}
	bc1.Commit(block1)
	bc0.Commit(block1)

	block2 := bc0.CreateNewBlock()
	err = bc1.CheckBlock(*block2)
	if err != nil {
		t.Error("Block2 validation failed")
	} else {
		fmt.Println("Block2 validation passed")
	}
	bc1.Commit(block2)
	bc0.Commit(block2)

	for _, acc := range bc0.config.Accounts {
		fmt.Println("Account ", acc.String(), " balance: ", bc0.StateDB.GetBalance(acc))
	}
	fmt.Println()

	for _, acc := range bc1.config.Accounts {
		fmt.Println("Account ", acc.String(), " balance: ", bc1.StateDB.GetBalance(acc))
	}
	fmt.Println()
}

func prettyPrint(i interface{}) string {
	s, _ := json.MarshalIndent(i, "", "\t")
	return string(s)
}

package types

import (
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/emirpasic/gods/utils"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
)

func TestTransaction(t *testing.T) {
	accounts := make([]common.Address, 10)
	for i := 0; i < 10; i++ {
		a := crypto.Keccak256Hash([]byte{byte(i)}).String()
		accounts[i] = common.HexToAddress(a)
	}

	tx := NewTransaction(1, 2, accounts[0], accounts[1], 1, []byte{1, 2, 3, 4})
	txCopy := tx.Copy()
	fmt.Println(txCopy)
}

func TestBlock(t *testing.T) {

	accounts := make([]common.Address, 10)
	for i := 0; i < 10; i++ {
		a := crypto.Keccak256Hash([]byte{byte(i)}).String()
		accounts[i] = common.HexToAddress(a)
	}

	h := NewHeader(1, 0, common.Hash{}, common.Hash{}, common.Hash{}, Bitmap{}, uint64(time.Now().Unix()))
	tx1 := NewTransaction(1, 2, accounts[0], accounts[1], 1, make([]byte, 0))
	tx2 := NewTransaction(2, 1, accounts[2], accounts[3], 1, make([]byte, 0))
	proof := [][]byte{[]byte{1}, []byte{2}}
	otx := NewOutboundChunk(common.Hash{}, []Transaction{*tx2}, proof)
	blk := NewBlock(*h, []Transaction{*tx1}, []OutboundChunk{*otx})

	//jsonByte, _ := json.Marshal(blk)
	blockName := "./Blocks/" + utils.ToString(1) + "-" + utils.ToString(blk.Height) + ".json"
	jsonByte := blk.MarshalJson()
	os.WriteFile(blockName, jsonByte, 0644)
	fmt.Println(jsonByte)

}

type ShardStartConfig struct {
	ShardId     uint32 `json:"ShardId"`
	StartNodeId uint32 `json:"StartNodeId"`
	EndNodeId   uint32 `json:"EndNodeId"`
}
type SystemConfig struct {
	MnodeNum    uint32
	CnodeNum    uint32
	ShardNum    uint32
	Safe        uint32
	ShardFlag   string
	ShardConfig []ShardStartConfig
}

func TestConfig(t *testing.T) {

	var cons []ShardStartConfig
	for i := 0; i < 2; i++ {
		cons = append(cons, ShardStartConfig{uint32(i), 0, 30})
	}
	syscons := SystemConfig{30, 30, 3, 1, "adddrrr", cons}

	for _, in := range cons {
		fmt.Println(in.ShardId, " ", in.StartNodeId, " ", in.EndNodeId)
	}
	jsonByte, err := json.Marshal(syscons)
	if err != nil {
		fmt.Println(err)
	}
	os.WriteFile("config.json", jsonByte, 0644)
	/*
		var info []StartConfig
		filePtr, _ := os.Open("./config.json")
		jsonData, _ := ioutil.ReadAll(filePtr)
		json.Unmarshal(jsonData, &info)
		for _, in := range info {
			fmt.Println(in.ShardId, " ", in.StartNodeId, " ", in.EndNodeId)
		}

	*/
}

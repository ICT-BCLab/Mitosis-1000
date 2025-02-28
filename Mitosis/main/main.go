package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"io/ioutil"
	"log"

	// "fmt"
	"time"

	"github.com/KyrinCode/Mitosis/config"
	"github.com/KyrinCode/Mitosis/eventbus"
	"github.com/KyrinCode/Mitosis/p2p"

	// "github.com/KyrinCode/Mitosis/types"
	"github.com/KyrinCode/Mitosis/core"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
)

type ConfigData struct {
	RShardIds []uint32            `json:"RShardIds"`
	PShardIds map[uint32][]uint32 `json:"PShardIds"`
	Nodes     map[string][]uint32 `json:"nodes"` // 添加 Nodes 字段
}

type BootNodeData struct {
	Bootnode string `json:"bootnode"`
}

var configData *ConfigData
var bootnodeData *BootNodeData

func LoadConfigFromFile(filePath string) (*ConfigData, error) {
	file, err := ioutil.ReadFile(filePath)
	if err != nil {
		return nil, err
	}

	var configData ConfigData
	err = json.Unmarshal(file, &configData)
	if err != nil {
		return nil, err
	}

	return &configData, nil
}

func LoadBootnodeFromFile(filePath string) (*BootNodeData, error) {
	file, err := ioutil.ReadFile(filePath)
	if err != nil {
		return nil, err
	}

	var bootnodeData BootNodeData
	err = json.Unmarshal(file, &bootnodeData)
	if err != nil {
		return nil, err
	}

	return &bootnodeData, nil
}

func NewConfig(nodeId uint32) *config.Config {
	if configData == nil {
		log.Fatalf("Config data not loaded")
	}

	if bootnodeData == nil {
		log.Fatalf("bootnode data not loaded")
	}

	topo := config.Topology{
		RShardIds: configData.RShardIds,
		PShardIds: configData.PShardIds,
	}

	bootnode := bootnodeData.Bootnode

	// topo := config.Topology{
	// 	RShardIds: []uint32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12},
	// 	PShardIds: map[uint32][]uint32{
	// 		1:  {1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008, 1009, 1010},
	// 		2:  {1011, 1012, 1013, 1014, 1015, 1016, 1017, 1018, 1019, 1020},
	// 		3:  {1021, 1022, 1023, 1024, 1025, 1026, 1027, 1028, 1029, 1030},
	// 		4:  {1031, 1032, 1033, 1034, 1035, 1036, 1037, 1038, 1039, 1040},
	// 		5:  {1041, 1042, 1043, 1044, 1045, 1046, 1047, 1048, 1049, 1050},
	// 		6:  {1051, 1052, 1053, 1054, 1055, 1056, 1057, 1058, 1059, 1060},
	// 		7:  {1061, 1062, 1063, 1064, 1065, 1066, 1067, 1068, 1069, 1070},
	// 		8:  {1071, 1072, 1073, 1074, 1075, 1076, 1077, 1078, 1079, 1080},
	// 		9:  {1081, 1082, 1083, 1084, 1085, 1086, 1087, 1088, 1089, 1090},
	// 		10: {1091, 1092, 1093, 1094, 1095, 1096, 1097, 1098, 1099, 1100},
	// 		11: {1101, 1102, 1103, 1104, 1105, 1106, 1107, 1108, 1109, 1110},
	// 		12: {1111, 1112, 1113, 1114, 1115, 1116, 1117, 1118, 1119, 1120},
	// 	},
	// }

	accounts := make([]common.Address, 10)
	for i := 0; i < len(accounts); i++ {
		data := int64(i)
		bytebuf := bytes.NewBuffer([]byte{})
		binary.Write(bytebuf, binary.BigEndian, data)
		a := crypto.Keccak256Hash(bytebuf.Bytes()).String()
		accounts[i] = common.HexToAddress(a)
	}

	//bootnode := "/ip4/10.156.169.19/tcp/10000/p2p/12D3KooWCHAfRBZ7WWwwHmnqhRFmqqUPN9LypbdkEmLbdtVsuYbd"

	return config.NewConfig("test", topo, 4, nodeId, accounts, bootnode)
}

func NewBFTProtocolWithId(nodeId uint32) *BFTProtocol {
	conf := NewConfig(nodeId)
	eb := eventbus.New()
	p2pNode := p2p.NewProtocol(eb, conf)
	bc := core.NewBlockchain(p2pNode, conf, eb)
	bft := NewBFTProtocol(p2pNode, eb, bc, conf)
	p2pNode.Start()
	bc.Server()
	bft.Server()
	return bft
}

func NewBlockChainWithId(nodeId uint32) *core.Blockchain {
	conf := NewConfig(nodeId)
	eb := eventbus.New()
	p2pNode := p2p.NewProtocol(eb, conf)
	bc := core.NewBlockchain(p2pNode, conf, eb)
	p2pNode.Start()
	return bc
}

func SetConfigData(data *ConfigData) {
    configData = data
}

func SetBootnodeData(data *BootNodeData) {
    bootnodeData = data
}

func main() {
	// time.Sleep(15 * time.Minute)
	host := "host2"

	// 读取配置
	configData1, _ := LoadConfigFromFile("../config/config.json")
	configData = configData1

	// 读取bootnode
	bootnodeData1, _ := LoadBootnodeFromFile("../config/bootnode.json")
	bootnodeData = bootnodeData1
	nodes := configData.Nodes[host]
	r_begin, r_end := nodes[0], nodes[1]
	p_begin, p_end := nodes[2], nodes[3]

	blockchains := []*core.Blockchain{}

	for nodeId := uint32(r_begin); nodeId <= r_end; nodeId++ {
		bc := NewBlockChainWithId(nodeId)
		bc.Server()
		blockchains = append(blockchains, bc)
	}

	var bfts []*BFTProtocol
	for nodeId := uint32(p_begin); nodeId <= p_end; nodeId++ {
		bft := NewBFTProtocolWithId(nodeId)
		bft.Start()
		bfts = append(bfts, bft)
	}

	for {
		time.Sleep(time.Minute)
	}
}

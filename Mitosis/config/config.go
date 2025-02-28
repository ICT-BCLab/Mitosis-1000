package config

import (
	"bytes"
	"encoding/json"
	"log"
	"os"

	"github.com/ethereum/go-ethereum/common"

	logger "github.com/sirupsen/logrus"
)

type Topology struct {
	// GShardId  uint32              `json:"GShardId"`
	RShardIds []uint32            `json:"RShardIds"`
	PShardIds map[uint32][]uint32 `json:"PShardIds"` // key: RShardId value: PShardId list
}

type Config struct {
	Version string `json:"version"` // ShardFlag

	Topo            Topology            `json:"topology"`
	Nodes           map[uint32][]uint32 `json:"nodes"` // key: ShardId value: nodeId list
	NodeNumPerShard uint32              `json:"nodeNumPerShard"`
	NodeId          uint32              `json:"nodeId"`
	ShardId         uint32              `json:"shardId"`
	RShardId        uint32              `json:"RShardId"` // PShard only

	// Failed   bool `json:"failed"`
	IsLeader bool `json:"isLeader"`
	// IsMShard bool `json:"is_MShard"`

	RShardNum uint32 `json:"RShardNum"`
	PShardNum uint32 `json:"PShardNum"`
	// ShardNumber      uint32 `json:"ShardNumber"`
	// CShardNodeNumber uint32 `json:"CShardNodeNumber"`
	// MShardNodeNumber uint32 `json:"MShardNodeNumber"`

	// MaxFaultyShards            uint32 `json:"MaxFaultyShards"`
	ConsensusThreshold uint32 `json:"consensusThreshold"`
	// ConsensusThresholdInMShard uint32 `json:"ConsensusThresholdInMShard"`
	// ValidateThresholdInCShard  uint32 `json:"ValidateThresholdInCShard"`
	// ValidateThresholdInMShard  uint32 `json:"ValidateThresholdInMShard"`

	// ChallengePeriod int64
	Accounts []common.Address `json:"accounts"`
	Bootnode string           `json:"bootnode"`
}

func NewConfig(version string, topo Topology, nodeNumPerShard, nodeId uint32, accounts []common.Address, bootnode string) *Config {

	version = "Mitosis-Shard-" + version

	// compute conf.Nodes
	nodes := make(map[uint32][]uint32)
	nodeIdCounter := uint32(1)
	// RShards
	for shardId := 1; shardId <= len(topo.RShardIds); shardId++ {
		nodes[uint32(shardId)] = make([]uint32, nodeNumPerShard)
		for i := 0; i < int(nodeNumPerShard); i++ {
			nodes[uint32(shardId)][i] = nodeIdCounter
			nodeIdCounter++
		}
	}
	// PShards
	for _, shardId := range topo.RShardIds {
		// fmt.Printf("shardId: %d\n", shardId)
		// fmt.Printf("topo.RShardIds[shardId-1]: %d\n", topo.RShardIds[shardId-1])
		// fmt.Printf("topo.PShardIds[topo.RShardIds[shardId-1]]: %d\n", topo.PShardIds[topo.RShardIds[shardId-1]])
		for _, pShardId := range topo.PShardIds[topo.RShardIds[shardId-1]] {
			nodes[pShardId] = make([]uint32, nodeNumPerShard)
			for i := 0; i < int(nodeNumPerShard); i++ {
				nodes[pShardId][i] = nodeIdCounter
				nodeIdCounter++
			}
		}
	}

	// compute conf.ShardId
	var shardId uint32
	a := (nodeId - 1) / nodeNumPerShard
	// b := conf.NodeId % conf.NodeNumPerShard
	if int(a) < len(topo.RShardIds) {
		shardId = a + 1
	} else {
		shardId = a + 1001 - uint32(len(topo.RShardIds))
	}

	// compute conf.RShardId conf.RShardNum conf.PShardNum
	shardIds := []uint32{}
	var rShardId uint32
	for _, RShardId := range topo.RShardIds {
		shardIds = append(shardIds, topo.PShardIds[RShardId]...)
		for _, PShardId := range topo.PShardIds[RShardId] {
			if PShardId == shardId {
				rShardId = RShardId
			}
		}
	}

	// compute conf.ConsensusThreshold
	consensusThreshold := nodeNumPerShard*2/3 + 1
	// consensusThreshold := uint32(1)
	// compute conf.IsLeader
	isLeader := false
	if nodeId%nodeNumPerShard == 0 {
		isLeader = true
	}

	return &Config{
		Version:            version,
		Topo:               topo,
		Nodes:              nodes,
		NodeNumPerShard:    nodeNumPerShard,
		NodeId:             nodeId,
		ShardId:            shardId,
		RShardId:           rShardId,
		IsLeader:           isLeader,
		ConsensusThreshold: consensusThreshold,
		// IsMShard:                   shardId == 0,
		RShardNum: uint32(len(topo.RShardIds)),
		PShardNum: uint32(len(shardIds)),
		// ShardNumber:                shardNum,
		// CShardNodeNumber:           cShardnodeNumber,
		// MShardNodeNumber:           mShardnodeNumber,
		// MaxFaultyShards:            safeThreshold,
		// ConsensusThresholdInCShard: consensusThresholdInCShard,
		// ValidateThresholdInCShard:  validateThresholdInCShard,
		// ValidateThresholdInMShard:  validateThresholdInMShard,
		// ChallengePeriod:            0,
		Accounts: accounts,
		Bootnode: bootnode,
		// Failed:                     false,
	}
}

// func NewBadConfig(shardId, nodeId, shardNum, cShardnodeNumber, mShardnodeNumber, safeThreshold uint32, shardFlag string) *Config {
// 	cfg := NewConfig(shardId, nodeId, shardNum, cShardnodeNumber, mShardnodeNumber, safeThreshold, shardFlag)
// 	cfg.Failed = true
// 	return cfg
// }

func LoadConfig(filename string) (Config, bool) {
	var conf Config

	data, err := os.ReadFile(filename)
	if err != nil {
		logger.Error("Read config file error")
		return conf, false
	}

	configJson := []byte(data)
	err = json.Unmarshal(configJson, &conf)

	if err != nil {
		logger.Error("unmarshal json data error")
		return conf, false
	}
	return conf, true
}

func (cfg *Config) SaveConfig(filename string) error {
	var prettyJSON bytes.Buffer
	data, err := json.Marshal(cfg)

	err = json.Indent(&prettyJSON, data, "", "\t")

	if err != nil {
		logger.Fatal(err)
	}

	fp, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE, 0755)
	if err != nil {
		logger.Fatal(err)
		return err
	}

	_, err = fp.Write(prettyJSON.Bytes())
	if err != nil {
		log.Fatal(err)
		return err
	}
	return nil
}

func (cfg *Config) String() string {
	var prettyJSON bytes.Buffer
	data, _ := json.Marshal(cfg)

	json.Indent(&prettyJSON, data, "", "\t")

	return prettyJSON.String()
}

// func (cfg Config) Copy(shardId, nodeId uint32) *Config {
// 	accounts := make([]common.Address, len(cfg.Accounts))
// 	copy(accounts, cfg.Accounts)
// 	return &Config{
// 		NodeId:                     nodeId,
// 		ShardId:                    shardId,
// 		ShardFlag:                  cfg.ShardFlag,
// 		IsLeader:                   nodeId == 0,
// 		IsMShard:                   shardId == 0,
// 		ShardNumber:                cfg.ShardNumber,
// 		CShardNodeNumber:           cfg.CShardNodeNumber,
// 		MShardNodeNumber:           cfg.MShardNodeNumber,
// 		MaxFaultyShards:            cfg.MaxFaultyShards,
// 		ConsensusThresholdInCShard: cfg.ConsensusThresholdInCShard,
// 		ConsensusThresholdInMShard: cfg.ConsensusThresholdInMShard,
// 		ValidateThresholdInCShard:  cfg.ValidateThresholdInCShard,
// 		ValidateThresholdInMShard:  cfg.ValidateThresholdInMShard,
// 		Accounts:                   accounts,
// 		ChallengePeriod:            300,
// 		Failed:                     false,
// 	}

// }

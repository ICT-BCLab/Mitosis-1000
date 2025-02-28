package config

import (
	// "github.com/ethereum/go-ethereum/common"
	// "github.com/ethereum/go-ethereum/crypto"

	"fmt"
	"testing"
)

func TestConfig(t *testing.T) {

	conf, success := LoadConfig("./config_test.json")
	if success {
		fmt.Println(conf.String())
	}

	conf2 := NewConfig(conf.Version, conf.Topo, conf.NodeNumPerShard, conf.NodeId, conf.Accounts, conf.Bootnode)

	fmt.Println(conf2.String())

	conf2.SaveConfig("./config_test2.json")

	// // compute conf.Nodes
	// nodeId := uint32(1)
	// // RShards
	// for shardId := 1; shardId <= len(conf.Topo.RShardIds); shardId++ {
	// 	conf.Nodes[uint32(shardId)] = make([]uint32, conf.NodeNumPerShard)
	// 	for i := 0; i < int(conf.NodeNumPerShard); i++ {
	// 		conf.Nodes[uint32(shardId)][i] = nodeId
	// 		nodeId++
	// 	}
	// }
	// // PShards
	// for _, shardId := range conf.Topo.RShardIds {
	// 	for _, pShardId := range conf.Topo.PShardIds[conf.Topo.RShardIds[shardId-1]] {
	// 		conf.Nodes[pShardId] = make([]uint32, conf.NodeNumPerShard)
	// 		for i := 0; i < int(conf.NodeNumPerShard); i++ {
	// 			conf.Nodes[pShardId][i] = nodeId
	// 			nodeId++
	// 		}
	// 	}
	// }

	// // compute conf.ShardId
	// a := (conf.NodeId - 1) / conf.NodeNumPerShard
	// // b := conf.NodeId % conf.NodeNumPerShard
	// if int(a) < len(conf.Topo.RShardIds) {
	// 	conf.ShardId = a + 1
	// } else {
	// 	conf.ShardId = a + 1001 - uint32(len(conf.Topo.RShardIds))
	// }

	// // compute conf.RShardId conf.RShardNum conf.PShardNum
	// shardIds := []uint32{}
	// for _, RShardId := range conf.Topo.RShardIds {
	// 	shardIds = append(shardIds, conf.Topo.PShardIds[RShardId]...)
	// 	for _, PShardId := range conf.Topo.PShardIds[RShardId] {
	// 		if PShardId == conf.ShardId {
	// 			conf.RShardId = RShardId
	// 		}
	// 	}
	// }
	// conf.RShardNum = uint32(len(conf.Topo.RShardIds))
	// conf.PShardNum = uint32(len(shardIds))

	// // compute conf.ConsensusThreshold
	// conf.ConsensusThreshold = conf.NodeNumPerShard*2/3 + 1

	// // compute conf.IsLeader
	// conf.IsLeader = false
	// if conf.NodeId%conf.NodeNumPerShard == 0 {
	// 	conf.IsLeader = true
	// }

	// fmt.Println(conf.String())

	// conf.SaveConfig("./config_test2.json")
}

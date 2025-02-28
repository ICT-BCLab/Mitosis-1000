package p2p

import (
	"github.com/KyrinCode/Mitosis/config"
	"github.com/KyrinCode/Mitosis/eventbus"
	"github.com/KyrinCode/Mitosis/types"
	"github.com/ethereum/go-ethereum/common"
)

// Protocol
type Protocol struct {
	node   *P2PNode
	config *config.Config
}

// NewProtocol create a new Protocol object
func NewProtocol(broker *eventbus.EventBus, config *config.Config) *Protocol {
	br := NewBaseReader(broker)
	node := NewP2PNode(br, config)
	return &Protocol{
		node:   node,
		config: config,
	}
}

func (p *Protocol) Start() {
	go p.node.Launch()
}

func (p *Protocol) Gossip(msg []byte, shardId uint32) {
	p.node.Gossip(msg, shardId)
}

// GShard only
func (p *Protocol) GossipAll(msg []byte) {
	if p.node.conf.ShardId != 0 {
		return
	}
	p.node.GossipAll(msg)
}

func (p *Protocol) Gossip_validator(bftMessage *types.BFTMessage, shardId uint32, length1 int, phase int) {
	p.node.Gossip_validator(bftMessage, shardId, length1, phase)
}

func (p *Protocol) Transfer(shardId uint32, nodeId uint32, block_height uint32, block_hash common.Hash, rshardId uint32) {
	p.node.Transfer1(shardId, nodeId, block_height, block_hash, rshardId)
}

// type GossipLevel int

// const (
// 	GossipInLocalShard GossipLevel = iota
// 	GossipInLeaderNet
// 	GossipInALlShard
// )

// func (p *Protocol) Gossip(msg []byte, s GossipLevel) {
// 	switch s {
// 	case GossipInLocalShard:
// 		p.GossipInLocalShard(msg)
// 	case GossipInLeaderNet:
// 		p.GossipInLeaderNet(msg)
// 	case GossipInALlShard:
// 		p.GossipInAllShards(msg)
// 	}
// }
// func (p *Protocol) GossipInLocalShard(msg []byte) {
// 	p.shardNode.Gossip(msg)
// }

// func (p *Protocol) GossipInLeaderNet(msg []byte) {
// 	if !p.config.IsLeader {
// 		return
// 	}
// 	p.leaderNode.Gossip(msg)
// }

// func (p *Protocol) GossipInAllShards(msg []byte) {
// 	if !p.config.IsLeader {
// 		return
// 	}
// 	p.leaderNode.Gossip(msg)
// 	p.shardNode.Gossip(msg)
// }

package main

import (
	"fmt"
	"sync"
	"time"

	mitosisbls "github.com/KyrinCode/Mitosis/bls"
	"github.com/KyrinCode/Mitosis/config"
	"github.com/KyrinCode/Mitosis/core"
	"github.com/KyrinCode/Mitosis/eventbus"
	"github.com/KyrinCode/Mitosis/message"
	"github.com/KyrinCode/Mitosis/p2p"
	"github.com/KyrinCode/Mitosis/topics"
	"github.com/KyrinCode/Mitosis/types"

	"github.com/ethereum/go-ethereum/common"
	"github.com/herumi/bls-eth-go-binary/bls"
	logger "github.com/sirupsen/logrus"
)

var logBFT = logger.WithField("process", "consensus")

// Leader: Prepare (准备一下) -> phase: Prepare
// Validator: Prepare-Vote (我准备好了) -> phase: Prepare
// Leader: Precommit (足够人准备好了) -> phase: Precommit
// Validator: Precommit-Vote (我知道足够人准备好了) -> phase: Precommit
// Leader: Commit (足够人知道足够人准备好了，可以commit了) -> phase: Commit
// Validator: Commit-Vote (commit了) -> phase: Commit
// (这里Leader要不要把足够人commit的消息广播一下呢)
// Leader: Prepare (足够人上一轮commit了 下一轮准备一下) -> phase: Prepare
// ...

type BFTPhase uint

const (
	BFT_INIT BFTPhase = iota
	BFT_PREPARE
	BFT_PRECOMMIT
	BFT_COMMIT
)

type Phase struct {
	height   uint32
	bftPhase BFTPhase
}

func (p *Phase) Switch(height uint32, bftPhase BFTPhase) {
	if p.height > height || (p.height == height && p.bftPhase >= bftPhase) {
		return
	}
	p.height = height
	p.bftPhase = bftPhase
}

func (p *Phase) IsNew(x Phase) bool { // if x is later than p?
	if p.height > x.height || (p.height == x.height && p.bftPhase >= x.bftPhase) {
		return false
	}
	return true
}

func (p *Phase) IsOut(x Phase) bool { // used to rebroadcast msgs until outdated
	if p.height > x.height || (p.height == x.height && p.bftPhase > x.bftPhase) {
		return true
	}
	return false
}

type BFTProtocol struct {
	node       *p2p.Protocol
	broker     *eventbus.EventBus
	bls        *mitosisbls.BLS
	blockchain *core.Blockchain
	config     *config.Config

	// blockDB   map[common.Hash]types.Block
	// blockLink map[common.Hash]common.Hash

	phase Phase
	mu    sync.Mutex

	latestCommittedBlockHash common.Hash // 用 blockchain.LatestBlock.Hash()
	currentBlock             *types.Block
	aggregatePrepareSig      *bls.Sign
	aggregatePrecommitSig    *bls.Sign // 放到blockHeader里的签名
	aggregateCommitSig       *bls.Sign
	prepareBitmap            *types.Bitmap
	precommitBitmap          *types.Bitmap // 放到blockHeader里的签名
	commitBitmap             *types.Bitmap

	// viewId uint32

	bftMsgQueue chan message.Message
	bftMsgSubID uint32
	// stop           bool
}

func NewBFTProtocol(p2pNode *p2p.Protocol, eb *eventbus.EventBus, blockchain *core.Blockchain, config *config.Config) *BFTProtocol {

	return &BFTProtocol{
		node:         p2pNode,
		broker:       eb,
		blockchain:   blockchain,
		bls:          blockchain.BLS,
		config:       config,
		phase:        Phase{0, BFT_INIT},
		currentBlock: nil,
		bftMsgQueue:  make(chan message.Message, 1000000),
		// blockDB:      make(map[common.Hash]types.Block),
		// blockLink:    make(map[common.Hash]common.Hash),
		// viewId:       0,
		// stop:         false,
	}
}

func (bft *BFTProtocol) Server() {
	if bft.config.IsLeader {
		bft.bftMsgSubID = bft.broker.Subscribe(topics.ConsensusLeader, eventbus.NewChanListener(bft.bftMsgQueue))
	} else {
		bft.bftMsgSubID = bft.broker.Subscribe(topics.ConsensusValidator, eventbus.NewChanListener(bft.bftMsgQueue))
	}
	go bft.HandleBFTMsg()
}

func (bft *BFTProtocol) HandleBFTMsg() {
	for data := range bft.bftMsgQueue {
		bftMsg := data.Payload().(types.BFTMessage)
		var err error
		if bft.config.IsLeader {
			switch bftMsg.MessageType {
			case types.MessageType_PREPAREVOTE:
				err = bft.OnPrepareVoteMsg(bftMsg)
			case types.MessageType_PRECOMMITVOTE:
				err = bft.OnPrecommitVoteMsg(bftMsg)
			case types.MessageType_COMMITVOTE:
				err = bft.OnCommitVoteMsg(bftMsg)
			}
		} else {
			switch bftMsg.MessageType {
			case types.MessageType_PREPARE:
				err = bft.OnPrepareMsg(bftMsg)
			case types.MessageType_PRECOMMIT:
				err = bft.OnPrecommitMsg(bftMsg)
			case types.MessageType_COMMIT:
				err = bft.OnCommitMsg(bftMsg)
			}
		}
		if err != nil {
			logBFT.WithError(err).Error(fmt.Sprintf("[Node-%d-%d] process block-%d-%d-%x, type %d error",
				bft.config.ShardId, bft.config.NodeId, bftMsg.Block.ShardId, bftMsg.BlockNum, bftMsg.BlockHash, bftMsg.MessageType))
		}
	}
	logBFT.Infof("[Node-%d-%d] Stop!!!", bft.config.ShardId, bft.config.NodeId)
}

func (bft *BFTProtocol) Start() {
	if bft.config.IsLeader {
		go func() {
			if bft.blockchain.LatestBlock != nil {
				bft.currentBlock = bft.blockchain.LatestBlock
				bft.latestCommittedBlockHash = bft.blockchain.LatestBlock.Hash
			}
			logBFT.Infof("[Node-%d-%d] start consensus ", bft.config.ShardId, bft.config.NodeId)
			time.Sleep(40 * time.Minute)
			bft.Prepare()
		}()
	} else {
		if bft.blockchain.LatestBlock != nil {
			bft.currentBlock = bft.blockchain.LatestBlock
			bft.latestCommittedBlockHash = bft.blockchain.LatestBlock.Hash
		}
	}
}

func (bft *BFTProtocol) TryGossip(phase Phase, bftMessage *types.BFTMessage, topic topics.Topic) {
	for {
		msg := message.NewBlockchainMessage(topic, *bftMessage)
		msgBytes, _ := msg.MarshalBinary()
		bft.node.Gossip(msgBytes, bft.config.ShardId)
		logBFT.Infof("[Node-%d-%d] gossip bft msg <block-%d-%d-%x, type-%d>, size: %d.", bft.config.ShardId, bft.config.NodeId, bftMessage.Block.ShardId, bftMessage.BlockNum, bftMessage.BlockHash, bftMessage.MessageType, len(msgBytes))
		// if bftMessage.MessageType == types.MessageType_PREPARE || bftMessage.MessageType == types.MessageType_PRECOMMIT || bftMessage.MessageType == types.MessageType_COMMIT {
		// 	time.Sleep(15 * time.Second)
		// } else {
		// 	time.Sleep(15 * time.Second)
		// }
		// 间隔一段时间后phase仍为变化就再发一次
		time.Sleep(15 * time.Second)
		bft.mu.Lock()
		out := bft.phase.IsOut(phase)
		bft.mu.Unlock()
		if !out {
			bft.node.Gossip(msgBytes, bft.config.ShardId)
		}
	}
}

func (bft *BFTProtocol) nodeId2Key() uint32 {
	return bft.config.NodeId - (bft.config.RShardNum+bft.config.ShardId-1001)*bft.config.NodeNumPerShard - 1
}

func (bft *BFTProtocol) key2NodeId(key uint32) uint32 {
	return key + (bft.config.RShardNum+bft.config.ShardId-1001)*bft.config.NodeNumPerShard + 1
}

func (bft *BFTProtocol) getKeyOffset() uint32 {
	return (bft.config.RShardNum+bft.config.ShardId-1001)*bft.config.NodeNumPerShard + 1
}

func (bft *BFTProtocol) TryGossip_validator(phase Phase, bftMessage *types.BFTMessage, topic topics.Topic) {
	msg := message.NewBlockchainMessage(topic, *bftMessage)
	msgBytes, _ := msg.MarshalBinary()
	logBFT.Infof("[Node-%d-%d] gossip bft msg <block-%d-%d-%x, type-%d>, size: %d.", bft.config.ShardId, bft.config.NodeId, bftMessage.Block.ShardId, bftMessage.BlockNum, bftMessage.BlockHash, bftMessage.MessageType, len(msgBytes))
	bft.node.Gossip_validator(bftMessage, bft.config.ShardId, len(msgBytes), int(phase.bftPhase))
}

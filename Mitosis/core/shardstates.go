package core

import (
	"sync"

	mitosisbls "github.com/KyrinCode/Mitosis/bls"
	"github.com/KyrinCode/Mitosis/config"
	"github.com/KyrinCode/Mitosis/trie"
	"github.com/KyrinCode/Mitosis/types"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethdb/memorydb"
	"github.com/ethereum/go-ethereum/rlp"
)

type ShardState struct { // mitosis heads从中继拿
	Headers       map[common.Hash]*types.Header        // Header db, key: blockHash
	InboundChunks map[common.Hash]*types.OutboundChunk // out Tx db 存其他分片发给本分片的交易
	mu            sync.Mutex
}

func NewShardState(c *config.Config) *ShardState {
	shardState := &ShardState{
		Headers:       make(map[common.Hash]*types.Header),
		InboundChunks: make(map[common.Hash]*types.OutboundChunk),
	}
	return shardState
}

func (s *ShardState) UpdateShardStateWithInboundChunk(inboundChunk *types.OutboundChunk) (bool, bool) { // 第一个bool代表是否是第一次收到，第二个bool代表是否header和chunk都凑齐了
	s.mu.Lock()
	defer s.mu.Unlock()

	_, ok := s.InboundChunks[inboundChunk.BlockHash] // 重复收到的处理在这里做 先检查已经有了的话 直接return false
	if !ok {
		s.InboundChunks[inboundChunk.BlockHash] = inboundChunk
	} else {
		return false, false
	}

	checked := false
	header, ok := s.Headers[inboundChunk.BlockHash] // check header exists or not, if exists: verify
	if ok {
		checked = s.CheckInboundChunk(header, inboundChunk)
		if checked {
			logChain.Infof("InboundChunk from shard-%d block-%s has been checked successfully.", inboundChunk.Txs[0].FromShard, inboundChunk.BlockHash)
		}
	}
	return true, checked // if both header and inboundChunk: return true for outside function to decide whether put into txpool (leader)
}

func (s *ShardState) UpdateShardStateWithHeader(header *types.Header) (bool, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	_, ok := s.Headers[header.Hash] // 重复收到的处理在这里做 先检查已经有了的话 直接return false
	if !ok {
		s.Headers[header.Hash] = header
	} else {
		return false, false
	}

	checked := false
	inboundChunk, ok := s.InboundChunks[header.Hash] // check inboundChunk exists or not, if exists: verify
	if ok {
		checked = s.CheckInboundChunk(header, inboundChunk)
		if checked {
			logChain.Infof("InboundChunk from shard-%d block-%s has been checked successfully.", inboundChunk.Txs[0].FromShard, inboundChunk.BlockHash)
		}
	}
	return true, checked // if both header and inboundChunk: return true for outside function to decide whether put into txpool (leader)
}

func (s *ShardState) UpdateShardStateWithHeaderRShard(header *types.Header) bool { // 是否是第一次
	// RShard only record header here
	s.mu.Lock()
	defer s.mu.Unlock()

	_, ok := s.Headers[header.Hash]
	if !ok {
		s.Headers[header.Hash] = header
		return true
	} else {
		return false
	}
}

func (s *ShardState) CheckInboundChunk(header *types.Header, inboundChunk *types.OutboundChunk) bool {
	if len(inboundChunk.Txs) != 0 {
		key, _ := rlp.EncodeToBytes(inboundChunk.Txs[0].ToShard) // key是shardId
		chunkRoot := inboundChunk.Root()
		db := memorydb.New()
		for j := 0; j < len(inboundChunk.ChunkProof); j++ {
			db.Put(crypto.Keccak256(inboundChunk.ChunkProof[j]), inboundChunk.ChunkProof[j])
		}
		hash, _ := trie.VerifyProof(header.TxRoot, key, db)

		if common.BytesToHash(hash) != chunkRoot {
			logChain.Errorf("Chunk proof from shard-%d block-%s is incorrect.", inboundChunk.Txs[0].FromShard, inboundChunk.BlockHash)
			return false
		}
	}
	return true
}

type ShardStates struct {
	StateDB   map[uint32]*ShardState // key: shardId
	stateDBMu sync.Mutex

	txPool *TxPool

	//BLS
	BLSSign *mitosisbls.BLS
	Config  *config.Config
}

func NewShardStates(conf *config.Config, txpool *TxPool, bls *mitosisbls.BLS) *ShardStates {
	s := &ShardStates{
		StateDB: make(map[uint32]*ShardState),
		txPool:  txpool,
		BLSSign: bls,
		Config:  conf,
	}

	for i := uint32(0); i < conf.PShardNum; i++ { // 这里RShard和PShard都做一样的处理
		s.StateDB[i+1001] = NewShardState(conf)
	}
	return s
}

func (s *ShardStates) UpdateHeader(msg types.Header) bool { // 是否第一次收到
	s.stateDBMu.Lock()
	defer s.stateDBMu.Unlock()

	first, checked := false, false
	st, ok := s.StateDB[msg.ShardId]
	if ok && s.CheckHeader(msg) {
		if s.Config.ShardId > 1000 { // PShard
			first, checked = st.UpdateShardStateWithHeader(&msg)
		} else { // RShard只更新header，在外层根据是否是第一次收到广播给片内及header中的dstShards
			first = st.UpdateShardStateWithHeaderRShard((&msg))
		}
	}
	if checked && s.Config.IsLeader {
		inboundChunk := st.InboundChunks[msg.Hash]
		s.AddInboundChunkToTxPool(inboundChunk)
	}
	return first
}

func (s *ShardStates) CheckHeader(msg types.Header) bool {
	consensusThreshold := s.Config.ConsensusThreshold
	if len(msg.SignBitMap.GetElement()) < int(consensusThreshold) {
		logChain.Errorf("Signatures of Header-%d-%d is not enough, (%d / %d)", msg.ShardId, msg.Height, len(msg.SignBitMap.GetElement()), consensusThreshold)
		return false
	}
	offset := s.getKeyOffset(msg.ShardId)
	if !s.BLSSign.VerifyAggregateSig(msg.Signature, msg.Hash.Bytes(), msg.SignBitMap, msg.ShardId, offset) {
		if s.BLSSign.PubKeys.GetAggregatePubKey(msg.ShardId, msg.SignBitMap, offset) == nil {
			logChain.Infof("[Node %d-%d ] CrossMSG Error: Block %d-%d signature is incorrect !!! NoPUB!!, Nodes: %v",
				s.Config.ShardId, s.Config.NodeId, msg.ShardId, msg.Height, msg.SignBitMap.GetElement())
			for _, i := range msg.SignBitMap.GetElement() {
				pub := s.BLSSign.GetPubKeyWithNodeId(msg.ShardId, i)
				logChain.Infof("[Node %d-%d ] CrossMSG Error: Block %d-%d signature is incorrect !!! NoPUB!!, Nodes %d-%d Pub: %v",
					s.Config.ShardId, s.Config.NodeId, msg.ShardId, msg.Height, msg.ShardId, i, pub)
			}

		} else {
			logChain.Infof("[Node %d-%d ] CrossMSG Error: Block %d-%d signature is incorrect !!! SignMsg: %s, Sign: %v, AggPub: %v, ValidateResult:%v",
				s.Config.ShardId, s.Config.NodeId, msg.ShardId, msg.Height, msg.Hash.String(), msg.Signature,
				s.BLSSign.PubKeys.GetAggregatePubKey(msg.ShardId, msg.SignBitMap, offset).Serialize(), s.BLSSign.VerifyAggregateSig(msg.Signature, msg.Hash.Bytes(), msg.SignBitMap, msg.ShardId, offset))

		}

		//logChain.Error("signatures of Block %d-%d is incorrect !!!!", s.BLSSign.VerifyAggregateSign(msg.Signature, msg.Hash.Bytes(), msg.SignBitMap, msg.ShardId))
		//logChain.Errorf("Crosstx: signatures of Block %d-%d is incorrect !!!!,hash:%s,sign:%v", msg.ShardId, msg.Height, msg.Hash.String(), msg.Signature)
		return false
	}
	return true
}

func (s *ShardStates) UpdateInboundChunk(msg types.OutboundChunk) bool { // 是否是第一次
	s.stateDBMu.Lock()
	defer s.stateDBMu.Unlock()

	first, checked := false, false
	st, ok := s.StateDB[msg.Txs[0].FromShard]
	if ok {
		first, checked = st.UpdateShardStateWithInboundChunk(&msg)
	}
	if checked && s.Config.IsLeader {
		s.AddInboundChunkToTxPool(&msg)
	}
	return first
}

func (s *ShardStates) AddInboundChunkToTxPool(inboundChunk *types.OutboundChunk) {
	if len(inboundChunk.Txs) > 0 {
		s.txPool.AddInboundChunk(inboundChunk)
		logChain.Infof("[Node %d-%d] inboundChunk from block-%s total %d txs have been committed to txpool ", s.Config.ShardId, s.Config.NodeId, inboundChunk.BlockHash, len(inboundChunk.Txs))
	}
}

func (s *ShardStates) getKeyOffset(shardId uint32) uint32 {
	return (s.Config.RShardNum+shardId-1001)*s.Config.NodeNumPerShard + 1
}

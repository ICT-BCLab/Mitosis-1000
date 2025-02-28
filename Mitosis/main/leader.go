package main

import (
	"errors"
	"fmt"
	"time"

	"github.com/KyrinCode/Mitosis/topics"
	"github.com/KyrinCode/Mitosis/types"

	"github.com/herumi/bls-eth-go-binary/bls"
)

func (bft *BFTProtocol) Prepare() error {
	block := bft.blockchain.CreateNewBlock()
	// 输出即将被打包的交易的哈希值
	logBFT.Infof("[Node-%d-%d] The following inboundChunks transactions will be included in the block:", bft.config.ShardId, bft.config.NodeId)
	for _, inboundChunk := range block.InboundChunks {
		for _, tx := range inboundChunk.Txs {
			logBFT.Infof("[Node-%d-%d] From Shard-%d Transaction Hash: %x,Block Hash: %x", bft.config.ShardId, bft.config.NodeId, tx.FromShard, tx.Hash, block.Hash)
		}
	}
	logBFT.Infof("[Node-%d-%d] start bft consensus with new block-%d-%d-%x.", bft.config.ShardId, bft.config.NodeId, block.ShardId, block.Height, block.Hash)
	bitmap := types.NewBitmap(bft.config.NodeNumPerShard)
	bitmap.SetKey(bft.nodeId2Key())
	msg := append(block.Hash.Bytes(), byte(types.MessageType_PREPARE))
	signedMsg := bft.bls.Sign(msg)
	prepareMsg := types.NewBFTMsg(types.MessageType_PREPARE, *block, signedMsg.Serialize(), bitmap)

	bft.currentBlock = block
	// bft.blockDB[block.Hash] = *block
	bft.prepareBitmap = &bitmap
	bft.aggregatePrepareSig = signedMsg

	bft.mu.Lock()
	bft.phase.Switch(block.Height, BFT_PREPARE)
	phase := bft.phase
	bft.mu.Unlock()
	go bft.TryGossip(phase, prepareMsg, topics.ConsensusValidator) // 发给validators的
	logBFT.Infof("[Node-%d-%d] (leader) prepare new block-%d-%d-%x", bft.config.ShardId, bft.config.NodeId, block.ShardId, block.Height, block.Hash)
	// bft.TryGossip_validator(phase, prepareMsg, topics.ConsensusValidator) // 发给validators的
	// bft.Precommit()
	return nil
}

func (bft *BFTProtocol) OnPrepareVoteMsg(bftMsg types.BFTMessage) error {
	logBFT.Infof("[Node-%d-%d] (leader) phase: %+v, on prepare vote msg", bft.config.ShardId, bft.config.NodeId, bft.phase)
	if bft.currentBlock == nil {
		return errors.New("bft.currentBlock is empty")
	}
	if bft.phase.IsOut(Phase{bftMsg.BlockNum, BFT_PREPARE}) { // phase相同的话也要查看
		return nil
	}
	// if bftMsg.BlockNum < bft.currentBlock.Height {
	// 	// old vote
	// 	return nil
	// }
	if bftMsg.BlockHash != bft.currentBlock.Hash {
		return errors.New("block in prepare-vote msg differs from the current block")
	}
	if bft.prepareBitmap.GetSize() >= bft.config.ConsensusThreshold { //enough votes
		return nil
	}

	msg := append(bftMsg.BlockHash.Bytes(), byte(types.MessageType_PREPARE))
	key := bftMsg.SenderPubkeyBitmap.GetElement()[0]
	signer := bft.key2NodeId(key)
	if !bft.bls.VerifySign(bftMsg.SenderSig, msg, signer, bft.config.ShardId) {
		return fmt.Errorf("wrong signature (signer: node-%d)", signer)
	}

	newSigners := bft.prepareBitmap.Merge(bftMsg.SenderPubkeyBitmap)
	if len(newSigners) == 0 { //prepareBitmap has included node
		return nil
	}

	var sign bls.Sign
	sign.Deserialize(bftMsg.SenderSig)
	bft.aggregatePrepareSig.Add(&sign)

	if bft.prepareBitmap.GetSize() >= bft.config.ConsensusThreshold {
		bft.Precommit()
	} else {
		logBFT.Infof("[Shard-%d] block-%d-%d-%x prepared: %d/%d", bft.config.ShardId, bftMsg.Block.ShardId, bftMsg.BlockNum, bftMsg.BlockHash, bft.prepareBitmap.GetSize(), bft.config.ConsensusThreshold)
	}
	return nil
}

func (bft *BFTProtocol) Precommit() error {
	bft.mu.Lock()
	bft.phase.Switch(bft.currentBlock.Height, BFT_PRECOMMIT)
	phase := bft.phase
	bft.mu.Unlock()
	precommitMsg := types.NewBFTMsg(types.MessageType_PRECOMMIT, *bft.currentBlock, bft.aggregatePrepareSig.Serialize(), *bft.prepareBitmap)
	go bft.TryGossip(phase, precommitMsg, topics.ConsensusValidator)
	logBFT.Infof("[Node-%d-%d] (leader) precommit new block-%d-%d-%x", bft.config.ShardId, bft.config.NodeId, bft.currentBlock.ShardId, bft.currentBlock.Height, bft.currentBlock.Hash)
	// bft.TryGossip_validator(phase, precommitMsg, topics.ConsensusValidator) // 发给validators的
	bitmap := types.NewBitmap(bft.config.NodeNumPerShard)
	bitmap.SetKey(bft.nodeId2Key())
	bft.precommitBitmap = &bitmap

	msg := bft.currentBlock.Hash.Bytes()
	signedMsg := bft.bls.Sign(msg)
	bft.aggregatePrecommitSig = signedMsg
	// bft.Commit()
	return nil
}

func (bft *BFTProtocol) OnPrecommitVoteMsg(bftMsg types.BFTMessage) error {
	logBFT.Infof("[Node-%d-%d] (leader) phase: %+v, on precommit vote msg", bft.config.ShardId, bft.config.NodeId, bft.phase)
	if bft.currentBlock == nil {
		return errors.New("bft.currentBlock is empty")
	}
	if bft.phase.IsOut(Phase{bftMsg.BlockNum, BFT_PRECOMMIT}) { // phase相同的话也要查看
		return nil
	}
	// if bftMsg.BlockNum < bft.currentBlock.Height {
	// 	// old vote
	// 	return nil
	// }
	if bftMsg.BlockHash != bft.currentBlock.Hash {
		return errors.New("block in precommit-vote msg differs from the current block")
	}
	if bft.precommitBitmap.GetSize() >= bft.config.ConsensusThreshold { //enough votes
		return nil
	}

	msg := bftMsg.BlockHash.Bytes()
	key := bftMsg.SenderPubkeyBitmap.GetElement()[0]
	signer := bft.key2NodeId(key)
	if !bft.bls.VerifySign(bftMsg.SenderSig, msg, signer, bft.config.ShardId) {
		return fmt.Errorf("wrong signature (signer: node-%d)", signer)
	}

	newSigners := bft.precommitBitmap.Merge(bftMsg.SenderPubkeyBitmap)
	if len(newSigners) == 0 { // included before
		return nil
	}

	var sign bls.Sign
	sign.Deserialize(bftMsg.SenderSig)
	bft.aggregatePrecommitSig.Add(&sign)

	if bft.precommitBitmap.GetSize() >= bft.config.ConsensusThreshold {
		bft.Commit()
	} else {
		logBFT.Infof("[Shard-%d] block-%d-%d-%x precommitted: %d/%d", bft.config.ShardId, bftMsg.Block.ShardId, bftMsg.BlockNum, bftMsg.BlockHash, bft.precommitBitmap.GetSize(), bft.config.ConsensusThreshold)
	}
	return nil
}

func (bft *BFTProtocol) Commit() error { // 将precommit过程的签名（对header的签名）更新到block中commit
	bft.mu.Lock()
	bft.phase.Switch(bft.currentBlock.Height, BFT_COMMIT)
	phase := bft.phase
	bft.mu.Unlock()
	commitMsg := types.NewBFTMsg(types.MessageType_COMMIT, *bft.currentBlock, bft.aggregatePrecommitSig.Serialize(), *bft.precommitBitmap)
	go bft.TryGossip(phase, commitMsg, topics.ConsensusValidator)
	logBFT.Infof("[Node-%d-%d] (leader) commit new block-%d-%d-%x", bft.config.ShardId, bft.config.NodeId, bft.currentBlock.ShardId, bft.currentBlock.Height, bft.currentBlock.Hash)
	// bft.TryGossip_validator(phase, commitMsg, topics.ConsensusValidator) // 发给validators的
	bitmap := types.NewBitmap(bft.config.NodeNumPerShard)
	bitmap.SetKey(bft.nodeId2Key())
	bft.commitBitmap = &bitmap

	msg := append(bft.currentBlock.Hash.Bytes(), byte(types.MessageType_COMMIT))
	signedMsg := bft.bls.Sign(msg)
	bft.aggregateCommitSig = signedMsg

	bft.currentBlock.Signature = bft.aggregatePrecommitSig.Serialize()
	bft.currentBlock.SignBitMap = bft.precommitBitmap.Copy()
	bft.blockchain.Commit(bft.currentBlock)
	bft.latestCommittedBlockHash = bft.blockchain.LatestBlock.Hash
	// bft.blockDB[bft.currentBlock.Hash] = *bft.currentBlock
	// time.Sleep(3 * time.Second)
	// bft.Prepare()
	return nil
}

func (bft *BFTProtocol) OnCommitVoteMsg(bftMsg types.BFTMessage) error {
	logBFT.Infof("[Node-%d-%d] (leader) phase: %+v, on commit vote msg", bft.config.ShardId, bft.config.NodeId, bft.phase)
	if bft.currentBlock == nil {
		return errors.New("bft.currentBlock is empty")
	}
	if bft.phase.IsOut(Phase{bftMsg.BlockNum, BFT_COMMIT}) { // phase相同的话也要查看
		return nil
	}
	if bftMsg.BlockHash != bft.currentBlock.Hash {
		return errors.New("block in commit-vote msg differs from the current block")
	}
	if bft.commitBitmap.GetSize() >= bft.config.ConsensusThreshold { //enough votes
		return nil
	}

	msg := append(bft.currentBlock.Hash.Bytes(), byte(types.MessageType_COMMIT))
	key := bftMsg.SenderPubkeyBitmap.GetElement()[0]
	signer := bft.key2NodeId(key)
	if !bft.bls.VerifySign(bftMsg.SenderSig, msg, signer, bft.config.ShardId) {
		return fmt.Errorf("wrong signature (signer: %d)", signer)
	}

	newSigners := bft.commitBitmap.Merge(bftMsg.SenderPubkeyBitmap)
	if len(newSigners) == 0 { // included before
		return nil
	}

	var sign bls.Sign
	sign.Deserialize(bftMsg.SenderSig)
	bft.aggregateCommitSig.Add(&sign)

	if bft.commitBitmap.GetSize() >= bft.config.ConsensusThreshold { // decide 就不把commit的签名下发了
		time.Sleep(5 * time.Second) // 出块间隔
		bft.Prepare()
	} else {
		logBFT.Infof("[Shard-%d] block-%d-%d-%x committed: %d/%d", bft.config.ShardId, bftMsg.Block.ShardId, bftMsg.BlockNum, bftMsg.BlockHash, bft.commitBitmap.GetSize(), bft.config.ConsensusThreshold)
	}
	return nil
}

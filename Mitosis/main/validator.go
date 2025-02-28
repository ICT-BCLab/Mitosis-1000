package main

import (
	"fmt"

	"github.com/KyrinCode/Mitosis/topics"
	"github.com/KyrinCode/Mitosis/types"

	"github.com/herumi/bls-eth-go-binary/bls"
)

func (bft *BFTProtocol) OnPrepareMsg(bftMsg types.BFTMessage) error {
	logBFT.Infof("[Node-%d-%d] (validator) phase: %+v, on prepare msg", bft.config.ShardId, bft.config.NodeId, bft.phase)
	if !bft.phase.IsNew(Phase{bftMsg.BlockNum, BFT_PREPARE}) { // phase相同就不用查看了
		return nil
	}

	bft.mu.Lock()
	phase := bft.phase // 本节点之前的phase
	bft.mu.Unlock()

	if bft.currentBlock == nil && bftMsg.BlockNum > 0 { // 未创世
		return fmt.Errorf("skip genesis block (current bft phase is %+v, received prepare msg block height is %d)", phase, bftMsg.BlockNum)
	}

	if bft.currentBlock != nil && bftMsg.BlockNum > bft.currentBlock.Height {
		if bftMsg.Block.PrevBlockHash != bft.latestCommittedBlockHash || bftMsg.BlockNum != bft.currentBlock.Height+1 {
			return fmt.Errorf("skip some blocks (current bft phase is %+v, received prepare msg block height is %d)", phase, bftMsg.BlockNum)
		}
	}

	// check sig
	msg := append(bftMsg.BlockHash.Bytes(), byte(types.MessageType_PREPARE))
	key := bftMsg.SenderPubkeyBitmap.GetElement()[0]
	signer := bft.key2NodeId(key)
	if !bft.bls.VerifySign(bftMsg.SenderSig, msg, signer, bft.config.ShardId) {
		return fmt.Errorf("wrong signature (sign node: %d)", signer)
	}
	if err := bft.blockchain.CheckBlock(bftMsg.Block); err != nil {
		return err
	}

	bft.currentBlock = &bftMsg.Block
	block := types.Block{Header: bftMsg.Block.Header.Copy().(types.Header)} // block with just header

	bitmap := types.NewBitmap(bft.config.NodeNumPerShard)
	bitmap.SetKey(bft.nodeId2Key())

	// new prepareVoteMsg
	msg = append(bftMsg.BlockHash.Bytes(), byte(types.MessageType_PREPARE))
	signedMsg := bft.bls.Sign(msg)
	prepareVoteMsg := types.NewBFTMsg(types.MessageType_PREPAREVOTE, block, signedMsg.Serialize(), bitmap)
	bft.mu.Lock()
	bft.phase.Switch(bftMsg.Block.Height, BFT_PREPARE)
	phase = bft.phase
	bft.mu.Unlock()
	go bft.TryGossip(phase, prepareVoteMsg, topics.ConsensusLeader) // 发给leader的
	logBFT.Infof("[Node-%d-%d] (validator) prepare vote block-%d-%d-%x", bft.config.ShardId, bft.config.NodeId, bftMsg.Block.ShardId, bftMsg.BlockNum, bftMsg.BlockHash)

	var sign bls.Sign
	sign.Deserialize(bftMsg.SenderSig)
	sign.Add(signedMsg)
	bft.aggregatePrepareSig = &sign

	newBitmap := bitmap.Copy()
	newBitmap.Merge(bftMsg.SenderPubkeyBitmap)
	bft.prepareBitmap = &newBitmap

	return nil
}

func (bft *BFTProtocol) OnPrecommitMsg(bftMsg types.BFTMessage) error {
	logBFT.Infof("[Node-%d-%d] (validator) phase: %+v, on precommit msg", bft.config.ShardId, bft.config.NodeId, bft.phase)
	if !bft.phase.IsNew(Phase{bftMsg.BlockNum, BFT_PRECOMMIT}) { // phase相同就不用查看了
		return nil
	}

	bft.mu.Lock()
	phase := bft.phase // 本节点之前的phase
	bft.mu.Unlock()

	if bft.currentBlock == nil && bftMsg.BlockNum > 0 { // 未创世
		return fmt.Errorf("skip genesis block (current bft phase is %+v, received precommit msg block height is %d)", phase, bftMsg.BlockNum)
	}

	if bft.currentBlock != nil && bftMsg.BlockNum > bft.currentBlock.Height {
		if bftMsg.Block.PrevBlockHash != bft.latestCommittedBlockHash || bftMsg.BlockNum != bft.currentBlock.Height+1 {
			return fmt.Errorf("skip some blocks (current bft phase is %+v, received precommit msg block height is %d)", phase, bftMsg.BlockNum)
		}
	}

	if bftMsg.BlockHash != bft.currentBlock.Hash {
		return fmt.Errorf("fork occurred!!! (block-%d prepared hash: %x, precommit hash: %x)", bftMsg.BlockNum, bft.currentBlock.Hash, bftMsg.BlockHash)
	}

	if bftMsg.SenderPubkeyBitmap.GetSize() < bft.config.ConsensusThreshold {
		return fmt.Errorf("not enough signatures in prepare phase (%d/%d)", bftMsg.SenderPubkeyBitmap.GetSize(), bft.config.ConsensusThreshold)
	}

	// check sig
	msg := append(bft.currentBlock.Hash.Bytes(), byte(types.MessageType_PREPARE))
	offset := bft.getKeyOffset()
	if !bft.bls.VerifyAggregateSig(bftMsg.SenderSig, msg, bftMsg.SenderPubkeyBitmap, bft.config.ShardId, offset) {
		keys := bftMsg.SenderPubkeyBitmap.GetElement()
		signers := []uint32{}
		for _, key := range keys {
			signers = append(signers, bft.key2NodeId(key))
		}
		return fmt.Errorf("wrong signature (signers: node-%v)", signers)
	}

	var sign bls.Sign
	sign.Deserialize(bftMsg.SenderSig)
	bft.aggregatePrepareSig = &sign
	bft.prepareBitmap = &bftMsg.SenderPubkeyBitmap

	block := types.Block{Header: bftMsg.Block.Header.Copy().(types.Header)} // block with just header

	bitmap := types.NewBitmap(bft.config.NodeNumPerShard)
	bitmap.SetKey(bft.nodeId2Key())

	// new precommitVoteMsg
	msg = bft.currentBlock.Hash.Bytes()
	signedMsg := bft.bls.Sign(msg)
	precommitVoteMsg := types.NewBFTMsg(types.MessageType_PRECOMMITVOTE, block, signedMsg.Serialize(), bitmap)

	bft.precommitBitmap = &bitmap
	bft.aggregatePrecommitSig = signedMsg

	bft.mu.Lock()
	bft.phase.Switch(bft.currentBlock.Height, BFT_PRECOMMIT)
	phase = bft.phase
	bft.mu.Unlock()
	go bft.TryGossip(phase, precommitVoteMsg, topics.ConsensusLeader)
	logBFT.Infof("[Node-%d-%d] (validator) precommit vote block-%d-%d-%x", bft.config.ShardId, bft.config.NodeId, bftMsg.Block.ShardId, bftMsg.BlockNum, bftMsg.BlockHash)
	return nil
}

func (bft *BFTProtocol) OnCommitMsg(bftMsg types.BFTMessage) error {
	logBFT.Infof("[Node-%d-%d] (validator) phase: %+v, on commit msg", bft.config.ShardId, bft.config.NodeId, bft.phase)
	if !bft.phase.IsNew(Phase{bftMsg.BlockNum, BFT_COMMIT}) { // phase相同就不用查看了
		return nil
	}

	bft.mu.Lock()
	phase := bft.phase
	bft.mu.Unlock()

	if bft.currentBlock == nil && bftMsg.BlockNum > 0 { // 未创世
		return fmt.Errorf("skip genesis block (current bft phase is %+v, received commit msg block height is %d)", phase, bftMsg.BlockNum)
	}

	if bft.currentBlock != nil && bftMsg.BlockNum > bft.currentBlock.Height {
		if bftMsg.Block.PrevBlockHash != bft.latestCommittedBlockHash || bftMsg.BlockNum != bft.currentBlock.Height+1 {
			return fmt.Errorf("skip some blocks (current bft phase is %+v, received commit msg block height is %d)", phase, bftMsg.BlockNum)
		}
	}

	if bftMsg.BlockHash != bft.currentBlock.Hash {
		return fmt.Errorf("fork occurred!!! (block-%d precommitted hash: %x, commit hash: %x)", bftMsg.BlockNum, bft.currentBlock.Hash, bftMsg.BlockHash)
	}

	if bftMsg.SenderPubkeyBitmap.GetSize() < bft.config.ConsensusThreshold {
		return fmt.Errorf("not enough signatures in precommit phase (%d/%d)", bftMsg.SenderPubkeyBitmap.GetSize(), bft.config.ConsensusThreshold)
	}

	// check sig
	msg := bft.currentBlock.Hash.Bytes()
	offset := bft.getKeyOffset()
	if !bft.bls.VerifyAggregateSig(bftMsg.SenderSig, msg, bftMsg.SenderPubkeyBitmap, bft.config.ShardId, offset) {
		keys := bftMsg.SenderPubkeyBitmap.GetElement()
		signers := []uint32{}
		for _, key := range keys {
			signers = append(signers, bft.key2NodeId(key))
		}
		return fmt.Errorf("wrong signature (signers: nodes%v)", signers)
	}

	var sign bls.Sign
	sign.Deserialize(bftMsg.SenderSig)
	bft.aggregatePrecommitSig = &sign
	bft.precommitBitmap = &bftMsg.SenderPubkeyBitmap

	block := types.Block{Header: bftMsg.Block.Header.Copy().(types.Header)} // block with just header

	bitmap := types.NewBitmap(bft.config.NodeNumPerShard)
	bitmap.SetKey(bft.nodeId2Key())
	bft.commitBitmap = &bitmap

	// new commitVoteMsg
	msg = append(bft.currentBlock.Hash.Bytes(), byte(types.MessageType_COMMIT))
	signedMsg := bft.bls.Sign(msg)
	commitVoteMsg := types.NewBFTMsg(types.MessageType_COMMITVOTE, block, signedMsg.Serialize(), bitmap)
	bft.aggregateCommitSig = signedMsg

	bft.mu.Lock()
	bft.phase.Switch(bft.currentBlock.Height, BFT_COMMIT)
	phase = bft.phase
	bft.mu.Unlock()
	go bft.TryGossip(phase, commitVoteMsg, topics.ConsensusLeader)
	logBFT.Infof("[Node-%d-%d] (validator) commit vote block-%d-%d-%x", bft.config.ShardId, bft.config.NodeId, bftMsg.Block.ShardId, bftMsg.BlockNum, bftMsg.BlockHash)

	// bft.blockDB[bftMsg.BlockHash] = bftMsg.Block
	// bft.blockLink[bftMsg.Block.PrevBlockHash] = bftMsg.BlockHash

	// if bftMsg.Block.PrevBlockHash != bft.LatestCommittedBlock {
	// 	return errors.New(fmt.Sprintf(" Futher Committed Block ( Current BFT State is %s, Receive Committed Msg Height is %d).", ph.String(), bftMsg.BlockNum))
	// }
	// for blkHash, ok := bftMsg.BlockHash, true; ok; blkHash, ok = bft.blockLink[blkHash] {
	// 	blk := bft.blockDB[blkHash]
	// 	bft.blockchain.Commit(&blk)
	// 	bft.LatestCommittedBlock = blkHash
	// 	bft.CurrentBlock = &blk
	// }

	bft.currentBlock.Signature = bft.aggregatePrecommitSig.Serialize()
	bft.currentBlock.SignBitMap = bft.precommitBitmap.Copy()
	bft.blockchain.Commit(bft.currentBlock)
	bft.latestCommittedBlockHash = bft.blockchain.LatestBlock.Hash

	return nil
}

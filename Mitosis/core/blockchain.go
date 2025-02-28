package core

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	mitosisbls "github.com/KyrinCode/Mitosis/bls"
	"github.com/KyrinCode/Mitosis/config"
	"github.com/KyrinCode/Mitosis/eventbus"
	"github.com/KyrinCode/Mitosis/message"
	"github.com/KyrinCode/Mitosis/p2p"
	"github.com/KyrinCode/Mitosis/state"
	"github.com/KyrinCode/Mitosis/topics"
	"github.com/KyrinCode/Mitosis/trie"
	"github.com/KyrinCode/Mitosis/types"

	"github.com/emirpasic/gods/utils"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethdb/memorydb"
	"github.com/ethereum/go-ethereum/rlp"

	// "github.com/herumi/bls-eth-go-binary/bls"
	logger "github.com/sirupsen/logrus"
)

var logChain = logger.WithField("process", "blockchain")

type ProofList [][]byte

func (n *ProofList) Put(key []byte, value []byte) error {
	*n = append(*n, value)
	return nil
}

func (n *ProofList) Delete(key []byte) error {
	panic("not supported")
}

type Blockchain struct {
	Node        *p2p.Protocol
	BLS         *mitosisbls.BLS
	ShardStates *ShardStates
	broker      *eventbus.EventBus

	// Block DataBase
	LatestBlock *types.Block            // for bft consensus
	BlockDB     map[uint32]*types.Block // key: height

	// PShard
	StateDB         *state.StateDB // mitosis 节点轮换后要向上一epoch中在该分片的节点获取stateDB.Reset(latestBlock.stateRoot)
	stateMutex      sync.Mutex
	TxPool          *TxPool
	OutboundChunkDB map[common.Hash][]types.OutboundChunk // key: blockHash

	//network message channel
	HeaderMsgQueue       chan message.Message
	InboundChunkMsgQueue chan message.Message

	HeaderMsgSubID       uint32
	InboundChunkMsgSubID uint32

	config *config.Config
}

func NewBlockchain(node *p2p.Protocol, config *config.Config, eb *eventbus.EventBus) *Blockchain {
	chain := &Blockchain{
		Node:            node,
		broker:          eb,
		LatestBlock:     nil,
		BlockDB:         make(map[uint32]*types.Block),
		OutboundChunkDB: make(map[common.Hash][]types.OutboundChunk),
		config:          config,
	}

	filename := "../bls/keygen/keystore/node" + fmt.Sprint(config.NodeId) + ".keystore"
	kp, success := mitosisbls.LoadKeyPair(filename)
	if !success {
		logChain.Errorln("load key error")
	}
	chain.BLS = mitosisbls.NewBLS(*kp, config.Topo, config.Nodes)

	chain.StateDB, _ = state.New(common.Hash{}, state.NewDatabase(rawdb.NewMemoryDatabase()))
	chain.TxPool = NewTxPool(config)
	chain.ShardStates = NewShardStates(config, chain.TxPool, chain.BLS)

	return chain
}

func (chain *Blockchain) Genesis() *types.Block {
	chain.stateMutex.Lock()
	defer chain.stateMutex.Unlock()

	accounts := chain.config.Accounts
	for _, addr := range accounts {
		chain.StateDB.AddBalance(addr, 10000000)
	}
	root, _ := chain.StateDB.Commit(false)
	head := types.NewHeader(chain.config.ShardId, 0, common.Hash{}, root, common.Hash{}, types.NewBitmap(chain.config.PShardNum), uint64(time.Now().UnixMilli()))
	// chain.OutboundChunkDB[head.ComputeHash()] = make([]types.OutboundChunk, chain.config.PShardNum)
	block := types.NewBlock(*head, nil, nil)
	chain.LatestBlock = block
	return block

}

func (chain *Blockchain) CreateNewBlock() *types.Block { // 只有PShard会需要
	if chain.LatestBlock == nil {
		return chain.Genesis()
	}

	chain.stateMutex.Lock()
	defer chain.stateMutex.Unlock()

	var txs []types.Transaction
	// var inboundChunks []types.OutboundChunk
	// txs, inboundChunks = chain.TxPool.takeTxs(100)
	inboundChunks := chain.TxPool.takeTxs1(100)
	// for _, tx := range txs {
	// 	logChain.Infof("[Node-%d-%d] test1 - Transaction Hash: %x", chain.config.ShardId, chain.config.NodeId, tx.Hash)
	// }

	// 生成跨片交易
	if chain.config.ShardId >= 1001 {
		// if int(chain.LatestBlock.Height+1)%125 == int(chain.config.ShardId-1000)%125 {
		// b := (int(chain.LatestBlock.Height+1) / 125) % 3
		tx2 := chain.TxPool.GenerateCrossShardTxs4()
		// txs = append(txs, txs2...)
		txs = append(txs, *tx2)

		logChain.Infof(
			"generate cross-shard tx, [Node-%d-%d] fromShard %d to %d tx has been generated, tx.hash is %x, FromAddr: %s, ToAddr: %s, Value: %d",
			chain.config.ShardId,
			chain.config.NodeId,
			tx2.FromShard,
			tx2.ToShard,
			tx2.Hash,
			tx2.FromAddr.Hex(), // 转换地址为字符串
			tx2.ToAddr.Hex(),   // 转换地址为字符串
			tx2.Value,
		)
		// for _, tx := range txs2 {
		// 	// 打印交易信息
		// 	// logChain.Infof("generate cross-shard tx, [Node-%d-%d] fromShard %d to %d tx has been generated, tx.hash is %x", chain.config.ShardId, chain.config.NodeId, tx.FromShard, tx.ToShard, tx.Hash)
		// 	logChain.Infof(
		// 		"generate cross-shard tx, [Node-%d-%d] fromShard %d to %d tx has been generated, tx.hash is %x, FromAddr: %s, ToAddr: %s, Value: %d",
		// 		chain.config.ShardId,
		// 		chain.config.NodeId,
		// 		tx.FromShard,
		// 		tx.ToShard,
		// 		tx.Hash,
		// 		tx.FromAddr.Hex(), // 转换地址为字符串
		// 		tx.ToAddr.Hex(),   // 转换地址为字符串
		// 		tx.Value,
		// 	)
		// }
	}

	// inbound txs
	for _, inboundChunk := range inboundChunks {
		for _, tx := range inboundChunk.Txs {
			if tx.FromShard != chain.config.ShardId && tx.ToShard == chain.config.ShardId {
				logChain.Infof("[Node-%d-%d] test2 - Transaction Hash: %x", chain.config.ShardId, chain.config.NodeId, tx.Hash)
				chain.StateDB.AddBalance(tx.ToAddr, tx.Value)
			}
		}
	}

	// outboundBitmap
	outboundDstBitmap := types.NewBitmap(chain.config.PShardNum)

	// intra-shard or outbound txs
	for _, tx := range txs {
		if tx.FromShard == chain.config.ShardId && tx.ToShard == chain.config.ShardId { // intra-shard
			balance := chain.StateDB.GetBalance(tx.FromAddr)
			// chain.StateDB.AddBalance(tx.FromAddr, tx.Value)
			if balance < tx.Value {
				continue
			}
			chain.StateDB.SubBalance(tx.FromAddr, tx.Value)
			chain.StateDB.AddBalance(tx.ToAddr, tx.Value)
		}
		if tx.FromShard == chain.config.ShardId && tx.ToShard != chain.config.ShardId { // outbound
			balance := chain.StateDB.GetBalance(tx.FromAddr)
			// chain.StateDB.AddBalance(tx.FromAddr, tx.Value)
			if balance < tx.Value {
				continue
			}
			chain.StateDB.SubBalance(tx.FromAddr, tx.Value)
			outboundDstBitmap.SetKey(tx.ToShard - 1001)
		}
	}

	stateRoot, _ := chain.StateDB.Commit(false)
	outboundChunks := make([]types.OutboundChunk, chain.config.PShardNum)
	for _, tx := range txs {
		outboundChunks[tx.ToShard-1001].Txs = append(outboundChunks[tx.ToShard-1001].Txs, tx) // intra-shard and outbound txs
	}

	// merkle tree
	txTrie := new(trie.Trie)
	txRoot := types.GetBlockTxRoot(outboundChunks, txTrie) // each outboundChunk as a leaf node
	height := chain.LatestBlock.Height + 1
	prevHash := chain.LatestBlock.GetHash()
	head := types.NewHeader(chain.config.ShardId, height, prevHash, stateRoot, txRoot, outboundDstBitmap, uint64(time.Now().UnixMilli()))

	keyBuf := new(bytes.Buffer)
	for i := range outboundChunks {
		var px ProofList
		keyBuf.Reset()
		rlp.Encode(keyBuf, uint(i+1001)) // key: shardId
		txTrie.Prove(keyBuf.Bytes(), 0, &px)
		outboundChunks[i].ChunkProof = px // each outboundChunk's proof
		outboundChunks[i].BlockHash = head.Hash
	}

	block := types.NewBlock(*head, txs, inboundChunks)
	for _, tx := range txs {
		// 打印交易信息
		logChain.Infof("[Node-%d-%d] fromShard %d to %d tx has been packed into block by leader, tx.hash is %x, block.hash is %x", chain.config.ShardId, chain.config.NodeId, tx.FromShard, tx.ToShard, tx.Hash, block.Hash)
	}
	chain.LatestBlock = block
	chain.OutboundChunkDB[block.Hash] = outboundChunks
	return block
}

func (chain *Blockchain) Commit(block *types.Block) {
	logChain.Infof("[Node-%d-%d] commit block-%d-%d-%x", chain.config.ShardId, chain.config.NodeId, block.ShardId, block.Height, block.Hash)
	chain.LatestBlock = block
	chain.BlockDB[block.Height] = block
	// leader负责落盘区块
	if chain.config.IsLeader {
		jsonByte := block.MarshalJson()
		blockName := "../blocks/" + utils.ToString(chain.config.ShardId) + "-" + utils.ToString(block.Height) + ".json"
		go os.WriteFile(blockName, jsonByte, 0644)
	}

	if ok := chain.RollBack(block.StateRoot); !ok { // leader的createNewBlock和prepare期间checkBlock的时候就执行过了，如果没收到prepare就收到commit的话需要重新执行
		chain.ExecuteBlock(*block)
	}

	chain.ShardStates.UpdateHeader(block.Header)

	// send header to RShard
	headerMsg := message.NewBlockchainMessage(topics.HeaderGossip, block.Header)
	headerMsgByte, _ := headerMsg.MarshalBinary()
	// chain.Node.Transfer(chain.config.ShardId, chain.config.NodeId, block.Height, block.Hash, chain.config.RShardId)
	chain.Node.Gossip(headerMsgByte, chain.config.RShardId)
	logChain.Infof("[Node-%d-%d] header of block-%d-%d-%x has been sent to RShard-%d", chain.config.ShardId, chain.config.NodeId, chain.config.ShardId, block.Height, block.Hash, chain.config.RShardId)
	// send crossShardMsg to dst PShard
	for i, outboundChunk := range chain.OutboundChunkDB[block.Hash] {
		if uint32(i)+1001 == chain.config.ShardId { // skip intra-shard
			continue
		}
		if len(outboundChunk.Txs) > 0 { // skip 0 txs chunk
			outboundChunkMsg := message.NewBlockchainMessage(topics.OutboundChunkGossip, outboundChunk)
			outboundChunkMsgByte, _ := outboundChunkMsg.MarshalBinary()
			chain.Node.Gossip(outboundChunkMsgByte, uint32(i)+1001)
			logChain.Infof("[Node-%d-%d] outboundChunk of block-%d-%d-%x has been sent to PShard-%d with %d txs (size: %d)", chain.config.ShardId, chain.config.NodeId, chain.config.ShardId, block.Height, block.Hash, uint32(i)+1001, len(outboundChunk.Txs), len(outboundChunkMsgByte))
		}
	}
}

func (chain *Blockchain) Server() {
	chain.HeaderMsgQueue = make(chan message.Message, 1000000)
	chain.HeaderMsgSubID = chain.broker.Subscribe(topics.HeaderGossip, eventbus.NewChanListener(chain.HeaderMsgQueue))
	go chain.HandleHeaderMsg()

	// only PShards receive inboundChunks
	if chain.config.ShardId > 1000 {
		chain.InboundChunkMsgQueue = make(chan message.Message, 1000000)
		chain.InboundChunkMsgSubID = chain.broker.Subscribe(topics.OutboundChunkGossip, eventbus.NewChanListener(chain.InboundChunkMsgQueue))
		go chain.HandleInboundChunkMsg()
	}
}

func (chain *Blockchain) HandleHeaderMsg() {
	for data := range chain.HeaderMsgQueue {
		header, _ := data.Payload().(types.Header)
		first := chain.ShardStates.UpdateHeader(header)
		logChain.Infof("[Node-%d-%d] receive header of block-%d-%d-%x", chain.config.ShardId, chain.config.NodeId, header.ShardId, header.Height, header.Hash)
		if first {
			// gossip to local shard
			headerMsg := message.NewBlockchainMessage(topics.HeaderGossip, header)
			headerMsgByte, _ := headerMsg.MarshalBinary()
			chain.Node.Gossip(headerMsgByte, chain.config.ShardId)
			logChain.Infof("[Node-%d-%d] gossip header of block-%d-%d-%x to local shard (size: %d)", chain.config.ShardId, chain.config.NodeId, header.ShardId, header.Height, header.Hash, len(headerMsgByte))
			// RShard gossip to dst PShard
			if chain.config.ShardId < 1001 {
				// todo: extract PShardIds from outboundDstBitmap
				dstPShardIds := header.OutboundDstBitmap.GetElement()
				for _, PShardId := range dstPShardIds {
					chain.Node.Gossip(headerMsgByte, PShardId+1001)
					logChain.Infof("[Node-%d-%d] gossip header of block-%d-%d-%x to dst shard-%d (size: %d)", chain.config.ShardId, chain.config.NodeId, header.ShardId, header.Height, header.Hash, PShardId, len(headerMsgByte))
				}
			}
		}
	}
}

func (chain *Blockchain) HandleInboundChunkMsg() {
	for data := range chain.InboundChunkMsgQueue {
		inboundChunk, _ := data.Payload().(types.OutboundChunk)
		if len(inboundChunk.Txs) > 0 && inboundChunk.Txs[0].ToShard == chain.config.ShardId {
			first := chain.ShardStates.UpdateInboundChunk(inboundChunk)
			logChain.Infof("[Node-%d-%d] receive inboundChunk of block-%d-%x", chain.config.ShardId, chain.config.NodeId, inboundChunk.Txs[0].FromShard, inboundChunk.BlockHash)
			for _, tx := range inboundChunk.Txs {
				// 打印交易信息
				logChain.Infof("received txs,[Node-%d-%d] fromShard %d to %d tx has been received, tx.hash is %x, block.hash is %x, FromAddr: %s, ToAddr: %s, Value: %d", chain.config.ShardId, chain.config.NodeId, tx.FromShard, tx.ToShard, tx.Hash, inboundChunk.BlockHash, tx.FromAddr.Hex(), tx.ToAddr.Hex(), tx.Value)
			}
			if first {
				inboundChunkMsg := message.NewBlockchainMessage(topics.OutboundChunkGossip, inboundChunk)
				inboundChunkMsgByte, _ := inboundChunkMsg.MarshalBinary()
				chain.Node.Gossip(inboundChunkMsgByte, chain.config.ShardId)
				logChain.Infof("[Node-%d-%d] gossip inboundChunk of block-%d-%x (size: %d)", chain.config.ShardId, chain.config.NodeId, inboundChunk.Txs[0].FromShard, inboundChunk.BlockHash, len(inboundChunkMsgByte))
			}
		}
	}
}

func (chain *Blockchain) CheckBlock(block types.Block) error { // do not check signatures here
	chain.stateMutex.Lock()
	defer chain.stateMutex.Unlock()

	prevStateRoot, _ := chain.StateDB.Commit(false)
	defer chain.RollBack(prevStateRoot)

	if block.Height == 0 {
		accounts := chain.config.Accounts
		for _, addr := range accounts {
			chain.StateDB.AddBalance(addr, 10000000)
		}
		stateRoot, _ := chain.StateDB.Commit(false)
		if stateRoot != block.StateRoot {
			return fmt.Errorf("[Node-%d-%d] block-%d-%d-%x state root is incorrect", chain.config.ShardId, chain.config.NodeId, block.ShardId, block.Height, block.Hash)
		}
		return nil
	}
	if chain.LatestBlock == nil {
		return errors.New("genesis block has not been committed")
	}
	if block.Height != chain.LatestBlock.Height+1 {
		return fmt.Errorf("[Node-%d-%d] previous block has not been committed (latest committed height: %d, this block height: %d)", chain.config.ShardId, chain.config.NodeId, chain.LatestBlock.Height, block.Height)
	}

	txs, inboundChunks := block.Transactions, block.InboundChunks

	// inbound txs
	for _, inboundChunk := range inboundChunks {
		if len(inboundChunk.Txs) == 0 {
			return errors.New("inboundChunk with 0 txs")
		}
		// check inboundChunk proof
		key, _ := rlp.EncodeToBytes(inboundChunk.Txs[0].ToShard)
		chunkRoot := inboundChunk.Root()
		db := memorydb.New()
		for j := 0; j < len(inboundChunk.ChunkProof); j++ {
			db.Put(crypto.Keccak256(inboundChunk.ChunkProof[j]), inboundChunk.ChunkProof[j])
		}
		// get the corresponding header
		fromShard := inboundChunk.Txs[0].FromShard
		var fromShardHeader *types.Header
		for {
			v, ok := chain.ShardStates.StateDB[fromShard].Headers[inboundChunk.BlockHash]
			if ok {
				fromShardHeader = v
				break
			}
			time.Sleep(100 * time.Millisecond)
		}

		hash, _ := trie.VerifyProof(fromShardHeader.TxRoot, key, db)
		if common.BytesToHash(hash) != chunkRoot {
			return fmt.Errorf("[Node-%d-%d] chunk proof from block-%d-%x is incorrect", chain.config.ShardId, chain.config.NodeId, inboundChunk.Txs[0].FromShard, inboundChunk.BlockHash)
		}
		// update balance
		for _, tx := range inboundChunk.Txs {
			if tx.FromShard != chain.config.ShardId && tx.ToShard == chain.config.ShardId {
				chain.StateDB.AddBalance(tx.ToAddr, tx.Value)
			}
		}
	}

	// intra-shard and outbound txs
	for _, tx := range txs {
		if tx.FromShard == chain.config.ShardId && tx.ToShard == chain.config.ShardId { // intra-shard
			balance := chain.StateDB.GetBalance(tx.FromAddr)
			if balance < tx.Value {
				continue
			}
			chain.StateDB.SubBalance(tx.FromAddr, tx.Value)
			chain.StateDB.AddBalance(tx.ToAddr, tx.Value)
		}
		if tx.FromShard == chain.config.ShardId && tx.ToShard != chain.config.ShardId { // outbound
			balance := chain.StateDB.GetBalance(tx.FromAddr)
			if balance < tx.Value {
				continue
			}
			chain.StateDB.SubBalance(tx.FromAddr, tx.Value)
		}
	}

	// Check StateRoot
	stateRoot, _ := chain.StateDB.Commit(false)
	if stateRoot != block.StateRoot {
		return fmt.Errorf("block-%d-%d-%x stateRoot is incorrect", block.ShardId, block.Height, block.Hash)
	}

	// Check TxRoot
	outboundChunks := make([]types.OutboundChunk, chain.config.PShardNum)
	for _, tx := range txs {
		outboundChunks[tx.ToShard-1001].Txs = append(outboundChunks[tx.ToShard-1001].Txs, tx)
	}
	txTrie := new(trie.Trie)
	txRoot := types.GetBlockTxRoot(outboundChunks, txTrie)
	if txRoot != block.TxRoot {
		return fmt.Errorf("block-%d-%d-%x txRoot is incorrect", block.ShardId, block.Height, block.Hash)
	}

	// todo: check outboundBitmap
	return nil
}

func (chain *Blockchain) ExecuteBlock(block types.Block) {
	chain.stateMutex.Lock()
	defer chain.stateMutex.Unlock()

	if block.Height == 0 {
		accounts := chain.config.Accounts
		for _, addr := range accounts {
			chain.StateDB.AddBalance(addr, 10000000)
		}
		stateRoot, _ := chain.StateDB.Commit(false)
		if stateRoot != block.StateRoot {
			logChain.Errorf("[Node-%d-%d] block-%d-%d-%x state root is incorrect", chain.config.ShardId, chain.config.NodeId, chain.config.ShardId, block.Height, block.Hash)
		}
	}
	if chain.LatestBlock == nil {
		logChain.Errorf("[Node-%d-%d] genesis block has not been committed", chain.config.ShardId, chain.config.NodeId)
	}
	if block.Height != chain.LatestBlock.Height+1 {
		logChain.Errorf("[Node-%d-%d] previous block has not been committed (latest committed height: %d, this block height: %d)", chain.config.ShardId, chain.config.NodeId, chain.LatestBlock.Height, block.Height)
	}

	txs, inboundChunks := block.Transactions, block.InboundChunks

	// inbound txs
	for _, inboundChunk := range inboundChunks {
		if len(inboundChunk.Txs) == 0 {
			logChain.Errorf("[Node-%d-%d] inboundChunk with 0 txs", chain.config.ShardId, chain.config.NodeId)
		}
		// check inboundChunk proof
		key, _ := rlp.EncodeToBytes(inboundChunk.Txs[0].ToShard)
		chunkRoot := inboundChunk.Root()
		db := memorydb.New()
		for j := 0; j < len(inboundChunk.ChunkProof); j++ {
			db.Put(crypto.Keccak256(inboundChunk.ChunkProof[j]), inboundChunk.ChunkProof[j])
		}
		// get the corresponding header
		fromShard := inboundChunk.Txs[0].FromShard
		fromShardHeader := chain.ShardStates.StateDB[fromShard].Headers[inboundChunk.BlockHash]
		hash, _ := trie.VerifyProof(fromShardHeader.TxRoot, key, db)
		if common.BytesToHash(hash) != chunkRoot {
			logChain.Errorf("[Node-%d-%d] chunk proof from block-%d-%x is incorrect", chain.config.ShardId, chain.config.NodeId, inboundChunk.Txs[0].FromShard, inboundChunk.BlockHash)
		}
		// update balance
		for _, tx := range inboundChunk.Txs {
			if tx.FromShard != chain.config.ShardId && tx.ToShard == chain.config.ShardId {
				chain.StateDB.AddBalance(tx.ToAddr, tx.Value)
			}
		}
	}

	// intra-shard and outbound txs
	for _, tx := range txs {
		if tx.FromShard == chain.config.ShardId && tx.ToShard == chain.config.ShardId { // intra-shard
			balance := chain.StateDB.GetBalance(tx.FromAddr)
			if balance < tx.Value {
				continue
			}
			chain.StateDB.SubBalance(tx.FromAddr, tx.Value)
			chain.StateDB.AddBalance(tx.ToAddr, tx.Value)
		}
		if tx.FromShard == chain.config.ShardId && tx.ToShard != chain.config.ShardId { // outbound
			balance := chain.StateDB.GetBalance(tx.FromAddr)
			if balance < tx.Value {
				continue
			}
			chain.StateDB.SubBalance(tx.FromAddr, tx.Value)
		}
	}

	// Check StateRoot
	stateRoot, _ := chain.StateDB.Commit(false)
	if stateRoot != block.StateRoot {
		logChain.Errorf("[Node-%d-%d] block-%d-%d-%x stateRoot is incorrect", chain.config.ShardId, chain.config.NodeId, block.ShardId, block.Height, block.Hash)
	}

	// Check TxRoot
	outboundChunks := make([]types.OutboundChunk, chain.config.PShardNum)
	for _, tx := range txs {
		outboundChunks[tx.ToShard-1001].Txs = append(outboundChunks[tx.ToShard-1001].Txs, tx)
	}
	txTrie := new(trie.Trie)
	txRoot := types.GetBlockTxRoot(outboundChunks, txTrie)
	if txRoot != block.TxRoot {
		logChain.Errorf("[Node-%d-%d] block-%d-%d-%x txRoot is incorrect", chain.config.ShardId, chain.config.NodeId, block.ShardId, block.Height, block.Hash)
	}

	// todo: check outboundBitmap
}

func (chain *Blockchain) RollBack(hash common.Hash) bool { // stateRoot
	stateDB, err := state.New(hash, chain.StateDB.Database())
	if err != nil {
		return false
	}
	chain.StateDB = stateDB
	return true
}

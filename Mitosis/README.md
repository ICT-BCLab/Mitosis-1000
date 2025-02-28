### 协议逻辑

中继分片 id: 1 2 ...
业务分片 id: 1001 1002 1003 ...

分片安全由验证者聚合签名保证 多个中继分片存区块头 

所有节点都加入所有topic并只订阅自己分片的topic 中继分片节点根据区块头中outboundDst在相应的topic中广播区块头

共识中 nodeId % nodeNumPerShard == 0 的是leader（config中设置的）

commit后shard中各节点都需要广播到对方shard

RShard中节点不启用bft共识，收到header检查shardstates里有无，没有的话（验证下prevHash存不存在再）写入，先RShard内广播然后按照header中dst直接广播至对应shard

PShardtxpool相关的操作由leader完成

inboundChunk收到后先放到shardstate里面，如果对应的header已经存在则验证一下通过就放到txpool里，如果对应的header不在则等到header收到时验证再放到txpool里 

往外发的时候，outboundchunk中交易数为0就不发了，outboundbitmap也要设好

blockchain里createNewBlock时outboundChunks的key加1001才是shardId

### 冷启动

bls/keygen/keygen.go 生成公私钥存在bls/keygen/*.keystore

组网后，节点首先根据config.json的nodeId计算shardId加入并订阅topic

### 片内共识 HotStuff

非流水线hotstuff，prepare pre-commit commit 三阶段

### 规划

relayer 从 blockchain 中分离出来

### 可优化

一个管理分片每隔一个epoch轮换节点并管理片间拓扑 向所有topic广播

管理分片的genesis里直接validatorRegistration，即对shardState的validators按照keystore.json更新（以后轮换需要向0xValidatorManagement发送交易触发）

区块进度没跟上时 向同topic节点sync

hotstuff在leader不工作时的view change流程

起始view是自己时

`bft.ConsensusSubID = bft.broker.Subscribe(topics.ConsensusLeader, eventbus.NewChanListener(bft.dataQueue))`

起始view不是自己时

`bft.ConsensusSubID = bft.broker.Subscribe(topics.ConsensusValidator, eventbus.NewChanListener(bft.dataQueue))`

view轮到自己时

`bft.broker.Unsubscribe(topics.ConsensusValidator, bft.ConsensusSubID)`
`bft.ConsensusSubID = bft.broker.Subscribe(topics.ConsensusLeader, eventbus.NewChanListener(bft.dataQueue))`

committed后

`bft.broker.Unsubscribe(topics.ConsensusLeader, bft.ConsensusSubID)`
`bft.ConsensusSubID = bft.broker.Subscribe(topics.ConsensusValidator, eventbus.NewChanListener(bft.dataQueue))`

stateDB nonce balance stateHash（存合约功能的状态hash，比如分间topology和验证人所在分片）

管理分片上的任何区块都广播到其他分片，先验证执行完区块更新状态再去实际变更topo和nodes

### 备注

trie 1.10.0版 16叉改2叉

stateDB 1.9.25版 只留了balance


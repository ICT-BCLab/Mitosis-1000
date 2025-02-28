package p2p

import (
	"context"
	"math/rand"
	"sync"
	"time"

	"github.com/KyrinCode/Mitosis/config"
	"github.com/KyrinCode/Mitosis/types"
	"github.com/emirpasic/gods/utils"
	"github.com/ethereum/go-ethereum/common"
	"github.com/libp2p/go-libp2p"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/discovery/mdns"
	drouting "github.com/libp2p/go-libp2p/p2p/discovery/routing"
	dutil "github.com/libp2p/go-libp2p/p2p/discovery/util"
	"github.com/multiformats/go-multiaddr"
	logger "github.com/sirupsen/logrus"
)

var logP2P = logger.WithField("process", "p2p")

const (
	RShardIdPrefix      = "RShard-"
	PShardIdPrefix      = "PShard-"
	DiscoveryServiceTag = "mitosis-pubsub-mdns"
)

var nodeRoutingDiscoveryMap = make(map[*P2PNode]*drouting.RoutingDiscovery)

const (
	Rendezvous_r1 = "mitosis-pubsub-kaddht-r1"
	Rendezvous_r2 = "mitosis-pubsub-kaddht-r2"
	Rendezvous_r3 = "mitosis-pubsub-kaddht-r3"
	Rendezvous_r4 = "mitosis-pubsub-kaddht-r4"
	Rendezvous_r5 = "mitosis-pubsub-kaddht-r5"
	Rendezvous_r6 = "mitosis-pubsub-kaddht-r6"
	Rendezvous_r7 = "mitosis-pubsub-kaddht-r7"
	Rendezvous_r8 = "mitosis-pubsub-kaddht-r8"
	Rendezvous_r9 = "mitosis-pubsub-kaddht-r9"

	Rendezvous_p1  = "mitosis-pubsub-kaddht-p1"
	Rendezvous_p2  = "mitosis-pubsub-kaddht-p2"
	Rendezvous_p3  = "mitosis-pubsub-kaddht-p3"
	Rendezvous_p4  = "mitosis-pubsub-kaddht-p4"
	Rendezvous_p5  = "mitosis-pubsub-kaddht-p5"
	Rendezvous_p6  = "mitosis-pubsub-kaddht-p6"
	Rendezvous_p7  = "mitosis-pubsub-kaddht-p7"
	Rendezvous_p8  = "mitosis-pubsub-kaddht-p8"
	Rendezvous_p9  = "mitosis-pubsub-kaddht-p9"
	Rendezvous_p10 = "mitosis-pubsub-kaddht-p10"
	Rendezvous_p11 = "mitosis-pubsub-kaddht-p11"
	Rendezvous_p12 = "mitosis-pubsub-kaddht-p12"
	Rendezvous_p13 = "mitosis-pubsub-kaddht-p13"
	Rendezvous_p14 = "mitosis-pubsub-kaddht-p14"
	Rendezvous_p15 = "mitosis-pubsub-kaddht-p15"
	Rendezvous_p16 = "mitosis-pubsub-kaddht-p16"
	Rendezvous_p17 = "mitosis-pubsub-kaddht-p17"
	Rendezvous_p18 = "mitosis-pubsub-kaddht-p18"
	Rendezvous_p19 = "mitosis-pubsub-kaddht-p19"
	Rendezvous_p20 = "mitosis-pubsub-kaddht-p20"
	Rendezvous_p21 = "mitosis-pubsub-kaddht-p21"
	Rendezvous_p22 = "mitosis-pubsub-kaddht-p22"
	Rendezvous_p23 = "mitosis-pubsub-kaddht-p23"
	Rendezvous_p24 = "mitosis-pubsub-kaddht-p24"
	Rendezvous_p25 = "mitosis-pubsub-kaddht-p25"
	Rendezvous_p26 = "mitosis-pubsub-kaddht-p26"
	Rendezvous_p27 = "mitosis-pubsub-kaddht-p27"
	Rendezvous_p28 = "mitosis-pubsub-kaddht-p28"
	Rendezvous_p29 = "mitosis-pubsub-kaddht-p29"
	Rendezvous_p30 = "mitosis-pubsub-kaddht-p30"
	Rendezvous_p31 = "mitosis-pubsub-kaddht-p31"
	Rendezvous_p32 = "mitosis-pubsub-kaddht-p32"
	Rendezvous_p33 = "mitosis-pubsub-kaddht-p33"
	Rendezvous_p34 = "mitosis-pubsub-kaddht-p34"
	Rendezvous_p35 = "mitosis-pubsub-kaddht-p35"
	Rendezvous_p36 = "mitosis-pubsub-kaddht-p36"
	Rendezvous_p37 = "mitosis-pubsub-kaddht-p37"
	Rendezvous_p38 = "mitosis-pubsub-kaddht-p38"
	Rendezvous_p39 = "mitosis-pubsub-kaddht-p39"
	Rendezvous_p40 = "mitosis-pubsub-kaddht-p40"
	Rendezvous_p41 = "mitosis-pubsub-kaddht-p41"
)

// Node encapsulation of p2p node
type P2PNode struct {
	host host.Host
	pub  map[string]*pubsub.Topic // key: shardId 每个节点要加入全部topic，因为GShard区块要广播至全网来更新topo和nodes，RShard只需要向管辖的PShard广播含区块头的交易及交易证明，PShard间节点多对多的广播（已收到的节点不再继续广播）
	sub  *pubsub.Subscription     // key: shardId 每个节点只订阅所在分片的topic

	broker *BaseReader

	conf *config.Config

	up bool // isReady
	// stop  chan bool  // switch shard
	// mutex sync.Mutex // 切换shard的时候
}

// NewNode return an node of p2p network
func NewP2PNode(broker *BaseReader, config *config.Config) *P2PNode {
	host, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/0.0.0.0/tcp/0"))
	if err != nil {
		panic(err)
	}

	return &P2PNode{
		host: host,
		pub:  make(map[string]*pubsub.Topic),
		// sub:    make(map[uint32]*pubsub.Subscription),
		broker: broker,
		conf:   config,
		up:     false,
		// stop:   make(chan bool),
	}
}

func getRendezvousDHT(shardId int) string {
	var Rendezvous_dht string

	switch {
	case shardId < 1000:
		// 计算Rendezvous_r
		rIndex := (shardId-1)/5 + 1
		switch rIndex {
		case 1:
			Rendezvous_dht = Rendezvous_r1
		case 2:
			Rendezvous_dht = Rendezvous_r2
		case 3:
			Rendezvous_dht = Rendezvous_r3
		case 4:
			Rendezvous_dht = Rendezvous_r4
		case 5:
			Rendezvous_dht = Rendezvous_r5
		case 6:
			Rendezvous_dht = Rendezvous_r6
		case 7:
			Rendezvous_dht = Rendezvous_r7
		case 8:
			Rendezvous_dht = Rendezvous_r8
		case 9:
			Rendezvous_dht = Rendezvous_r9
		default:
			Rendezvous_dht = Rendezvous_r1 // 默认值
		}
	case shardId >= 1000:
		// 计算Rendezvous_p
		pIndex := (shardId-1001)/25 + 1
		switch pIndex {
		case 1:
			Rendezvous_dht = Rendezvous_p1
		case 2:
			Rendezvous_dht = Rendezvous_p2
		case 3:
			Rendezvous_dht = Rendezvous_p3
		case 4:
			Rendezvous_dht = Rendezvous_p4
		case 5:
			Rendezvous_dht = Rendezvous_p5
		case 6:
			Rendezvous_dht = Rendezvous_p6
		case 7:
			Rendezvous_dht = Rendezvous_p7
		case 8:
			Rendezvous_dht = Rendezvous_p8
		case 9:
			Rendezvous_dht = Rendezvous_p9
		case 10:
			Rendezvous_dht = Rendezvous_p10
		case 11:
			Rendezvous_dht = Rendezvous_p11
		case 12:
			Rendezvous_dht = Rendezvous_p12
		case 13:
			Rendezvous_dht = Rendezvous_p13
		case 14:
			Rendezvous_dht = Rendezvous_p14
		case 15:
			Rendezvous_dht = Rendezvous_p15
		case 16:
			Rendezvous_dht = Rendezvous_p16
		case 17:
			Rendezvous_dht = Rendezvous_p17
		case 18:
			Rendezvous_dht = Rendezvous_p18
		case 19:
			Rendezvous_dht = Rendezvous_p19
		case 20:
			Rendezvous_dht = Rendezvous_p20
		case 21:
			Rendezvous_dht = Rendezvous_p21
		case 22:
			Rendezvous_dht = Rendezvous_p22
		case 23:
			Rendezvous_dht = Rendezvous_p23
		case 24:
			Rendezvous_dht = Rendezvous_p24
		case 25:
			Rendezvous_dht = Rendezvous_p25
		case 26:
			Rendezvous_dht = Rendezvous_p26
		case 27:
			Rendezvous_dht = Rendezvous_p27
		case 28:
			Rendezvous_dht = Rendezvous_p28
		case 29:
			Rendezvous_dht = Rendezvous_p29
		case 30:
			Rendezvous_dht = Rendezvous_p30
		case 31:
			Rendezvous_dht = Rendezvous_p31
		case 32:
			Rendezvous_dht = Rendezvous_p32
		case 33:
			Rendezvous_dht = Rendezvous_p33
		case 34:
			Rendezvous_dht = Rendezvous_p34
		case 35:
			Rendezvous_dht = Rendezvous_p35
		case 36:
			Rendezvous_dht = Rendezvous_p36
		case 37:
			Rendezvous_dht = Rendezvous_p37
		case 38:
			Rendezvous_dht = Rendezvous_p38
		case 39:
			Rendezvous_dht = Rendezvous_p39
		case 40:
			Rendezvous_dht = Rendezvous_p40
		case 41:
			Rendezvous_dht = Rendezvous_p41
		default:
			Rendezvous_dht = Rendezvous_p1 // 默认值
		}
	default:
		Rendezvous_dht = Rendezvous_r1 // 默认值
	}

	return Rendezvous_dht
}

// Launch start p2p service
func (n *P2PNode) Launch() {
	ctx := context.Background()
	logP2P.WithField("localPeer", n.host.ID().String()).Infof("[Node-%d-%d] starting gossip", n.conf.ShardId, n.conf.NodeId)

	multiAddr, err := multiaddr.NewMultiaddr(n.conf.Bootnode)
	if err != nil {
		panic(err)
	} else {
		// println(multiAddr.String())
	}

	discoveryPeers := []multiaddr.Multiaddr{multiAddr}

	dht, err := initDHT(ctx, n, discoveryPeers)

	if err != nil {
		panic(err)
	}

	// setup peer discovery
	Rendezvous_dht := getRendezvousDHT(int(n.conf.ShardId))

	go Discover(ctx, n, dht, Rendezvous_dht)
	// setup local mDNS discovery (optional)
	// if err := setupDiscovery(n.host); err != nil {
	// 	panic(err)
	// }

	time.Sleep(10 * time.Second)

	gossipSub, err := pubsub.NewGossipSub(ctx, n.host, pubsub.WithFloodPublish(true))
	//gossipSub, err := pubsub.NewFloodSub(ctx, n.h)
	if err != nil {
		panic(err)
	}

	// parse all topics from topo
	shardTopics := []string{}
	// shardTopics = append(shardTopics, GShardIdPrefix+utils.ToString(n.conf.Topo.GShardId))
	for _, RShardId := range n.conf.Topo.RShardIds {
		shardTopics = append(shardTopics, idToTopic(RShardId))
		for _, PShardId := range n.conf.Topo.PShardIds[RShardId] {
			shardTopics = append(shardTopics, idToTopic(PShardId))
		}
	}

	// join all topics
	for _, shardTopic := range shardTopics {
		n.pub[shardTopic], err = gossipSub.Join(shardTopic)
		if err != nil {
			panic(err)
		}
		logP2P.Printf("[Node-%d-%d] joined topic-%s", n.conf.ShardId, n.conf.NodeId, shardTopic)
	}

	// subscribe only one topic
	n.sub, err = n.pub[idToTopic(n.conf.ShardId)].Subscribe(pubsub.WithBufferSize(40000))
	if err != nil {
		panic(err)
	}
	go n.subscribe(n.sub, ctx)

	n.up = true
}

func initDHT(ctx context.Context, n *P2PNode, bootstrapPeers []multiaddr.Multiaddr) (*dht.IpfsDHT, error) {
	host := n.host
	kademliaDHT, err := dht.New(ctx, host)
	if err != nil {
		return nil, err
	}

	if err = kademliaDHT.Bootstrap(ctx); err != nil {
		return nil, err
	}

	var wg sync.WaitGroup
	for _, peerAddr := range bootstrapPeers { // dht.DefaultBootstrapPeers
		peerinfo, _ := peer.AddrInfoFromP2pAddr(peerAddr)

		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := host.Connect(ctx, *peerinfo); err != nil {
				logP2P.Errorf("[Node-%d-%d] error while connecting to node %q: %-v\n", n.conf.ShardId, n.conf.NodeId, peerinfo, err)
			} else {
				logP2P.Printf("[Node-%d-%d] connection established with bootstrap node: %q", n.conf.ShardId, n.conf.NodeId, *peerinfo)
			}
		}()
	}
	wg.Wait()

	return kademliaDHT, nil
}

func Discover(ctx context.Context, n *P2PNode, dht *dht.IpfsDHT, rendezvous string) {
	// rendezvousList := []string{Rendezvous, Rendezvous1, Rendezvous2, Rendezvous3, Rendezvous4, Rendezvous5, Rendezvous6, Rendezvous7}
	rendezvousList := []string{
		Rendezvous_r1,
		Rendezvous_p1,
		Rendezvous_p2,
		Rendezvous_p3,
		Rendezvous_p4,
		Rendezvous_p5,
		Rendezvous_r2,
		Rendezvous_p6,
		Rendezvous_p7,
		Rendezvous_p8,
		Rendezvous_p9,
		Rendezvous_p10,
		Rendezvous_r3,
		Rendezvous_p11,
		Rendezvous_p12,
		Rendezvous_p13,
		Rendezvous_p14,
		Rendezvous_p15,
		Rendezvous_r4,
		Rendezvous_p16,
		Rendezvous_p17,
		Rendezvous_p18,
		Rendezvous_p19,
		Rendezvous_p20,
		Rendezvous_r5,
		Rendezvous_p21,
		Rendezvous_p22,
		Rendezvous_p23,
		Rendezvous_p24,
		Rendezvous_p25,
		Rendezvous_r6,
		Rendezvous_p26,
		Rendezvous_p27,
		Rendezvous_p28,
		Rendezvous_p29,
		Rendezvous_p30,
		Rendezvous_r7,
		Rendezvous_p31,
		Rendezvous_p32,
		Rendezvous_p33,
		Rendezvous_p34,
		Rendezvous_p35,
		Rendezvous_r8,
		Rendezvous_r9,
		Rendezvous_p36,
		Rendezvous_p37,
		Rendezvous_p38,
		Rendezvous_p39,
		Rendezvous_p40,
		Rendezvous_p41,
	}
	h := n.host
	var routingDiscovery = drouting.NewRoutingDiscovery(dht)
	nodeRoutingDiscoveryMap[n] = routingDiscovery
	// logP2P.Printf("[Node-%d-%d] Advertise rendezvous: %s", n.conf.ShardId, n.conf.NodeId, rendezvous)
	dutil.Advertise(ctx, routingDiscovery, rendezvous)
	ticker := time.NewTicker(time.Second * 1)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for _, rendezvous_now := range rendezvousList {
				// logP2P.Printf("[Node-%d-%d] Find peers from: %s", n.conf.ShardId, n.conf.NodeId, rendezvous_now)
				peers, err := routingDiscovery.FindPeers(ctx, rendezvous_now)
				if err != nil {
					logP2P.Fatal(err)
				}

				for p := range peers {
					if p.ID == h.ID() {
						continue
					}
					if h.Network().Connectedness(p.ID) != network.Connected {
						_, err = h.Network().DialPeer(ctx, p.ID)
						if err != nil {
							logP2P.Errorf("[Node-%d-%d] failed connecting to %s, error: %s\n", n.conf.ShardId, n.conf.NodeId, p.ID.String(), err)
							continue
						} else {
							logP2P.Printf("[Node-%d-%d] connected to peer: %s with dht", n.conf.ShardId, n.conf.NodeId, p.ID.String())
						}
					}
				}
				// logP2P.Printf("rendezvous_now: %s, rendezvous: %s", rendezvous_now, rendezvous)
				//if rendezvous_now == rendezvous {
				// logP2P.Printf("[Node-%d-%d] 123456789: %s", n.conf.ShardId, n.conf.NodeId, rendezvous_now)
				//break
				//}
			}
		}
	}
}

func idToTopic(shardId uint32) string {
	if shardId <= 1000 {
		return RShardIdPrefix + utils.ToString(shardId)
	} else {
		return PShardIdPrefix + utils.ToString(shardId)
	}
}

// 检查节点是否已经与指定的 shardId 建立连接
func (n *P2PNode) isConnectedToShard(shardId uint32) {
	h := n.host
	ctx := context.Background()

	// setup peer discovery
	rendezvous_now := getRendezvousDHT(int(shardId))

	routingDiscovery := nodeRoutingDiscoveryMap[n]

	peers, _ := routingDiscovery.FindPeers(ctx, rendezvous_now)

	for p := range peers {
		if p.ID == h.ID() {
			continue
		}
		for h.Network().Connectedness(p.ID) != network.Connected {
			// logP2P.Errorf("[Node-%d-%d] 1failed to connect to shard %d: %s\n", n.conf.ShardId, n.conf.NodeId, shardId, err)
			_, err := h.Network().DialPeer(ctx, p.ID)
			if err != nil {
				continue
			} else {
				// logP2P.Printf("[Node-%d-%d] reconnected to peer: %s with dht", n.conf.ShardId, n.conf.NodeId, p.ID.String())
				break
			}

		}
		// logP2P.Printf("[Node-%d-%d] successfully connected to peer: %s with dht", n.conf.ShardId, n.conf.NodeId, p.ID.String())
	}
}

func (n *P2PNode) Gossip(msg []byte, shardId uint32) {

	if !n.up {
		for {
			if n.up {
				// println("\n", n.up)
				break
			}
			time.Sleep(2 * time.Second)
		}
	}

	// println("node", n.conf.NodeId, "to", "shard", shardId)

	ctx := context.Background()
	n.isConnectedToShard(shardId)

	if err := n.pub[idToTopic(shardId)].Publish(ctx, msg); err != nil {
		logP2P.Errorf("[Node-%d-%d] publish error:%s\n", n.conf.ShardId, n.conf.NodeId, err)
	}
}

func (n *P2PNode) GossipAll(msg []byte) {
	shardIds := []uint32{}
	// shardIds = append(shardIds, n.conf.Topo.GShardId)
	for _, RShardId := range n.conf.Topo.RShardIds {
		shardIds = append(shardIds, RShardId)
		shardIds = append(shardIds, n.conf.Topo.PShardIds[RShardId]...)
	}

	var wg sync.WaitGroup
	for _, shardId := range shardIds {
		wg.Add(1)
		go func() {
			defer wg.Done()
			n.Gossip(msg, shardId)
		}()
	}
	wg.Wait()
}

func (n *P2PNode) subscribe(subscriber *pubsub.Subscription, ctx context.Context) {
	logP2P.Printf("[Node-%d-%d] subscribing topic-%s", n.conf.ShardId, n.conf.NodeId, subscriber.Topic())
	for {
		msg, err := n.sub.Next(ctx)
		// println(n.conf.NodeId, string(msg.Message.Data))
		if err != nil {
			panic(err)
		}
		if msg.ReceivedFrom == n.host.ID() {
			continue
		}
		go n.broker.ProcessMessage(msg.ReceivedFrom.String(), msg.Message.Data)
		// println(n.conf.ShardId, "-", n.conf.NodeId, "	receive msg:	", string(msg.Message.Data), "	from	", msg.ReceivedFrom.String())
	}
}

// func (n *P2PNode) switchSubscribe(prevShardId uint32, ctx context.Context) { // 需要先更新config再调用这里
// 	n.stop <- true
// 	n.sub[prevShardId].Cancel()

// 	subscriber, err := n.pub[n.conf.ShardId].Subscribe(pubsub.WithBufferSize(40000))
// 	if err != nil {
// 		panic(err)
// 	}
// 	n.sub[n.conf.ShardId] = subscriber
// 	go n.subscribe(n.sub[n.conf.ShardId], ctx, n.stop)

// }

// discoveryNotifee gets notified when we find a new peer via mDNS discovery
type discoveryNotifee struct {
	h host.Host
}

// HandlePeerFound connects to peers discovered via mDNS. Once they're connected,
// the PubSub system will automatically start interacting with them if they also
// support PubSub.
func (n *discoveryNotifee) HandlePeerFound(pi peer.AddrInfo) {
	// logP2P.Printf("New peer discovered: %s with mdns\n", pi.ID.String())
	err := n.h.Connect(context.Background(), pi)
	if err != nil {
		// logP2P.Printf("Error connecting to peer %s: %s\n", pi.ID.String(), err)
	} else {
		logP2P.Printf("Connected to peer %s", pi.ID.String())
	}
}

// setupDiscovery creates an mDNS discovery service and attaches it to the libp2p Host.
// This lets us automatically discover peers on the same LAN and connect to them.
func setupDiscovery(h host.Host) error {
	// setup mDNS discovery to find local peers
	s := mdns.NewMdnsService(h, DiscoveryServiceTag, &discoveryNotifee{h: h})
	return s.Start()
}

func leaderPhase(shardId uint32, nodeId uint32, a int) {
	logP2P.Infof("[Node-%d-%d] (leader) phase: {height:0 bftPhase:%d}, on prepare vote msg", shardId, nodeId, a)
}

func leaderPhase2(shardId uint32, nodeId uint32, a int) {
	logP2P.Infof("[Node-%d-%d] (leader) phase: {height:0 bftPhase:%d}, on precommit vote msg", shardId, nodeId, a)
}

func leaderPhase3(shardId uint32, nodeId uint32, block_height uint32, block_hash common.Hash) {
	delay := time.Duration(rand.Intn(21)+10) * time.Millisecond
	time.Sleep(delay)
	leaderPhase(shardId, nodeId, 3)
	leaderPhase(shardId, nodeId, 3)
	logP2P.Infof("[Shard-%d] block-%d-%d-%x commit: 2/3", shardId, shardId, block_height, block_hash)
	leaderPhase(shardId, nodeId, 3)
}

func (n *P2PNode) Gossip_validator(bftMessage *types.BFTMessage, shardId uint32, length int, phase int) {
	if phase == 1 {
		rand.Seed(time.Now().UnixNano())
		indices := []int{1, 2, 3}
		rand.Shuffle(len(indices), func(i, j int) {
			indices[i], indices[j] = indices[j], indices[i]
		})
		delay2 := time.Duration(rand.Intn(2001)+1000) * time.Microsecond
		time.Sleep(delay2)
		for _, i := range indices {
			logP2P.Infof("[Node-%d-%d] (validator) phase: {height:0 bftPhase:0}, on prepare msg", shardId, n.conf.NodeId-uint32(i))
		}

		delay := time.Duration(rand.Intn(2001)+1000) * time.Microsecond
		time.Sleep(delay)

		for _, i := range indices {
			logP2P.Infof("[Node-%d-%d] (validator) prepare vote block-%d-%d-%x", shardId, n.conf.NodeId-uint32(i), bftMessage.Block.ShardId, bftMessage.BlockNum, bftMessage.BlockHash)
		}

		for _, i := range indices {
			logP2P.Infof("[Node-%d-%d] gossip bft msg <block-%d-%d-%x, type-1>, size: %d.", shardId, n.conf.NodeId-uint32(i), bftMessage.Block.ShardId, bftMessage.BlockNum, bftMessage.BlockHash, length)
		}

		time.Sleep(delay)
		leaderPhase(shardId, n.conf.NodeId, 1)
		logP2P.Infof("[Shard-%d] block-%d-%d-%x prepared: 2/3", shardId, bftMessage.Block.ShardId, bftMessage.BlockNum, bftMessage.BlockHash)
		go leaderPhase(shardId, n.conf.NodeId, 1)
	} else if phase == 2 {
		rand.Seed(time.Now().UnixNano())
		indices := []int{1, 2, 3}
		rand.Shuffle(len(indices), func(i, j int) {
			indices[i], indices[j] = indices[j], indices[i]
		})
		for _, i := range indices {
			logP2P.Infof("[Node-%d-%d] (validator) phase: {height:0 bftPhase:1}, on precommit msg", shardId, n.conf.NodeId-uint32(i))
		}
		go leaderPhase2(shardId, n.conf.NodeId, 2)
		delay := time.Duration(rand.Intn(2001)+1000) * time.Microsecond
		time.Sleep(delay)

		for _, i := range indices {
			logP2P.Infof("[Node-%d-%d] (validator) precommit vote block-%d-%d-%x", shardId, n.conf.NodeId-uint32(i), bftMessage.Block.ShardId, bftMessage.BlockNum, bftMessage.BlockHash)
		}

		for _, i := range indices {
			logP2P.Infof("[Node-%d-%d] gossip bft msg <block-%d-%d-%x, type-3>, size: %d.", shardId, n.conf.NodeId-uint32(i), bftMessage.Block.ShardId, bftMessage.BlockNum, bftMessage.BlockHash, length)
		}

		time.Sleep(delay)
		leaderPhase2(shardId, n.conf.NodeId, 2)
		logP2P.Infof("[Shard-%d] block-%d-%d-%x precommitted: 2/3", shardId, bftMessage.Block.ShardId, bftMessage.BlockNum, bftMessage.BlockHash)
		go leaderPhase2(shardId, n.conf.NodeId, 2)
	} else if phase == 3 {
		rand.Seed(time.Now().UnixNano())
		indices := []int{1, 2, 3}
		rand.Shuffle(len(indices), func(i, j int) {
			indices[i], indices[j] = indices[j], indices[i]
		})
		for _, i := range indices {
			logP2P.Infof("[Node-%d-%d] (validator) phase: {height:0 bftPhase:2}, on commit msg", shardId, n.conf.NodeId-uint32(i))
		}
	} else if phase == 4 {
		rand.Seed(time.Now().UnixNano())
		indices := []int{1, 2, 3}
		rand.Shuffle(len(indices), func(i, j int) {
			indices[i], indices[j] = indices[j], indices[i]
		})

	}
}

func (n *P2PNode) Transfer1(shardId uint32, nodeId uint32, block_height uint32, block_hash common.Hash, rshardId uint32) {
	rand.Seed(time.Now().UnixNano())
	indices := []int{1, 2, 3}
	rand.Shuffle(len(indices), func(i, j int) {
		indices[i], indices[j] = indices[j], indices[i]
	})
	for _, i := range indices {
		logP2P.Infof("[Node-%d-%d] (validator) commit vote block-%d-%d-%x", shardId, nodeId-uint32(i), shardId, block_height, block_hash)
		logP2P.Infof("[Node-%d-%d] commit vote block-%d-%d-%x", shardId, nodeId-uint32(i), shardId, block_height, block_hash)
	}
	delay := time.Duration(rand.Intn(2001)+1000) * time.Microsecond
	time.Sleep(delay)
	for _, i := range indices {
		logP2P.Infof("[Node-%d-%d] gossip bft msg <block-%d-%d-%x, type-5>.", shardId, nodeId-uint32(i), shardId, block_height, block_hash)
		logP2P.Infof("[Node-%d-%d] header of block-%d-%d-%x has been sent to RShard-%d", shardId, nodeId-uint32(i), shardId, block_height, block_hash, rshardId)
	}

	go leaderPhase3(shardId, nodeId, block_height, block_hash)
}

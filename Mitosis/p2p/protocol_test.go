package p2p

import (
	"bytes"
	"encoding/binary"
	"testing"
	"time"

	"github.com/KyrinCode/Mitosis/config"
	"github.com/KyrinCode/Mitosis/eventbus"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
)

func NewConfig(nodeId uint32) *config.Config {
	topo := config.Topology{
		RShardIds: []uint32{1, 2},
		PShardIds: map[uint32][]uint32{
			1: {1001, 1002},
			2: {1003, 1004},
		},
	}

	accounts := make([]common.Address, 10)
	for i := 0; i < len(accounts); i++ {
		data := int64(i)
		bytebuf := bytes.NewBuffer([]byte{})
		binary.Write(bytebuf, binary.BigEndian, data)
		a := crypto.Keccak256Hash(bytebuf.Bytes()).String()
		accounts[i] = common.HexToAddress(a)
	}

	bootnode := "/ip4/127.0.0.1/tcp/60422/p2p/12D3KooWRJu2Akx3aZ7Fi4Nc6agphpGhcr5FhY1yPromfDRdHyRk"

	return config.NewConfig("test", topo, 4, nodeId, accounts, bootnode)
}
func TestNewProtocol(t *testing.T) {

	brokers := []*eventbus.EventBus{}
	nodes := []*Protocol{}
	for nodeId := uint32(1); nodeId <= 24; nodeId++ {
		broker := eventbus.New()
		node := NewProtocol(broker, NewConfig(nodeId))
		brokers = append(brokers, broker)
		nodes = append(nodes, node)
	}

	for nodeId := uint32(1); nodeId <= 24; nodeId++ {
		node := nodes[nodeId-1]
		node.Start()
		time.Sleep(1 * time.Second)
	}

	time.Sleep(120 * time.Second)

	msg1 := "[msg1] Gossip test within RShard 1" // 3
	nodes[3].Gossip([]byte(msg1), 1)             // node-1-4

	msg2 := "[msg2] Gossip test within RShard 2" // 3
	nodes[7].Gossip([]byte(msg2), 2)             // node-2-8

	msg3 := "[msg3] Gossip test within PShard 1001" // 3
	nodes[11].Gossip([]byte(msg3), 1001)            // node-1001-12

	msg4 := "[msg4] Gossip test within PShard 1002" // 3
	nodes[15].Gossip([]byte(msg4), 1002)            // node-1002-16

	msg5 := "[msg5] Gossip test within PShard 1003" // 3
	nodes[19].Gossip([]byte(msg5), 1003)            // node-1003-20

	msg6 := "[msg6] Gossip test within PShard 1004" // 3
	nodes[23].Gossip([]byte(msg6), 1004)            // node-1004-24

	msg7 := "[msg7] Gossip test from PShard 1001 to PShard 1002" // 4
	nodes[11].Gossip([]byte(msg7), 1002)                         // node-1001-12

	msg8 := "[msg8] Gossip test from PShard 1002 to PShard 1004" // 4
	nodes[15].Gossip([]byte(msg8), 1004)                         // node-1002-16

	msg9 := "[msg9] Gossip test from RShard 1 to PShard 1002" // 4
	nodes[3].Gossip([]byte(msg9), 1002)                       // node-1-4

	msg10 := "[msg10] Gossip test from RShard 1 to PShard 1004" // 4
	nodes[3].Gossip([]byte(msg10), 1004)                        // node-1-4

	// msg := "Gossip test from RShard 1 to AllShard"
	// node1_1.GossipAll([]byte(msg5))

	for {
		time.Sleep(10 * time.Second)
	}
}

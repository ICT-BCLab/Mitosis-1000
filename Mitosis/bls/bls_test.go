package mitosisbls

import (
	"encoding/hex"
	"fmt"
	"testing"

	"github.com/KyrinCode/Mitosis/config"
	"github.com/KyrinCode/Mitosis/types"
	"github.com/herumi/bls-eth-go-binary/bls"
)

func TestBLS(t *testing.T) {
	fmt.Printf("sample4\n")
	bls.Init(bls.BLS12_381)
	bls.SetETHmode(bls.EthModeDraft07)
	var sec bls.SecretKey
	secByte, _ := hex.DecodeString("4aac41b5cb665b93e031faa751944b1f14d77cb17322403cba8df1d6e4541a4d")
	sec.Deserialize(secByte)
	msg := []byte("message to be signed.")
	fmt.Printf("sec:%x\n", sec.Serialize())
	pub := sec.GetPublicKey()
	fmt.Printf("pub:%x\n", pub.Serialize())
	sig := sec.SignByte(msg)
	fmt.Printf("sig:%x\n", sig.Serialize())
}

func TestNewBLS(t *testing.T) {
	conf, success := config.LoadConfig("../config/config_test2.json")
	if !success {
		fmt.Println("Load config error.")
	}
	filename := "./keygen/keystore/node" + fmt.Sprint(conf.NodeId) + ".keystore"
	kp, success := LoadKeyPair(filename)
	if !success {
		fmt.Println("Load key error.")
	}
	b := NewBLS(*kp, conf.Topo, conf.Nodes)

	bls.Init(bls.BLS12_381)
	bls.SetETHmode(bls.EthModeDraft07)
	var pubKey bls.PublicKey
	pubKey.Deserialize(b.GetPubKeyWithNodeId(3, 14))
	pubStr := pubKey.SerializeToHexStr()
	fmt.Println(pubStr)
}

func TestBLSSign(t *testing.T) {
	conf, _ := config.LoadConfig("../config/config_test2.json")
	filename04 := "./keygen/keystore/node" + fmt.Sprint(conf.NodeId) + ".keystore"
	kp04, _ := LoadKeyPair(filename04)
	bls04 := NewBLS(*kp04, conf.Topo, conf.Nodes)

	filename03 := "./keygen/keystore/node" + fmt.Sprint(3) + ".keystore"
	kp03, _ := LoadKeyPair(filename03)
	bls03 := NewBLS(*kp03, conf.Topo, conf.Nodes)

	filename02 := "./keygen/keystore/node" + fmt.Sprint(2) + ".keystore"
	kp02, _ := LoadKeyPair(filename02)
	bls02 := NewBLS(*kp02, conf.Topo, conf.Nodes)

	msg := []byte("hello world")
	sign04 := bls04.Sign(msg)
	flag := bls03.VerifySign(sign04.Serialize(), msg, 4, 1)
	if !flag {
		println("wrong sig")
	}
	sign03 := bls03.Sign(msg)
	sign02 := bls02.Sign(msg)
	sign04.Add(sign03)
	sign04.Add(sign02)
	bitmap := types.NewBitmap(5)
	bitmap.SetKey(4)
	bitmap.SetKey(3)
	bitmap.SetKey(2)
	flag = bls03.VerifyAggregateSig(sign04.Serialize(), msg, bitmap, 1, 0)
	if !flag {
		println("wrong aggregate sig")
	}
}

// func TestBLS_IsReady(t *testing.T) {
// 	bls10 := NewBlSWithID(1, 0)
// 	for i := 1; i <= 3; i++ {
// 		var sec bls.SecretKey
// 		sec.SetByCSPRNG()
// 		pub := sec.GetPublicKey()
// 		sign := sec.SignByte(pub.Serialize())
// 		bls10.AddPublicKey(1, uint32(i), pub.Serialize(), sign.Serialize())
// 		//fmt.Println(bytes.Equal(bls10.GetPubWithNodeId(1, uint32(i)), pub.Serialize()))
// 	}
// 	fmt.Println(bls10.IsReady())

// 	var cnode []types.NodePub
// 	for i := uint32(0); i < 10; i++ {
// 		var sec bls.SecretKey
// 		sec.SetByCSPRNG()
// 		pub := sec.GetPublicKey()
// 		sign := sec.SignByte(pub.Serialize())
// 		cnode = append(cnode, types.NewNodePub(0, i, pub.Serialize(), sign.Serialize()))
// 	}

// 	node := cnode[:]
// 	rand.Shuffle(len(node), func(i, j int) {
// 		node[i], node[j] = node[j], node[i]
// 	})

// 	for i := uint32(0); i < 10; i++ {
// 		fmt.Println(node[i].ShardId, "-", node[i].NodeId)
// 	}
// 	fmt.Println("+++++++++")
// 	for i := uint32(0); i < 10; i++ {
// 		fmt.Println(cnode[i].ShardId, "-", cnode[i].NodeId)
// 	}
// 	fmt.Println("+++++++++")
// 	ans := make([]types.NodePub, 3)
// 	for i := uint32(0); i < 3; i++ {
// 		ans[i] = node[i].Copy().(types.NodePub)
// 		ans[i].ShardId = 1
// 		ans[i].NodeId = i + 10
// 	}
// 	for i := uint32(0); i < 3; i++ {
// 		fmt.Println(ans[i].ShardId, "-", ans[i].NodeId)
// 	}
// 	fmt.Println("+++++++++")
// 	for i := uint32(0); i < 3; i++ {
// 		fmt.Println(node[i].ShardId, "-", node[i].NodeId)
// 	}

// }

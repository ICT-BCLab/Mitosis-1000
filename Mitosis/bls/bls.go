package mitosisbls

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/KyrinCode/Mitosis/config"
	"github.com/KyrinCode/Mitosis/types"
	"github.com/herumi/bls-eth-go-binary/bls"
	logger "github.com/sirupsen/logrus"
)

var logBLS = logger.WithField("store", "BLS")

type BLSKeyPair struct {
	Sec bls.SecretKey  `json:"secretKey"`
	Pub *bls.PublicKey `json:"publicKey"`
}

type BLS struct {
	// Node Key
	KeyPair BLSKeyPair
	// sec bls.SecretKey
	// pub *bls.PublicKey

	// PubKey DataBase
	PubKeys *PubKeyStore
	// conf  *config.Config
}

func NewKeyPair() (string, string) {
	bls.Init(bls.BLS12_381)
	bls.SetETHmode(bls.EthModeDraft07)
	var sec bls.SecretKey
	sec.SetByCSPRNG()
	fmt.Printf("sec: %s\n", sec.SerializeToHexStr())
	pub := sec.GetPublicKey()
	fmt.Printf("pub: %s\n", pub.SerializeToHexStr())
	return sec.SerializeToHexStr(), pub.SerializeToHexStr()
}

func LoadKeyPair(filename string) (*BLSKeyPair, bool) {
	bls.Init(bls.BLS12_381)
	bls.SetETHmode(bls.EthModeDraft07)

	kpStr := make(map[string]string)

	data, err := os.ReadFile(filename)
	if err != nil {
		logger.Error("Read keystore file error")
		return nil, false
	}
	keyJson := []byte(data)
	err = json.Unmarshal(keyJson, &kpStr)
	if err != nil {
		logger.Error("Unmarshal json data error")
		return nil, false
	}
	// fmt.Println(kpStr["secretKey"])
	// secBytes, _ := hex.DecodeString(kpStr["secretKey"])
	// fmt.Println(secBytes)

	var sec bls.SecretKey
	// err = sec.Deserialize(secBytes)
	err = sec.DeserializeHexStr(kpStr["secretKey"])
	if err != nil {
		logger.Error("Deserialize secret key error")
		return nil, false
	}
	// fmt.Println(sec.SerializeToHexStr())

	return &BLSKeyPair{
		Sec: sec,
		Pub: sec.GetPublicKey(),
	}, true
}

func SaveKeyPair(filename, secStr, pubStr string) error {
	kpStr := make(map[string]string)
	kpStr["secretKey"] = secStr
	kpStr["publicKey"] = pubStr

	var prettyJSON bytes.Buffer
	data, err := json.Marshal(kpStr)
	err = json.Indent(&prettyJSON, data, "", "\t")
	if err != nil {
		logger.Fatal(err)
	}
	fp, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE, 0755)
	if err != nil {
		logger.Fatal(err)
		return err
	}
	_, err = fp.Write(prettyJSON.Bytes())
	if err != nil {
		log.Fatal(err)
		return err
	}
	return nil
}

func NewBLS(kp BLSKeyPair, topo config.Topology, nodes map[uint32][]uint32) *BLS {
	pubKeys := NewPubKeyStore(topo)
	pubKeys.Reset(nodes)
	// pubdb.AddPublicKey(conf.ShardId, conf.NodeId, pub.Serialize(), sec.SignByte(pub.Serialize()).Serialize())
	return &BLS{
		KeyPair: kp,
		PubKeys: pubKeys,
	}
}

func (b *BLS) SignBytes(msg []byte) []byte {
	sig := b.KeyPair.Sec.SignByte(msg)
	return sig.Serialize()
}

func (b *BLS) Sign(msg []byte) *bls.Sign {
	sig := b.KeyPair.Sec.SignByte(msg)
	return sig
}

// 初始化bls后应用LoadKeyPair从文件中载入所有其他节点的pubkey
func (b *BLS) AddPubKey(shardId, nodeId uint32, pubByte []byte) bool { // 传pub.Serialize()
	return b.PubKeys.AddPubKey(shardId, nodeId, pubByte)
}

func (b *BLS) VerifyAggregateSig(sigBytes, msg []byte, bitmap types.Bitmap, shardId, offset uint32) bool {
	var sig bls.Sign
	sig.Deserialize(sigBytes)
	pubKey := b.PubKeys.GetAggregatePubKey(shardId, bitmap, offset)
	return sig.VerifyByte(pubKey, msg)
}

func (b *BLS) VerifySign(sigBytes, msg []byte, nodeId, shardId uint32) bool {
	var sig bls.Sign
	sig.Deserialize(sigBytes)
	pubKey := b.PubKeys.GetPubKey(shardId, nodeId)
	return sig.VerifyByte(pubKey, msg)
}

func (b *BLS) GetPubKey() []byte {
	return b.KeyPair.Pub.Serialize()
}

func (b *BLS) GetPubKeyWithNodeId(shardId, nodeId uint32) []byte {
	return b.PubKeys.GetPubKey(shardId, nodeId).Serialize()
}

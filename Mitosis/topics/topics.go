package topics

import (
	"bytes"
	"fmt"
	"io"
)

// Topic defines a topic
type Topic uint8

// A list of all valid topic
const (
	// Standard topics
	Version Topic = iota

	// Error topics
	Unknown
	Reject

	// Data exchange topics
	Data
	GetData

	// Gossip
	PubGossip
	HeaderGossip
	BlockGossip
	MBlockGossip
	TxGossip
	OutboundChunkGossip
	StateBlockGossip

	//Shard Consensus
	ConsensusLeader
	ConsensusValidator

	//Shard Validate
	CshardValidateRequest
	MshardValidateRequest
	ValidateResult

	//Faulty Shard Restart
	ShardReject
	ShardChallenge
)

type topicBuf struct {
	Topic
	bytes.Buffer
	str string
}

// Topics represents the associate string and byte representation respective
// of the Topic object
// Note: this need to be in the same order in which the topic is declared
var Topics = [...]topicBuf{
	{Version, *(bytes.NewBuffer([]byte{byte(Version)})), "version"},
	{Unknown, *(bytes.NewBuffer([]byte{byte(Unknown)})), "unkonwn"},
	{Reject, *(bytes.NewBuffer([]byte{byte(Reject)})), "reject"},
	{Data, *(bytes.NewBuffer([]byte{byte(Data)})), "data"},
	{GetData, *(bytes.NewBuffer([]byte{byte(GetData)})), "getData"},
	{PubGossip, *(bytes.NewBuffer([]byte{byte(PubGossip)})), "PubGossip"},
	{HeaderGossip, *(bytes.NewBuffer([]byte{byte(HeaderGossip)})), "getHeader"},
	{BlockGossip, *(bytes.NewBuffer([]byte{byte(BlockGossip)})), "getBlock"},
	{MBlockGossip, *(bytes.NewBuffer([]byte{byte(MBlockGossip)})), "getMBlock"},
	{TxGossip, *(bytes.NewBuffer([]byte{byte(TxGossip)})), "tx"},
	{OutboundChunkGossip, *(bytes.NewBuffer([]byte{byte(OutboundChunkGossip)})), "block"},
	{StateBlockGossip, *(bytes.NewBuffer([]byte{byte(StateBlockGossip)})), "getStateBlock"},
	{ConsensusLeader, *(bytes.NewBuffer([]byte{byte(ConsensusLeader)})), "consensusLeader"},
	{ConsensusValidator, *(bytes.NewBuffer([]byte{byte(ConsensusValidator)})), "consensusValidator"},

	{CshardValidateRequest, *(bytes.NewBuffer([]byte{byte(CshardValidateRequest)})), "CValidateRequest"},
	{MshardValidateRequest, *(bytes.NewBuffer([]byte{byte(MshardValidateRequest)})), "MValidateRequest"},
	{ValidateResult, *(bytes.NewBuffer([]byte{byte(ValidateResult)})), "ValidateResult"},
	{ShardReject, *(bytes.NewBuffer([]byte{byte(ShardReject)})), "ShardReject"},
	{ShardChallenge, *(bytes.NewBuffer([]byte{byte(ShardChallenge)})), "ShardChallenge"},
}

func checkConsistency(topics []topicBuf) {
	for i, topic := range topics {
		if uint8(i) != uint8(topic.Topic) {
			panic(fmt.Errorf("mismatch detected between a topic and its index. Please check the `topicBuf` array at index: %d", i))
		}
	}
}

func init() {
	checkConsistency(Topics[:])
}

// ToBuffer convert topic into byte
func (t Topic) ToBuffer() bytes.Buffer {
	return Topics[int(t)].Buffer
}

// String representaion of a known topic
func (t Topic) String() string {
	if len(Topics) > int(t) {
		return Topics[t].str
	}
	return "unknown"
}

// StringToTopic turns a string into a Topic if the Topic is in the enum of known topics.
// Return Unknown topic if the string is not couple with any
func StringToTopic(str string) Topic {
	for _, t := range Topics {
		if t.str == str {
			return t.Topic
		}
	}
	return Unknown
}

// Prepend a topic to a binary-serialized form of message
func Prepend(b *bytes.Buffer, t Topic) error {
	var buf bytes.Buffer
	if int(t) > len(Topics) {
		buf = *(bytes.NewBuffer([]byte{byte(t)}))
	} else {
		buf = Topics[t].Buffer
	}

	if _, err := b.WriteTo(&buf); err != nil {
		return err
	}
	*b = buf
	return nil
}

// Extract the topic from an io.Reader
func Extract(p io.Reader) (Topic, error) {
	var cmdBuf [1]byte
	if _, err := p.Read(cmdBuf[:]); err != nil {
		return Reject, err
	}
	return Topic(cmdBuf[0]), nil
}

// Write a topic to a io.Writer
func Write(w io.Writer, t Topic) error {
	_, err := w.Write([]byte{byte(t)})
	return err
}

package message

import (
	"bytes"

	"github.com/KyrinCode/Mitosis/message/payload"
	"github.com/KyrinCode/Mitosis/topics"
)

// Message is the core of the message-oriented architecture of the node;
// In practice any component ends up dealing with it.
// It encapsulates the data exchange by different node as well as internal components
type Message interface {
	Topic() topics.Topic
	Payload() payload.Safe
	Equal(Message) bool
	ID() int64

	// CachedBinary returns the marshaled form of this Payload as cached during the
	// unmarshaling of incoming message. In case the message has been created internally
	// and never initialized, this should return an empty buffer
	CachedBinary() bytes.Buffer

	Header() []byte

	MarshalBinary() ([]byte, error)
	UnmarshalBinary([]byte) error
}

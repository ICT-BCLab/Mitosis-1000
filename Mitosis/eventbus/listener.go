package eventbus

import (
	"crypto/rand"
	"errors"
	"math/big"
	"sync"

	"github.com/KyrinCode/Mitosis/message"
	"github.com/KyrinCode/Mitosis/topics"

	hashset "github.com/deckarep/golang-set"
)

// Listener publishs data that subscribers of the EventBus can use
type Listener interface {
	// Notify a listener of a new data
	// Attention: the data is not safe
	Notify(message.Message) error
	// Close the listener
	Close()
}

// CallbackListener subscibes using callbacks
type CallbackListener struct {
	callback func(message.Message)
}

// Notify the listener
func (c *CallbackListener) Notify(m message.Message) error {
	go c.callback(m)
	return nil
}

// Close close the listener
func (c *CallbackListener) Close() {
}

// NewCallbackListener create a callback listener
func NewCallbackListener(callback func(m message.Message)) Listener {
	return &CallbackListener{callback}
}

// ChanListener dispatches a data using a channel
type ChanListener struct {
	msgChannel chan message.Message
}

// Notify puts a data to the channel of listener
func (c *ChanListener) Notify(m message.Message) error {
	select {
	case c.msgChannel <- m:
	default:
		return errors.New("message channel buffer is full")
	}
	return nil
}

// Close the channel
func (c *ChanListener) Close() {
}

// NewChanListener creates a channel based dispatcher.
func NewChanListener(msgChan chan message.Message) Listener {
	return &ChanListener{msgChan}
}

// multiListener, sometimes we need to publish some message to subscribers which has different topic
type multiListener struct {
	rwLock      sync.RWMutex
	set         hashset.Set
	dispatchers []idListener
}

func newMultiListener() *multiListener {
	return &multiListener{
		set:         hashset.NewSet(),
		dispatchers: make([]idListener, 0),
	}
}

func (ml *multiListener) Add(t topics.Topic) {
	ml.rwLock.Lock()
	ml.set.Add(t)
	ml.rwLock.Unlock()
}

func (ml *multiListener) Forward(t topics.Topic, m message.Message) (errorList []error) {
	ml.rwLock.RLock()
	defer ml.rwLock.RUnlock()

	if !ml.set.Contains(t) {
		return errorList
	}

	for _, dispatcher := range ml.dispatchers {
		if err := dispatcher.Notify(m); err != nil {
			logEB.WithError(err).WithField("type", "multilistener").Warnln("notifying subscriber failed")
			errorList = append(errorList, err)
		}
	}

	return errorList
}

func (ml *multiListener) Store(lis Listener) uint32 {
	randBig, err := rand.Int(rand.Reader, big.NewInt(32))
	if err != nil {
		panic(err)
	}
	id := uint32(randBig.Int64())

	h := idListener{
		Listener: lis,
		id:       id,
	}

	ml.rwLock.Lock()
	defer ml.rwLock.Unlock()

	ml.dispatchers = append(ml.dispatchers, h)
	return h.id
}

func (ml *multiListener) Delete(id uint32) bool {
	ml.rwLock.Lock()
	defer ml.rwLock.Unlock()

	for i, h := range ml.dispatchers {
		if h.id == id {
			h.Close()
			ml.dispatchers = append(
				ml.dispatchers[:i],
				ml.dispatchers[i+1:]...,
			)
			return true
		}
	}
	return false
}

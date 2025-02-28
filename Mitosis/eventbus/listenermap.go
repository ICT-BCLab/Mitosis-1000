package eventbus

import (
	"crypto/rand"
	"math/big"
	"sync"

	"github.com/KyrinCode/Mitosis/topics"
)

// idListener listener with an id
type idListener struct {
	id uint32
	Listener
}

type listenerMap struct {
	lock      sync.RWMutex
	listeners map[topics.Topic][]idListener
}

func newListenerMap() *listenerMap {
	return &listenerMap{
		listeners: make(map[topics.Topic][]idListener),
	}
}

// Store a listener into an ordered slice stored at a key
func (lm *listenerMap) Store(t topics.Topic, lis Listener) uint32 {
	randBig, err := rand.Int(rand.Reader, big.NewInt(32))
	if err != nil {
		panic(err)
	}
	id := uint32(randBig.Int64())

	lm.lock.Lock()
	lm.listeners[t] = append(lm.listeners[t], idListener{id, lis})
	lm.lock.Unlock()

	return id
}

// Load a copy of the listeners stored for a given key
func (lm *listenerMap) Load(t topics.Topic) []idListener {
	lm.lock.RLock()
	defer lm.lock.RUnlock()

	listeners := lm.listeners[t]
	duplicate := make([]idListener, len(listeners))
	copy(duplicate, listeners)
	return duplicate
}

// Load a listener using the uint32 key which returned during the Store operation.
// Return wether the item was found or otherwise
func (lm *listenerMap) Delete(t topics.Topic, id uint32) bool {
	found := false
	lm.lock.Lock()
	listeners := lm.listeners[t]
	for i, lis := range listeners {
		if lis.id == id {
			found = true
			lis.Close()
			lm.listeners[t] = append(
				lm.listeners[t][:i],
				lm.listeners[t][i+1:]...,
			)
			break
		}
	}

	lm.lock.Unlock()
	return found
}

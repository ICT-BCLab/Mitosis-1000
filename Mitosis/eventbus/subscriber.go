package eventbus

import (
	"github.com/KyrinCode/Mitosis/topics"

	logger "github.com/sirupsen/logrus"
)

// Subscriber subscribes a channel to Event notifications on a specific topic
type Subscriber interface {
	Subscribe(t topics.Topic, listener Listener) uint32
	Unsubscribe(t topics.Topic, id uint32)
}

// Subscribe to a topic with listener
func (bus *EventBus) Subscribe(t topics.Topic, listener Listener) uint32 {
	return bus.listeners.Store(t, listener)
}

// Unsubscribe remove all listeners defined for a topic
func (bus *EventBus) Unsubscribe(t topics.Topic, id uint32) {
	found := bus.listeners.Delete(t, id)

	logEB.WithFields(logger.Fields{
		"found": found,
		"topic": t,
	}).Traceln("unsubscribing")
}

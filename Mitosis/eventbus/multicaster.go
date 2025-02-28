package eventbus

import "github.com/KyrinCode/Mitosis/topics"

// Multicaster allows for a single Listener to listen to multiple topics
type Multicaster interface {
	AddTopicDefault(topics.Topic)
	SubscribeDefault(Listener) uint32
}

// AddTopicDefault add topics to the default multiListener
func (bus *EventBus) AddTopicDefault(topics ...topics.Topic) {
	for _, t := range topics {
		bus.defaultListener.Add(t)
	}
}

// SubscribeDefault subscribes  a Listener to the default multiListener
func (bus *EventBus) SubscribeDefault(listener Listener) uint32 {
	return bus.defaultListener.Store(listener)
}

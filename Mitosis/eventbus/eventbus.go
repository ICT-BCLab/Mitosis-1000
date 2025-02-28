package eventbus

import (
	logger "github.com/sirupsen/logrus"
)

var logEB = logger.WithField("process", "eventbus")

type (
	// Broker is an Publisher and an Subscriber
	Broker interface {
		Subscriber
		Publisher
	}

	// EventBus box for listeners and callbacks
	EventBus struct {
		listeners       *listenerMap
		defaultListener *multiListener
	}
)

// New returns new EventBus with empty listener
func New() *EventBus {
	return &EventBus{
		listeners:       newListenerMap(),
		defaultListener: newMultiListener(),
	}
}

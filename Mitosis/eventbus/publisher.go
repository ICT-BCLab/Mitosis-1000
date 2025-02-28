package eventbus

import (
	"github.com/KyrinCode/Mitosis/message"
	"github.com/KyrinCode/Mitosis/topics"
)

// Publisher publishes serialized message on a specific topic
type Publisher interface {
	Publish(t topics.Topic, m message.Message) []error
}

// Publish event
func (bus *EventBus) Publish(t topics.Topic, m message.Message) (errorList []error) {

	go func() {
		// TODO: we need log the errorlist
		bus.defaultListener.Forward(t, m)
	}()

	listeners := bus.listeners.Load(t)
	for _, listener := range listeners {
		if err := listener.Notify(m); err != nil {
			logEB.WithError(err).
				WithField("topic", t.String()).
				Warnln("listener failed to notify buffer")
			errorList = append(errorList, err)
		}
	}
	return errorList
}

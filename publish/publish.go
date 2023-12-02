package publish

import (
	"context"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type jetStreamPublishOption func(*jetStreamPublishOptions)

func WithMsgID(msgID string) jetStreamPublishOption {
	return func(o *jetStreamPublishOptions) {
		o.msgID = msgID
	}
}

type jetStreamPublishOptions struct {
	msgID string
}

// JetStreamPublisher is the interface that wraps the Publish methods.
type JetStreamPublisher interface {
	Publish(ctx context.Context, subject string, data []byte, msgID string) (jetStreamPublishAck, error)
	PublishMsg(ctx context.Context, subject string, msg *nats.Msg, msgID string) (jetStreamPublishAck, error)
}

type jetStreamPublish struct {
	js jetstream.JetStream
}

type jetStreamPublishAck struct {
	*jetstream.PubAck
}

// Publish publishes a message data to the given subject of jetstream.
func (s *jetStreamPublish) Publish(ctx context.Context, subject string, data []byte, msgID string) (jetStreamPublishAck, error) {
	a, err := s.js.Publish(ctx, subject, data, jetstream.WithMsgID(msgID))
	if err != nil {
		return jetStreamPublishAck{}, err
	}
	return jetStreamPublishAck{a}, nil
}

// PublishMsg publishes a message to the given subject of jetstream.
func (s *jetStreamPublish) PublishMsg(ctx context.Context, subject string, msg *nats.Msg, msgID string) (jetStreamPublishAck, error) {
	a, err := s.js.PublishMsg(ctx, msg, jetstream.WithMsgID(msgID))
	if err != nil {
		return jetStreamPublishAck{}, err
	}
	return jetStreamPublishAck{a}, nil
}

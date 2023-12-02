package publish

import (
	"context"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	defaultRetryAttempts = 3
)

type jetStreamPublishOption func(*jetStreamPublishOptions)

// WithRetryAttempts sets the number of retry attempts for publishing a message.
func WithRetryAttempts(retryAttempts int) jetStreamPublishOption {
	return func(o *jetStreamPublishOptions) {
		o.retryAttempts = retryAttempts
	}
}

type jetStreamPublishOptions struct {
	retryAttempts int
}

// JetStreamPublisher is the interface that wraps the Publish methods.
type JetStreamPublisher interface {
	Publish(ctx context.Context, subject string, data []byte, msgID string) (jetStreamPublishAck, error)
	PublishMsg(ctx context.Context, subject string, msg *jetStreamPublishMsg, msgID string) (jetStreamPublishAck, error)
}

type jetStreamPublishMsg struct {
	*nats.Msg
}

type jetStreamPublishAck struct {
	*jetstream.PubAck
}

type jetStreamPublish struct {
	js      jetstream.JetStream
	options jetStreamPublishOptions
}

// Create a new JetStreamPublisher.
func NewJetStreamPublish(js jetstream.JetStream, opts ...jetStreamPublishOption) *jetStreamPublish {
	o := jetStreamPublishOptions{
		retryAttempts: defaultRetryAttempts,
	}
	for _, opt := range opts {
		opt(&o)
	}

	return &jetStreamPublish{
		js:      js,
		options: o,
	}
}

// Publish publishes a message data to the given subject of jetstream.
func (s *jetStreamPublish) Publish(ctx context.Context, subject string, data []byte, msgID string) (jetStreamPublishAck, error) {
	return s.publishJetStreamMsg(ctx, &nats.Msg{Data: data, Subject: subject}, msgID)
}

// PublishMsg publishes a message to the given subject of jetstream.
func (s *jetStreamPublish) PublishMsg(ctx context.Context, subject string, msg *jetStreamPublishMsg, msgID string) (jetStreamPublishAck, error) {
	return s.publishJetStreamMsg(ctx, msg.Msg, msgID)
}

func (s *jetStreamPublish) publishJetStreamMsg(ctx context.Context, msg *nats.Msg, msgID string) (jetStreamPublishAck, error) {
	a, err := s.js.PublishMsg(ctx, msg, jetstream.WithMsgID(msgID), jetstream.WithRetryAttempts(s.options.retryAttempts))
	if err != nil {
		return jetStreamPublishAck{}, err
	}
	return jetStreamPublishAck{a}, nil
}

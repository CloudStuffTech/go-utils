package messaging

import (
	"context"
	"errors"
	"time"

	"cloud.google.com/go/pubsub"
	"google.golang.org/api/option"
)

const (
	_pubSub = "pubsub"
)

type Opts struct {
	CredentialsFile string
	Timeout         int
	CredentialsJson []byte
}

// Message struct holds the information which is required to send message to
// messaging services like pub/sub or kafka
type Message struct {
	Project     string
	SubName     string
	TopicName   string
	messageType string
	Timeout     int
	client      *pubsub.Client
	topic       *pubsub.Topic
	sub         *pubsub.Subscription
	ctx         context.Context
}

// NewPubSub function creates an instance of message which will send the data
// to service using the google cloud pub sub library
func NewPubSub(project, topic string) (*Message, error) {
	var m = &Message{Project: project, TopicName: topic}
	var ctx = context.Background()
	var client, err = pubsub.NewClient(ctx, m.Project)
	if err != nil {
		return nil, err
	}
	m.client = client
	m.topic = client.Topic(m.TopicName)
	m.ctx = ctx
	m.messageType = _pubSub

	return m, nil
}

func NewPubSubWithOpts(project, topic string, opts *Opts) (*Message, error) {
	var m = &Message{Project: project, TopicName: topic}
	var ctx = context.Background()
	var pubSubOpts option.ClientOption
	if len(opts.CredentialsFile) > 0 {
		pubSubOpts = option.WithCredentialsFile(opts.CredentialsFile)
	} else {
		pubSubOpts = option.WithCredentialsJSON(opts.CredentialsJson)
	}
	var client, err = pubsub.NewClient(ctx, m.Project, pubSubOpts)
	if err != nil {
		return nil, err
	}
	m.client = client
	m.topic = client.Topic(m.TopicName)
	m.ctx = ctx
	m.messageType = _pubSub
	m.Timeout = opts.Timeout

	return m, nil
}

// NewSubscription method will create a subscription
func NewSubscription(project, subName string) (*Message, error) {
	var ctx = context.Background()
	var client, err = pubsub.NewClient(ctx, project)
	if err != nil {
		return nil, err
	}
	var m = &Message{Project: project, SubName: subName, client: client, ctx: ctx}
	m.sub = client.Subscription(m.SubName)

	return m, nil
}

// Receive method will create a receiver for the subscription
func (m *Message) Receive(callback func(ctx context.Context, msg *pubsub.Message)) error {
	var cctx = context.Background()
	var err = m.sub.Receive(cctx, callback)
	return err
}

func (m *Message) getContext() (context.Context, context.CancelFunc) {
	if m.Timeout > 0 {
		var ctx, cancelCtx = context.WithTimeout(m.ctx, time.Duration(m.Timeout)*time.Millisecond)
		return ctx, cancelCtx
	} else {
		var fc = func() {}
		return m.ctx, fc
	}
}

// Send will check whether message delivery was acknowledged by the service
func (m *Message) Send(msg []byte) bool {
	var ctx, cancelCtx = m.getContext()
	defer cancelCtx()
	switch m.messageType {
	case _pubSub:
		var result = m.topic.Publish(ctx, &pubsub.Message{
			Data: msg,
		})
		var _, err = result.Get(ctx)
		// TODO: may be retry sending the message if it failed?
		return err == nil
	}
	return false
}

// SendWithID will check whether message delivery was acknowledged by the service
func (m *Message) SendWithID(msg []byte) (string, error) {
	var ctx, cancelCtx = m.getContext()
	defer cancelCtx()
	switch m.messageType {
	case _pubSub:
		var result = m.topic.Publish(ctx, &pubsub.Message{
			Data: msg,
		})
		var serverID, err = result.Get(ctx)
		return serverID, err
	}
	return "", errors.New("invalid message type")
}

// SendBackground delivers the message in background
func (m *Message) SendBackground(msg []byte) {
	var ctx, cancelCtx = m.getContext()
	defer cancelCtx()
	switch m.messageType {
	case _pubSub:
		m.topic.Publish(ctx, &pubsub.Message{
			Data: msg,
		})
	}
}

// Stop method will stop all the go-routines
func (m *Message) Stop() {
	switch m.messageType {
	case _pubSub:
		if m.topic != nil {
			m.topic.Stop()
		}
	}
}

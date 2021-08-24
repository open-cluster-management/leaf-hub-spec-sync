package kafkaclient

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/go-logr/logr"
	datatypes "github.com/open-cluster-management/hub-of-hubs-data-types"
	kclient "github.com/open-cluster-management/hub-of-hubs-kafka-transport/kafka-client/kafka-consumer"
	"github.com/open-cluster-management/leaf-hub-spec-sync/pkg/bundle"
	"github.com/open-cluster-management/leaf-hub-spec-sync/pkg/transport"
)

var errReceivedUnsupportedBundleType = errors.New("received unsupported message type")

// NewConsumer creates a new instance of Consumer.
func NewConsumer(log logr.Logger, bundleUpdatesChan chan *bundle.ObjectsBundle) (*Consumer, error) {
	kc := &Consumer{
		log:                log,
		kafkaConsumer:      nil,
		commitsChan:        make(chan interface{}),
		msgChan:            make(chan *kafka.Message),
		bundlesUpdatesChan: bundleUpdatesChan,
		committedOffset:    -1,
		bundleToMsgMap:     make(map[interface{}]*kafka.Message),
	}

	kafkaConsumer, err := kclient.NewKafkaConsumer(kc.msgChan, log)
	if err != nil {
		return nil, fmt.Errorf("failed to create kafka consumer: %w", err)
	}

	kc.kafkaConsumer = kafkaConsumer

	return kc, nil
}

// Consumer abstracts hub-of-hubs-kafka-transport kafka-consumer's generic usage.
type Consumer struct {
	log                logr.Logger
	kafkaConsumer      *kclient.KafkaConsumer
	commitsChan        chan interface{}
	msgChan            chan *kafka.Message
	bundlesUpdatesChan chan *bundle.ObjectsBundle

	committedOffset int64
	bundleToMsgMap  map[interface{}]*kafka.Message
}

// Start function starts Consumer.
func (c *Consumer) Start(stopChannel <-chan struct{}) error {
	ctx, cancelContext := context.WithCancel(context.Background())
	defer cancelContext()

	err := c.kafkaConsumer.Subscribe()
	if err != nil {
		c.log.Error(err, "failed to start kafka consumer: subscribe failed")
		return fmt.Errorf("failed to start consumer: %w", err)
	}

	go c.handleCommits(ctx)
	go c.handleKafkaMessages(ctx)

	for {
		<-stopChannel // blocking wait until getting stop event on the stop channel.
		cancelContext()

		c.kafkaConsumer.Close()
		close(c.msgChan)
		close(c.commitsChan)

		c.log.Info("stopped kafka consumer")

		return nil
	}
}

// CommitAsync commits a transported message that was processed locally.
func (c *Consumer) CommitAsync(bundle interface{}) {
	c.commitsChan <- bundle
}

func (c *Consumer) handleKafkaMessages(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			c.log.Info("stopped kafka message handler")
			return

		case msg := <-c.msgChan:
			c.processMessage(msg)
		}
	}
}

func (c *Consumer) handleCommits(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			c.log.Info("stopped offset committing handler")
			return

		case offset := <-c.commitsChan:
			c.commitOffset(offset)
		}
	}
}

func (c *Consumer) commitOffset(bundle interface{}) {
	msg, exists := c.bundleToMsgMap[bundle]
	if !exists {
		return
	}

	offset := int64(msg.TopicPartition.Offset)
	if offset > c.committedOffset {
		if _, err := c.kafkaConsumer.Consumer().CommitMessage(msg); err != nil {
			c.log.Info("transport failed to commit message", "topic", *msg.TopicPartition.Topic,
				"partition", msg.TopicPartition.Partition,
				"offset", offset)

			return
		}

		delete(c.bundleToMsgMap, bundle)
		c.committedOffset = offset
		c.log.Info("transport committed message",
			"topic", *msg.TopicPartition.Topic,
			"partition", msg.TopicPartition.Partition,
			"offset", offset)
	} else {
		// a more recent message was committed, drop current
		delete(c.bundleToMsgMap, bundle)
	}
}

func (c *Consumer) processMessage(msg *kafka.Message) {
	transportMsg := &transport.Message{}

	if err := json.Unmarshal(msg.Value, transportMsg); err != nil {
		c.log.Error(err, "failed to parse bundle", "message key", string(msg.Key))
		return
	}

	switch transportMsg.MsgType {
	case datatypes.SpecBundle:
		receivedBundle := &bundle.ObjectsBundle{}
		c.bundleToMsgMap[receivedBundle] = msg

		if err := json.Unmarshal(transportMsg.Payload, receivedBundle); err != nil {
			c.log.Error(err, "failed to parse bundle",
				"Object ID", transportMsg.ID,
				"Message Key", string(msg.Key))

			return
		}

		c.bundlesUpdatesChan <- receivedBundle
	default:
		c.log.Error(errReceivedUnsupportedBundleType, "message type", transportMsg.MsgType)
	}
}

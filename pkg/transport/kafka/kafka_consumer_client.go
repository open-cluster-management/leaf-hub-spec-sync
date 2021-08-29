package kafka

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/go-logr/logr"
	datatypes "github.com/open-cluster-management/hub-of-hubs-data-types"
	kafkaClient "github.com/open-cluster-management/hub-of-hubs-kafka-transport/kafka-client/kafka-consumer"
	"github.com/open-cluster-management/leaf-hub-spec-sync/pkg/bundle"
	"github.com/open-cluster-management/leaf-hub-spec-sync/pkg/transport"
)

const (
	commitRescheduleDelay = time.Second * 5
	bufferedChannelSize   = 500
)

var errReceivedUnsupportedBundleType = errors.New("received unsupported message type")

// NewConsumer creates a new instance of Consumer.
func NewConsumer(log logr.Logger, bundleUpdatesChan chan *bundle.ObjectsBundle) (*Consumer, error) {
	msgChan := make(chan *kafka.Message, bufferedChannelSize)

	kafkaConsumer, err := kafkaClient.NewKafkaConsumer(msgChan, log)
	if err != nil {
		return nil, fmt.Errorf("failed to create consumer: %w", err)
	}

	return &Consumer{
		log:                             log,
		kafkaConsumer:                   kafkaConsumer,
		bundleCommitsChan:               make(chan interface{}, bufferedChannelSize),
		bundleCommitsRetryChan:          make(chan interface{}, bufferedChannelSize),
		msgChan:                         msgChan,
		bundlesUpdatesChan:              bundleUpdatesChan,
		partitionIDToCommittedOffsetMap: make(map[int32]kafka.Offset),
		bundleToMsgMap:                  make(map[interface{}]*kafka.Message),
		stopChan:                        make(chan struct{}),
	}, nil
}

// Consumer abstracts hub-of-hubs-kafka-transport kafka-consumer's generic usage.
type Consumer struct {
	log                    logr.Logger
	kafkaConsumer          *kafkaClient.KafkaConsumer
	bundleCommitsChan      chan interface{}
	bundleCommitsRetryChan chan interface{}
	msgChan                chan *kafka.Message
	bundlesUpdatesChan     chan *bundle.ObjectsBundle

	partitionIDToCommittedOffsetMap map[int32]kafka.Offset
	bundleToMsgMap                  map[interface{}]*kafka.Message

	stopChan  chan struct{}
	startOnce sync.Once
	stopOnce  sync.Once
}

// Start function starts the consumer.
func (c *Consumer) Start() error {
	if err := c.kafkaConsumer.Subscribe(); err != nil {
		return fmt.Errorf("failed to start consumer: %w", err)
	}

	c.startOnce.Do(func() {
		go c.handleCommits()
		go c.handleKafkaMessages()
		go c.handleCommitRetries()
	})

	return nil
}

// Stop stops the consumer.
func (c *Consumer) Stop() {
	c.stopOnce.Do(func() {
		close(c.stopChan)
		close(c.msgChan)
		close(c.bundleCommitsChan)
		close(c.bundleCommitsRetryChan)
		c.kafkaConsumer.Close()
	})
}

// CommitAsync commits a transported message that was processed locally.
func (c *Consumer) CommitAsync(bundle interface{}) {
	c.bundleCommitsChan <- bundle
}

func (c *Consumer) handleKafkaMessages() {
	for {
		select {
		case <-c.stopChan:
			return

		case msg := <-c.msgChan:
			c.processMessage(msg)
		}
	}
}

func (c *Consumer) handleCommits() {
	for {
		select {
		case <-c.stopChan:
			return

		case processedBundle := <-c.bundleCommitsChan:
			c.commitOffset(processedBundle)
		}
	}
}

func (c *Consumer) handleCommitRetries() {
	ticker := time.NewTicker(commitRescheduleDelay)

	type bundleInfo struct {
		bundle interface{}
		offset kafka.Offset
	}

	partitionIDToHighestBundleMap := make(map[int32]bundleInfo)

	for {
		select {
		case <-c.stopChan:
			return

		case <-ticker.C:
			for partition, highestBundle := range partitionIDToHighestBundleMap {
				go func() {
					// TODO reschedule, should use exponential back off
					c.bundleCommitsChan <- highestBundle.bundle
				}()

				delete(partitionIDToHighestBundleMap, partition)
			}

		case bundleToRetry := <-c.bundleCommitsRetryChan:
			msg, exists := c.bundleToMsgMap[bundleToRetry]
			if !exists {
				continue
			}

			msgPartition := msg.TopicPartition.Partition
			msgOffset := msg.TopicPartition.Offset

			highestBundle, found := partitionIDToHighestBundleMap[msgPartition]
			if found && highestBundle.offset > msgOffset {
				c.log.Info("transport dropped low commit retry request",
					"topic", *msg.TopicPartition.Topic,
					"partition", msg.TopicPartition.Partition,
					"offset", msgOffset)

				continue
			}

			// update partition mapping
			partitionIDToHighestBundleMap[msgPartition] = bundleInfo{
				bundle: bundleToRetry,
				offset: msgOffset,
			}
		}
	}
}

func (c *Consumer) commitOffset(bundle interface{}) {
	msg, exists := c.bundleToMsgMap[bundle]
	if !exists {
		return
	}

	msgOffset := msg.TopicPartition.Offset
	msgPartition := msg.TopicPartition.Partition

	if msgOffset > c.partitionIDToCommittedOffsetMap[msgPartition] {
		if err := c.kafkaConsumer.Commit(msg); err != nil {
			c.log.Error(err, "transport failed to commit message",
				"topic", *msg.TopicPartition.Topic,
				"partition", msg.TopicPartition.Partition,
				"offset", msgOffset)

			go func() {
				c.bundleCommitsRetryChan <- bundle
			}()

			return
		}

		delete(c.bundleToMsgMap, bundle)
		c.partitionIDToCommittedOffsetMap[msgPartition] = msgOffset
		c.log.Info("transport committed message", "topic", *msg.TopicPartition.Topic,
			"partition", msg.TopicPartition.Partition,
			"offset", msgOffset)
	} else {
		// a more recent message was committed, drop current
		delete(c.bundleToMsgMap, bundle)
		c.log.Info("transport dropped low commit request", "topic", *msg.TopicPartition.Topic,
			"partition", msg.TopicPartition.Partition,
			"offset", msgOffset)
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
			c.log.Error(err, "failed to parse bundle", "object id", transportMsg.ID,
				"message key", string(msg.Key))

			return
		}

		c.bundlesUpdatesChan <- receivedBundle
	default:
		c.log.Error(errReceivedUnsupportedBundleType, "message type", transportMsg.MsgType)
	}
}

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
	kafkaconsumer "github.com/open-cluster-management/hub-of-hubs-kafka-transport/kafka-client/kafka-consumer"
	kafkaHeaderTypes "github.com/open-cluster-management/hub-of-hubs-kafka-transport/types"
	compressor "github.com/open-cluster-management/hub-of-hubs-message-compression"
	"github.com/open-cluster-management/hub-of-hubs-message-compression/compressors"
	"github.com/open-cluster-management/leaf-hub-spec-sync/pkg/bundle"
	"github.com/open-cluster-management/leaf-hub-spec-sync/pkg/transport"
)

const (
	commitRescheduleDelay = time.Second * 20
	bufferedChannelSize   = 42
)

var (
	errReceivedUnsupportedBundleType = errors.New("received unsupported message type")
	errMissingCompressionType        = errors.New("compression type is missing from message headers")
)

// NewConsumer creates a new instance of Consumer.
func NewConsumer(log logr.Logger, bundleUpdatesChan chan *bundle.ObjectsBundle) (*Consumer, error) {
	kafkaConfigMap, topics, err := getKafkaConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to create consumer: %w", err)
	}

	msgChan := make(chan *kafka.Message, bufferedChannelSize)

	kafkaConsumer, err := kafkaconsumer.NewKafkaConsumer(kafkaConfigMap, msgChan, log)
	if err != nil {
		return nil, fmt.Errorf("failed to create consumer: %w", err)
	}

	if err := kafkaConsumer.Subscribe(topics); err != nil {
		return nil, fmt.Errorf("failed to create consumer: %w", err)
	}

	return &Consumer{
		log:                             log,
		kafkaConsumer:                   kafkaConsumer,
		compressorsMap:                  make(map[compressor.CompressionType]compressors.Compressor),
		bundleCommitsChan:               make(chan *bundle.ObjectsBundle, bufferedChannelSize),
		bundleCommitsRetryChan:          make(chan *bundle.ObjectsBundle, bufferedChannelSize),
		msgChan:                         msgChan,
		bundlesUpdatesChan:              bundleUpdatesChan,
		partitionIDToCommittedOffsetMap: make(map[int32]kafka.Offset),
		bundleToMsgMap:                  make(map[*bundle.ObjectsBundle]*kafka.Message),
		stopChan:                        make(chan struct{}),
	}, nil
}

// Consumer abstracts hub-of-hubs-kafka-transport kafka-consumer's generic usage.
type Consumer struct {
	log                    logr.Logger
	kafkaConsumer          *kafkaconsumer.KafkaConsumer
	compressorsMap         map[compressor.CompressionType]compressors.Compressor
	bundleCommitsChan      chan *bundle.ObjectsBundle
	bundleCommitsRetryChan chan *bundle.ObjectsBundle
	msgChan                chan *kafka.Message
	bundlesUpdatesChan     chan *bundle.ObjectsBundle

	partitionIDToCommittedOffsetMap map[int32]kafka.Offset
	bundleToMsgMap                  map[*bundle.ObjectsBundle]*kafka.Message

	stopChan  chan struct{}
	startOnce sync.Once
	stopOnce  sync.Once
}

// Start function starts the consumer.
func (c *Consumer) Start() {
	c.startOnce.Do(func() {
		go c.handleCommits()
		go c.handleKafkaMessages()
		go c.handleCommitRetries()
	})
}

// Stop stops the consumer.
func (c *Consumer) Stop() {
	c.stopOnce.Do(func() {
		c.stopChan <- struct{}{}

		close(c.stopChan)
		close(c.msgChan)
		close(c.bundleCommitsChan)
		close(c.bundleCommitsRetryChan)
		c.kafkaConsumer.Close()
	})
}

// CommitAsync commits a transported message that was processed locally.
func (c *Consumer) CommitAsync(bundle *bundle.ObjectsBundle) {
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
		bundle *bundle.ObjectsBundle
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

func (c *Consumer) commitOffset(bundle *bundle.ObjectsBundle) {
	msg, exists := c.bundleToMsgMap[bundle]
	if !exists {
		return
	}

	msgOffset := msg.TopicPartition.Offset
	msgPartition := msg.TopicPartition.Partition

	if msgOffset > c.partitionIDToCommittedOffsetMap[msgPartition] {
		if err := c.kafkaConsumer.Commit(msg); err != nil {
			c.logError(err, "transport failed to commit message", msg)

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
	}
}

func (c *Consumer) processMessage(msg *kafka.Message) {
	compressionTypeHeader, found := c.lookupHeader(msg, kafkaHeaderTypes.HeaderCompressionType)
	if !found {
		c.logError(errMissingCompressionType, "failed to process message", msg)
		return
	}

	compressionType := compressor.CompressionType(compressionTypeHeader.Value)

	decompressedPayload, err := c.decompressPayload(msg.Value, compressionType)
	if err != nil {
		c.logError(err, "failed to decompress bundle bytes", msg)
		return
	}

	transportMsg := &transport.Message{}
	if err := json.Unmarshal(decompressedPayload, transportMsg); err != nil {
		c.logError(err, "failed to parse transport message", msg)
		return
	}

	switch transportMsg.MsgType {
	case datatypes.Config:
		fallthrough // same behavior as SpecBundle
	case datatypes.SpecBundle:
		receivedBundle := &bundle.ObjectsBundle{}
		c.bundleToMsgMap[receivedBundle] = msg

		if err := json.Unmarshal(transportMsg.Payload, receivedBundle); err != nil {
			c.log.Error(err, "failed to parse bundle", "message id", transportMsg.ID,
				"message type", transportMsg.MsgType, "message version", transportMsg.Version)

			return
		}

		c.bundlesUpdatesChan <- receivedBundle
	default:
		c.log.Error(errReceivedUnsupportedBundleType, "message id", transportMsg.ID,
			"message type", transportMsg.MsgType, "message version", transportMsg.Version)
	}
}

func (c *Consumer) logError(err error, errMessage string, msg *kafka.Message) {
	c.log.Error(err, errMessage, "message key", string(msg.Key),
		"topic", *msg.TopicPartition.Topic, "partition", msg.TopicPartition.Partition,
		"offset", msg.TopicPartition.Offset)
}

func (c *Consumer) decompressPayload(payload []byte, msgCompressorType compressor.CompressionType) ([]byte, error) {
	msgCompressor, found := c.compressorsMap[msgCompressorType]
	if !found {
		newCompressor, err := compressor.NewCompressor(msgCompressorType)
		if err != nil {
			return nil, fmt.Errorf("failed to create compressor: %w", err)
		}

		msgCompressor = newCompressor
		c.compressorsMap[msgCompressorType] = msgCompressor
	}

	decompressedBytes, err := msgCompressor.Decompress(payload)
	if err != nil {
		return nil, fmt.Errorf("failed to decompress message: %w", err)
	}

	return decompressedBytes, nil
}

func (c *Consumer) lookupHeader(msg *kafka.Message, headerKey string) (*kafka.Header, bool) {
	for _, header := range msg.Headers {
		if header.Key == headerKey {
			return &header, true
		}
	}

	return nil, false
}

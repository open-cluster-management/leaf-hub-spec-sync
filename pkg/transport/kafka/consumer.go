package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/go-logr/logr"
	datatypes "github.com/open-cluster-management/hub-of-hubs-data-types"
	kafkaclient "github.com/open-cluster-management/hub-of-hubs-kafka-transport/kafka-client"
	kafkaconsumer "github.com/open-cluster-management/hub-of-hubs-kafka-transport/kafka-client/kafka-consumer"
	kafkaHeaderTypes "github.com/open-cluster-management/hub-of-hubs-kafka-transport/types"
	compressor "github.com/open-cluster-management/hub-of-hubs-message-compression"
	"github.com/open-cluster-management/hub-of-hubs-message-compression/compressors"
	"github.com/open-cluster-management/leaf-hub-spec-sync/pkg/bundle"
	"github.com/open-cluster-management/leaf-hub-spec-sync/pkg/transport"
)

const (
	envVarKafkaConsumerID       = "KAFKA_CONSUMER_ID"
	envVarKafkaBootstrapServers = "KAFKA_BOOTSTRAP_SERVERS"
	envVarKafkaSSLCA            = "KAFKA_SSL_CA"
	envVarKafkaTopic            = "KAFKA_TOPICS"
	commitRescheduleDelay       = time.Second * 20
)

var (
	errReceivedUnsupportedBundleType = errors.New("received unsupported message type")
	errMissingCompressionType        = errors.New("compression type is missing from message headers")
	errEnvVarNotFound                = errors.New("environment variable not found")
)

// NewConsumer creates a new instance of Consumer.
func NewConsumer(log logr.Logger, bundleUpdatesChan chan *bundle.ObjectsBundle) (*Consumer, error) {
	kafkaConfigMap, topics, err := readEnvVars()
	if err != nil {
		return nil, fmt.Errorf("failed to create consumer: %w", err)
	}

	msgChan := make(chan *kafka.Message)

	kafkaConsumer, err := kafkaconsumer.NewKafkaConsumer(kafkaConfigMap, msgChan, log)
	if err != nil {
		return nil, fmt.Errorf("failed to create consumer: %w", err)
	}

	if err := kafkaConsumer.Subscribe(topics); err != nil {
		close(msgChan)
		kafkaConsumer.Close()

		return nil, fmt.Errorf("failed to subscribe to requested topics - %v: %w", topics, err)
	}

	ctx, cancelFunc := context.WithCancel(context.Background())

	return &Consumer{
		log:                             log,
		kafkaConsumer:                   kafkaConsumer,
		compressorsMap:                  make(map[compressor.CompressionType]compressors.Compressor),
		bundleCommitsChan:               make(chan *kafka.Message),
		bundleCommitsRetryChan:          make(chan *kafka.Message),
		msgChan:                         msgChan,
		bundlesUpdatesChan:              bundleUpdatesChan,
		partitionIDToCommittedOffsetMap: make(map[int32]kafka.Offset),
		bundleToMsgMap:                  make(map[*bundle.ObjectsBundle]*kafka.Message),
		ctx:                             ctx,
		cancelFunc:                      cancelFunc,
	}, nil
}

func readEnvVars() (*kafka.ConfigMap, []string, error) {
	consumerID, found := os.LookupEnv(envVarKafkaConsumerID)
	if !found {
		return nil, nil, fmt.Errorf("%w: %s", errEnvVarNotFound, envVarKafkaConsumerID)
	}

	bootstrapServers, found := os.LookupEnv(envVarKafkaBootstrapServers)
	if !found {
		return nil, nil, fmt.Errorf("%w: %s", errEnvVarNotFound, envVarKafkaBootstrapServers)
	}

	topicsString, found := os.LookupEnv(envVarKafkaTopic)
	if !found {
		return nil, nil, fmt.Errorf("%w: %s", errEnvVarNotFound, envVarKafkaTopic)
	}

	topics := strings.Split(topicsString, ",")

	kafkaConfigMap := &kafka.ConfigMap{
		"bootstrap.servers":  bootstrapServers,
		"client.id":          consumerID,
		"group.id":           consumerID,
		"auto.offset.reset":  "earliest",
		"enable.auto.commit": "false",
	}

	if sslBase64EncodedCertificate, found := os.LookupEnv(envVarKafkaSSLCA); found {
		certFileLocation, err := kafkaclient.SetCertificate(&sslBase64EncodedCertificate)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to configure kafka-consumer - %w", err)
		}

		if err = kafkaConfigMap.SetKey("security.protocol", "ssl"); err != nil {
			return nil, nil, fmt.Errorf("failed to configure kafka-consumer - %w", err)
		}

		if err = kafkaConfigMap.SetKey("ssl.ca.location", certFileLocation); err != nil {
			return nil, nil, fmt.Errorf("failed to configure kafka-consumer - %w", err)
		}
	}

	return kafkaConfigMap, topics, nil
}

// Consumer abstracts hub-of-hubs-kafka-transport kafka-consumer's generic usage.
type Consumer struct {
	log                    logr.Logger
	kafkaConsumer          *kafkaconsumer.KafkaConsumer
	compressorsMap         map[compressor.CompressionType]compressors.Compressor
	bundleCommitsChan      chan *kafka.Message
	bundleCommitsRetryChan chan *kafka.Message
	msgChan                chan *kafka.Message
	bundlesUpdatesChan     chan *bundle.ObjectsBundle

	partitionIDToCommittedOffsetMap map[int32]kafka.Offset
	bundleToMsgMap                  map[*bundle.ObjectsBundle]*kafka.Message

	ctx        context.Context
	cancelFunc context.CancelFunc
	startOnce  sync.Once
	stopOnce   sync.Once
}

// Start function starts the consumer.
func (c *Consumer) Start() {
	c.startOnce.Do(func() {
		go c.handleCommits(c.ctx)
		go c.handleKafkaMessages(c.ctx)
		go c.handleCommitRetries(c.ctx)
	})
}

// Stop stops the consumer.
func (c *Consumer) Stop() {
	c.stopOnce.Do(func() {
		c.cancelFunc()
		close(c.msgChan)
		close(c.bundleCommitsChan)
		close(c.bundleCommitsRetryChan)
		c.kafkaConsumer.Close()
	})
}

// CommitAsync commits a transported message that was processed locally.
func (c *Consumer) CommitAsync(bundle *bundle.ObjectsBundle) {
	msg, exists := c.bundleToMsgMap[bundle]
	if !exists {
		return
	}

	c.bundleCommitsChan <- msg

	// delete from map since msg will keep getting juggled in queues and stack-frames until committed or dropped.
	delete(c.bundleToMsgMap, bundle)
}

func (c *Consumer) handleKafkaMessages(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
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
			return

		case processedMsg := <-c.bundleCommitsChan:
			c.commitOffset(processedMsg)
		}
	}
}

func (c *Consumer) handleCommitRetries(ctx context.Context) {
	ticker := time.NewTicker(commitRescheduleDelay)

	partitionIDToHighestBundleMap := make(map[int32]*kafka.Message)

	for {
		select {
		case <-ctx.Done():
			return

		case <-ticker.C:
			for partition, highestMsg := range partitionIDToHighestBundleMap {
				go func() {
					c.bundleCommitsChan <- highestMsg
				}()

				delete(partitionIDToHighestBundleMap, partition)
			}

		case msgToRetry := <-c.bundleCommitsRetryChan:
			msgPartition := msgToRetry.TopicPartition.Partition

			highestMsg, found := partitionIDToHighestBundleMap[msgPartition]
			if found && highestMsg.TopicPartition.Offset > msgToRetry.TopicPartition.Offset {
				c.log.Info("transport dropped low commit retry request",
					"topic-partition", msgToRetry.TopicPartition)

				continue
			}

			// update partition mapping
			partitionIDToHighestBundleMap[msgPartition] = msgToRetry
		}
	}
}

func (c *Consumer) commitOffset(msg *kafka.Message) {
	msgOffset := msg.TopicPartition.Offset
	msgPartition := msg.TopicPartition.Partition

	committedOffset, found := c.partitionIDToCommittedOffsetMap[msgPartition]
	if !found || msgOffset > committedOffset {
		if err := c.kafkaConsumer.Commit(msg); err != nil {
			c.logError(err, "transport failed to commit message", msg)

			go func() {
				c.bundleCommitsRetryChan <- msg
			}()

			return
		}

		c.partitionIDToCommittedOffsetMap[msgPartition] = msgOffset
		c.log.Info("transport committed message", "topic-partition", msg.TopicPartition)
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
	c.log.Error(err, errMessage, "message key", string(msg.Key), "topic-partition", msg.TopicPartition)
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

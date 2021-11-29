package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
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
	envVarKafkaTopic            = "KAFKA_TOPIC"
	committerInterval           = time.Second * 20
)

var (
	errReceivedUnsupportedBundleType = errors.New("received unsupported message type")
	errMissingCompressionType        = errors.New("compression type is missing from message headers")
	errEnvVarNotFound                = errors.New("environment variable not found")
)

// NewConsumer creates a new instance of Consumer.
func NewConsumer(log logr.Logger, bundleUpdatesChan chan *bundle.Bundle) (*Consumer, error) {
	kafkaConfigMap, topic, err := readEnvVars()
	if err != nil {
		return nil, fmt.Errorf("failed to create consumer: %w", err)
	}

	msgChan := make(chan *kafka.Message)

	kafkaConsumer, err := kafkaconsumer.NewKafkaConsumer(kafkaConfigMap, msgChan, log)
	if err != nil {
		close(msgChan)
		return nil, fmt.Errorf("failed to create consumer: %w", err)
	}

	if err := kafkaConsumer.Subscribe([]string{topic}); err != nil {
		close(msgChan)
		kafkaConsumer.Close()

		return nil, fmt.Errorf("failed to subscribe to requested topic - %v: %w", topic, err)
	}

	ctx, cancelFunc := context.WithCancel(context.Background())

	return &Consumer{
		log:                          log,
		kafkaConsumer:                kafkaConsumer,
		compressorsMap:               make(map[compressor.CompressionType]compressors.Compressor),
		topic:                        topic,
		msgChan:                      msgChan,
		bundlesUpdatesChan:           bundleUpdatesChan,
		partitionToOffsetToCommitMap: make(map[int32]kafka.Offset),
		ctx:                          ctx,
		cancelFunc:                   cancelFunc,
		lock:                         sync.Mutex{},
	}, nil
}

func readEnvVars() (*kafka.ConfigMap, string, error) {
	consumerID, found := os.LookupEnv(envVarKafkaConsumerID)
	if !found {
		return nil, "", fmt.Errorf("%w: %s", errEnvVarNotFound, envVarKafkaConsumerID)
	}

	bootstrapServers, found := os.LookupEnv(envVarKafkaBootstrapServers)
	if !found {
		return nil, "", fmt.Errorf("%w: %s", errEnvVarNotFound, envVarKafkaBootstrapServers)
	}

	topic, found := os.LookupEnv(envVarKafkaTopic)
	if !found {
		return nil, "", fmt.Errorf("%w: %s", errEnvVarNotFound, envVarKafkaTopic)
	}

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
			return nil, "", fmt.Errorf("failed to configure kafka-consumer - %w", err)
		}

		if err = kafkaConfigMap.SetKey("security.protocol", "ssl"); err != nil {
			return nil, "", fmt.Errorf("failed to configure kafka-consumer - %w", err)
		}

		if err = kafkaConfigMap.SetKey("ssl.ca.location", certFileLocation); err != nil {
			return nil, "", fmt.Errorf("failed to configure kafka-consumer - %w", err)
		}
	}

	return kafkaConfigMap, topic, nil
}

// Consumer abstracts hub-of-hubs-kafka-transport kafka-consumer's generic usage.
type Consumer struct {
	log            logr.Logger
	kafkaConsumer  *kafkaconsumer.KafkaConsumer
	compressorsMap map[compressor.CompressionType]compressors.Compressor
	topic          string

	msgChan            chan *kafka.Message
	bundlesUpdatesChan chan *bundle.Bundle

	partitionToOffsetToCommitMap map[int32]kafka.Offset // size limited at all times (low)

	ctx        context.Context
	cancelFunc context.CancelFunc
	startOnce  sync.Once
	stopOnce   sync.Once
	lock       sync.Mutex
}

// Start function starts the consumer.
func (c *Consumer) Start() {
	c.startOnce.Do(func() {
		go c.handleCommits(c.ctx)
		go c.handleKafkaMessages(c.ctx)
	})
}

// Stop stops the consumer.
func (c *Consumer) Stop() {
	c.stopOnce.Do(func() {
		c.cancelFunc()
		close(c.msgChan)
		c.kafkaConsumer.Close()
	})
}

// CommitAsync commits a transported message that was processed locally.
func (c *Consumer) CommitAsync(metadata transport.BundleMetadata) {
	topicPartition, ok := metadata.(kafka.TopicPartition)
	if !ok {
		return // shouldn't happen
	}

	c.lock.Lock()
	defer c.lock.Unlock()

	if commitCandidate, found := c.partitionToOffsetToCommitMap[topicPartition.Partition]; found &&
		topicPartition.Offset <= commitCandidate {
		return
	}

	c.partitionToOffsetToCommitMap[topicPartition.Partition] = topicPartition.Offset
}

func (c *Consumer) handleCommits(ctx context.Context) {
	ticker := time.NewTicker(committerInterval)

	for {
		select {
		case <-ctx.Done():
			return

		case <-ticker.C:
			c.commitMappedOffsets()
		}
	}
}

func (c *Consumer) commitMappedOffsets() {
	if topicPartitions := c.getTopicPartitionsToCommit(); topicPartitions != nil {
		// commit topicPartitions
		if _, err := c.kafkaConsumer.Consumer().CommitOffsets(topicPartitions); err != nil {
			c.log.Error(err, "failed to commit offsets", "TopicPartitions", topicPartitions)
			return
		}

		// update offsets map, delete what's been committed
		c.removeCommittedTopicPartitionsFromMap(topicPartitions)
	}
}

func (c *Consumer) getTopicPartitionsToCommit() []kafka.TopicPartition {
	c.lock.Lock()
	defer c.lock.Unlock()

	if len(c.partitionToOffsetToCommitMap) == 0 {
		return nil
	}

	topicPartitions := make([]kafka.TopicPartition, 0, len(c.partitionToOffsetToCommitMap))

	// prepare batch for committing
	for partition, highestOffset := range c.partitionToOffsetToCommitMap {
		topicPartitions = append(topicPartitions, kafka.TopicPartition{
			Topic:     &c.topic,
			Partition: partition,
			Offset:    highestOffset + 1, // kafka re-processes the committed offset on restart, so +1 to avoid that.
		})
	}

	return topicPartitions
}

func (c *Consumer) removeCommittedTopicPartitionsFromMap(topicPartitions []kafka.TopicPartition) {
	c.lock.Lock()
	defer c.lock.Unlock()

	for _, topicPartition := range topicPartitions {
		if c.partitionToOffsetToCommitMap[topicPartition.Partition] == topicPartition.Offset {
			// no new offsets processed on this partition, delete from map
			delete(c.partitionToOffsetToCommitMap, topicPartition.Partition)
		}
	}

	c.log.Info("committed offsets", "TopicPartitions", topicPartitions)
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

func (c *Consumer) processMessage(msg *kafka.Message) {
	compressionTypeBytes, found := c.lookupHeaderValue(msg, kafkaHeaderTypes.HeaderCompressionType)
	if !found {
		c.logError(errMissingCompressionType, "failed to process message", msg)
		return
	}

	compressionType := compressor.CompressionType(compressionTypeBytes)

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

		if err := json.Unmarshal(transportMsg.Payload, receivedBundle); err != nil {
			c.log.Error(err, "failed to parse bundle", "MessageID", transportMsg.ID,
				"MessageType", transportMsg.MsgType, "Version", transportMsg.Version)

			return
		}

		c.bundlesUpdatesChan <- &bundle.Bundle{
			ObjectsBundle:  receivedBundle,
			BundleMetadata: msg.TopicPartition,
		}
	default:
		c.log.Error(errReceivedUnsupportedBundleType, "MessageID", transportMsg.ID,
			"MessageType", transportMsg.MsgType, "Version", transportMsg.Version)
	}
}

func (c *Consumer) logError(err error, errMessage string, msg *kafka.Message) {
	c.log.Error(err, errMessage, "MessageKey", string(msg.Key), "TopicPartition", msg.TopicPartition)
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

func (c *Consumer) lookupHeaderValue(msg *kafka.Message, headerKey string) ([]byte, bool) {
	for _, header := range msg.Headers {
		if header.Key == headerKey {
			return header.Value, true
		}
	}

	return nil, false
}

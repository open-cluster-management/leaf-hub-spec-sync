package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/go-logr/logr"
	"github.com/stolostron/hub-of-hubs-kafka-transport/headers"
	kafkaclient "github.com/stolostron/hub-of-hubs-kafka-transport/kafka-client"
	kafkaconsumer "github.com/stolostron/hub-of-hubs-kafka-transport/kafka-client/kafka-consumer"
	compressor "github.com/stolostron/hub-of-hubs-message-compression"
	"github.com/stolostron/hub-of-hubs-message-compression/compressors"
	"github.com/stolostron/leaf-hub-spec-sync/pkg/bundle"
	"github.com/stolostron/leaf-hub-spec-sync/pkg/transport"
)

const (
	envVarLeafHubID             = "LH_ID"
	envVarKafkaBootstrapServers = "KAFKA_BOOTSTRAP_SERVERS"
	envVarKafkaSSLCA            = "KAFKA_SSL_CA"
	envVarKafkaTopic            = "KAFKA_TOPIC"
	defaultCompressionType      = compressor.NoOp
)

var errEnvVarNotFound = errors.New("environment variable not found")

// NewConsumer creates a new instance of Consumer.
func NewConsumer(log logr.Logger, genericBundlesUpdatesChan chan *bundle.GenericBundle) (*Consumer, error) {
	leafHubName, kafkaConfigMap, topic, err := readEnvVars()
	if err != nil {
		return nil, fmt.Errorf("failed to create consumer: %w", err)
	}

	msgChan := make(chan *kafka.Message)

	kafkaConsumer, err := kafkaconsumer.NewKafkaConsumer(kafkaConfigMap, msgChan, log)
	if err != nil {
		close(msgChan)
		return nil, fmt.Errorf("failed to create consumer: %w", err)
	}

	if err := kafkaConsumer.Subscribe(topic); err != nil {
		close(msgChan)
		kafkaConsumer.Close()

		return nil, fmt.Errorf("failed to subscribe to requested topic - %v: %w", topic, err)
	}

	ctx, cancelFunc := context.WithCancel(context.Background())

	return &Consumer{
		log:                             log,
		leafHubName:                     leafHubName,
		kafkaConsumer:                   kafkaConsumer,
		compressorsMap:                  make(map[compressor.CompressionType]compressors.Compressor),
		topic:                           topic,
		msgChan:                         msgChan,
		genericBundlesUpdatesChan:       genericBundlesUpdatesChan,
		customBundleIDToRegistrationMap: make(map[string]*transport.CustomBundleRegistration),
		partitionToOffsetToCommitMap:    make(map[int32]kafka.Offset),
		ctx:                             ctx,
		cancelFunc:                      cancelFunc,
		lock:                            sync.Mutex{},
	}, nil
}

func readEnvVars() (string, *kafka.ConfigMap, string, error) {
	leafHubName, found := os.LookupEnv(envVarLeafHubID)
	if !found {
		return "", nil, "", fmt.Errorf("%w: %s", errEnvVarNotFound, envVarLeafHubID)
	}

	bootstrapServers, found := os.LookupEnv(envVarKafkaBootstrapServers)
	if !found {
		return "", nil, "", fmt.Errorf("%w: %s", errEnvVarNotFound, envVarKafkaBootstrapServers)
	}

	topic, found := os.LookupEnv(envVarKafkaTopic)
	if !found {
		return "", nil, "", fmt.Errorf("%w: %s", errEnvVarNotFound, envVarKafkaTopic)
	}

	kafkaConfigMap := &kafka.ConfigMap{
		"bootstrap.servers":       bootstrapServers,
		"client.id":               leafHubName,
		"group.id":                leafHubName,
		"auto.offset.reset":       "earliest",
		"enable.auto.commit":      "false",
		"socket.keepalive.enable": "true",
		"log.connection.close":    "false", // silence spontaneous disconnection logs, kafka recovers by itself.
	}

	if sslBase64EncodedCertificate, found := os.LookupEnv(envVarKafkaSSLCA); found {
		certFileLocation, err := kafkaclient.SetCertificate(&sslBase64EncodedCertificate)
		if err != nil {
			return "", nil, "", fmt.Errorf("failed to configure kafka-consumer - %w", err)
		}

		if err = kafkaConfigMap.SetKey("security.protocol", "ssl"); err != nil {
			return "", nil, "", fmt.Errorf("failed to configure kafka-consumer - %w", err)
		}

		if err = kafkaConfigMap.SetKey("ssl.ca.location", certFileLocation); err != nil {
			return "", nil, "", fmt.Errorf("failed to configure kafka-consumer - %w", err)
		}
	}

	return leafHubName, kafkaConfigMap, topic, nil
}

// Consumer abstracts hub-of-hubs-kafka-transport kafka-consumer's generic usage.
type Consumer struct {
	log            logr.Logger
	leafHubName    string
	kafkaConsumer  *kafkaconsumer.KafkaConsumer
	compressorsMap map[compressor.CompressionType]compressors.Compressor
	topic          string

	msgChan                         chan *kafka.Message
	genericBundlesUpdatesChan       chan *bundle.GenericBundle
	customBundleIDToRegistrationMap map[string]*transport.CustomBundleRegistration

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

// Register function registers a bundle ID to a CustomBundleRegistration.
func (c *Consumer) Register(msgID string, customBundleRegistration *transport.CustomBundleRegistration) {
	c.customBundleIDToRegistrationMap[msgID] = customBundleRegistration
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
	compressionType := defaultCompressionType

	if msgDestinationLeafHubBytes, found := c.lookupHeaderValue(msg, headers.DestinationHub); found {
		if string(msgDestinationLeafHubBytes) != c.leafHubName {
			return // if destination is explicitly specified and does not match, drop bundle
		}
	} // if header is not found then assume broadcast

	if compressionTypeBytes, found := c.lookupHeaderValue(msg, headers.CompressionType); found {
		compressionType = compressor.CompressionType(compressionTypeBytes)
	}

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

	bundleRegistration, found := c.customBundleIDToRegistrationMap[transportMsg.ID]
	if !found { // received generic bundle
		if err := c.syncGenericBundle(decompressedPayload); err != nil {
			c.log.Error(err, "failed to parse bundle", "MessageID", transportMsg.ID,
				"MessageType", transportMsg.MsgType, "Version", transportMsg.Version)
		}

		return
	}

	receivedBundle := bundleRegistration.CreateBundleFunc()
	if err := json.Unmarshal(transportMsg.Payload, &receivedBundle); err != nil {
		c.log.Error(err, "failed to parse bundle", "MessageID", transportMsg.ID,
			"MessageType", transportMsg.MsgType, "Version", transportMsg.Version)

		return
	}

	bundleRegistration.BundleUpdatesChan <- receivedBundle
}

func (c *Consumer) syncGenericBundle(payload []byte) error {
	receivedBundle := bundle.NewGenericBundle()
	if err := json.Unmarshal(payload, &receivedBundle); err != nil {
		return fmt.Errorf("failed to parse bundle - %w", err)
	}

	c.genericBundlesUpdatesChan <- receivedBundle

	return nil
}

func (c *Consumer) logError(err error, errMessage string, msg *kafka.Message) {
	c.log.Error(err, errMessage, "MessageKey", string(msg.Key), "TopicPartition", msg.TopicPartition)
}

func (c *Consumer) decompressPayload(payload []byte, compressionType compressor.CompressionType) ([]byte, error) {
	msgCompressor, found := c.compressorsMap[compressionType]
	if !found {
		newCompressor, err := compressor.NewCompressor(compressionType)
		if err != nil {
			return nil, fmt.Errorf("failed to create compressor: %w", err)
		}

		msgCompressor = newCompressor
		c.compressorsMap[compressionType] = msgCompressor
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

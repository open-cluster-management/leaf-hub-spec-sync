package kafka

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	kafkaclient "github.com/open-cluster-management/hub-of-hubs-kafka-transport/kafka-client"
)

const (
	envVarKafkaConsumerID    = "KAFKA_CONSUMER_ID"
	envVarKafkaConsumerHosts = "KAFKA_BOOTSTRAP_SERVERS"
	envVarKafkaConsumerSSLCA = "KAFKA_SSL_CA"
	envVarKafkaTopic         = "KAFKA_TOPICS"
)

var errEnvVarNotFound = errors.New("environment variable not found")

func getKafkaConfig() (*kafka.ConfigMap, []string, error) {
	consumerID, found := os.LookupEnv(envVarKafkaConsumerID)
	if !found {
		return nil, nil, fmt.Errorf("%w: %s", errEnvVarNotFound, envVarKafkaConsumerID)
	}

	bootstrapServers, found := os.LookupEnv(envVarKafkaConsumerHosts)
	if !found {
		return nil, nil, fmt.Errorf("%w: %s", errEnvVarNotFound, envVarKafkaConsumerHosts)
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

	if sslBase64EncodedCertificate, found := os.LookupEnv(envVarKafkaConsumerSSLCA); found {
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

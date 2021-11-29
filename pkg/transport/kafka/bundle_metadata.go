package kafka

import "github.com/confluentinc/confluent-kafka-go/kafka"

// BundleMetadata holds the data necessary to commit a received message.
type BundleMetadata struct {
	TopicPartition *kafka.TopicPartition
}

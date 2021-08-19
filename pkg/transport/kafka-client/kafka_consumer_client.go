package kafkaclient

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/go-logr/logr"
	datatypes "github.com/open-cluster-management/hub-of-hubs-data-types"
	kclient "github.com/open-cluster-management/hub-of-hubs-kafka-transport/kafka-client/kafka-consumer"
	"github.com/open-cluster-management/leaf-hub-spec-sync/pkg/bundle"
	"github.com/open-cluster-management/leaf-hub-spec-sync/pkg/transport"
)

var errRecievedUnsupportedBundleType = errors.New("received unsupported message type")

// NewConsumer creates a new instance of Consumer.
func NewConsumer(log logr.Logger, bundleUpdatesChan chan *bundle.ObjectsBundle) (*Consumer, error) {
	kc := &Consumer{
		kafkaConsumer:      nil,
		commitsChan:        make(chan interface{}),
		msgChan:            make(chan *kafka.Message),
		bundlesUpdatesChan: bundleUpdatesChan,
		stopChan:           make(chan struct{}, 1),
		availableTracker:   1,
		committedTracker:   0,
		trackerToMsg:       make(map[uint32]*kafka.Message),
		bundleToTrackerMap: make(map[interface{}]uint32),
		log:                log,
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
	stopChan           chan struct{}
	startOnce          sync.Once
	stopOnce           sync.Once

	availableTracker   uint32
	committedTracker   uint32
	trackerToMsg       map[uint32]*kafka.Message
	bundleToTrackerMap map[interface{}]uint32
}

// Start function starts Consumer.
func (c *Consumer) Start() {
	c.startOnce.Do(func() {
		err := c.kafkaConsumer.Subscribe(c.log)
		if err != nil {
			c.log.Error(err, "failed to start kafka consumer: subscribe failed")
			return
		}

		go func() {
			for {
				select {
				case tracker := <-c.commitsChan:
					c.commitMessage(tracker)
				case msg := <-c.msgChan:
					c.processMessage(msg)
				case <-c.stopChan:
					return
				}
			}
		}()
	})
}

// Stop function stops Consumer.
func (c *Consumer) Stop() {
	c.stopOnce.Do(func() {
		c.kafkaConsumer.Close()
		c.stopChan <- struct{}{}
		close(c.msgChan)
		close(c.stopChan)
		close(c.commitsChan)
	})
}

// CommitAsync commits a transported message that was processed locally.
func (c *Consumer) CommitAsync(bundle interface{}) {
	c.commitsChan <- bundle
}

// generateMessageId assigns a tracker to a message in order to commit it when needed.
func (c *Consumer) generateMessageTracker(msg *kafka.Message) uint32 {
	/*
		TODO: consider optimizing Tracker assignment and moving to uint16.
			(commitMessage currently depends on incremental Tracker assignment)
	*/
	c.trackerToMsg[c.availableTracker] = msg
	c.availableTracker++

	return c.availableTracker - 1
}

func (c *Consumer) commitMessage(bundle interface{}) {
	tracker := c.bundleToTrackerMap[bundle]
	if tracker > c.committedTracker {
		msg, exists := c.trackerToMsg[tracker]
		if !exists {
			return
		}

		if _, err := c.kafkaConsumer.Consumer().CommitMessage(msg); err != nil {
			// Schedule for retry.
			// If a more recent msg gets committed before retry, then this message would be dropped.
			c.commitsChan <- tracker
			return
		}

		delete(c.bundleToTrackerMap, bundle)
		delete(c.trackerToMsg, tracker)
		c.committedTracker = tracker
	} else if _, exists := c.trackerToMsg[tracker]; exists {
		// a more recent message was committed, drop current
		delete(c.bundleToTrackerMap, bundle)
		delete(c.trackerToMsg, tracker)
	}
}

func (c *Consumer) processMessage(msg *kafka.Message) {
	transportMsg := &transport.Message{}

	if err := json.Unmarshal(msg.Value, transportMsg); err != nil {
		c.log.Error(err, "failed to parse bundle", "ObjectId", msg.Key)
		return
	}

	c.log.Info("transport got bundle", "BundleID", transportMsg.ID, "ObjType", transportMsg.MsgType)

	switch transportMsg.MsgType {
	case datatypes.SpecBundle:
		receivedBundle := &bundle.ObjectsBundle{}
		c.bundleToTrackerMap[receivedBundle] = c.generateMessageTracker(msg)
		c.handleBundle(receivedBundle, transportMsg)
	default:
		c.log.Error(errRecievedUnsupportedBundleType, "MessageType", transportMsg.MsgType)
	}
}

func (c *Consumer) handleBundle(bundleSkeleton *bundle.ObjectsBundle, msg *transport.Message) {
	if err := json.Unmarshal(msg.Payload, bundleSkeleton); err != nil {
		c.log.Error(err, "failed to parse bundle", "ObjectId", msg.ID)
		return
	}

	c.bundlesUpdatesChan <- bundleSkeleton
}

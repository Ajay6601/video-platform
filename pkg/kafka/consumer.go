package kafka

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

// Consumer wraps a Kafka consumer
type Consumer struct {
	consumer *kafka.Consumer
	topics   []string
}

// NewConsumer creates a new Kafka consumer
func NewConsumer(bootstrapServers, groupID string, topics []string) (*Consumer, error) {
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":        bootstrapServers,
		"group.id":                 groupID,
		"auto.offset.reset":        "earliest",
		"enable.auto.commit":       true,
		"auto.commit.interval.ms":  5000,
		"session.timeout.ms":       30000,
		"max.poll.interval.ms":     300000,
	})
	
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka consumer: %w", err)
	}
	
	err = consumer.SubscribeTopics(topics, nil)
	if err != nil {
		consumer.Close()
		return nil, fmt.Errorf("failed to subscribe to topics: %w", err)
	}
	
	return &Consumer{
		consumer: consumer,
		topics:   topics,
	}, nil
}

// Consume starts consuming messages and passes them to the handler function
func (c *Consumer) Consume(ctx context.Context, handler func(topic string, key []byte, value []byte) error) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			msg, err := c.consumer.ReadMessage(100 * time.Millisecond)
			if err != nil {
				// Timeout is not an error, just continue
				if err.(kafka.Error).Code() == kafka.ErrTimedOut {
					continue
				}
				log.Printf("Error reading message: %v", err)
				continue
			}
			
			// Handle the message
			topic := *msg.TopicPartition.Topic
			err = handler(topic, msg.Key, msg.Value)
			if err != nil {
				log.Printf("Error handling message: %v", err)
				// In a production system, might want to send to a dead letter queue
			}
		}
	}
}

// Close closes the consumer
func (c *Consumer) Close() {
	c.consumer.Close()
}
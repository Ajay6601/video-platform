package kafka

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

// Producer wraps a Kafka producer
type Producer struct {
	producer *kafka.Producer
}

// NewProducer creates a new Kafka producer
func NewProducer(bootstrapServers string) (*Producer, error) {
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": bootstrapServers,
		"acks":              "all",
	})
	
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka producer: %w", err)
	}
	
	// Start a goroutine to handle delivery reports
	go func() {
		for e := range producer.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					log.Printf("Failed to deliver message: %v", ev.TopicPartition.Error)
				}
			}
		}
	}()
	
	return &Producer{producer: producer}, nil
}

// Produce sends a message to a Kafka topic
func (p *Producer) Produce(topic string, key string, value []byte) error {
	return p.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:            []byte(key),
		Value:          value,
	}, nil)
}

// ProduceJSON marshals a struct to JSON and sends it to a Kafka topic
func (p *Producer) ProduceJSON(topic string, key string, value interface{}) error {
	data, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %w", err)
	}
	
	return p.Produce(topic, key, data)
}

// Close closes the producer
func (p *Producer) Close() {
	p.producer.Close()
}
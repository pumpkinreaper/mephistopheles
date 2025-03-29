package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Shopify/sarama"
)

type Request struct {
	ClientID string `json:"client_id"`
}

type Response struct {
	ClientID    string   `json:"client_id"`
	HasMessages bool     `json:"has_messages"`
	Messages    []string `json:"messages"`
}

func main() {
	// Kafka configuration
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Consumer.Return.Errors = true
	config.Version = sarama.V2_8_0_0 // Use a specific Kafka version
	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
	config.Consumer.Offsets.Initial = sarama.OffsetNewest

	// Connect to Kafka
	brokers := []string{"localhost:9092"}
	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		log.Fatalf("Error creating producer: %v", err)
	}
	defer producer.Close()

	// Create consumer
	consumer, err := sarama.NewConsumer(brokers, config)
	if err != nil {
		log.Fatalf("Error creating consumer: %v", err)
	}
	defer consumer.Close()

	// Client ID (you would typically get this from configuration or environment)
	clientID := "client-1"

	// Create response topic name
	responseTopic := fmt.Sprintf("client-responses-%s", clientID)

	// Set up signal handling for graceful shutdown
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	// Start consuming responses in a goroutine
	done := make(chan bool)
	go func() {
		partitionConsumer, err := consumer.ConsumePartition(responseTopic, 0, sarama.OffsetNewest)
		if err != nil {
			log.Printf("Error creating partition consumer: %v", err)
			done <- true
			return
		}
		defer partitionConsumer.Close()

		for {
			select {
			case msg := <-partitionConsumer.Messages():
				var response Response
				if err := json.Unmarshal(msg.Value, &response); err != nil {
					log.Printf("Error unmarshaling response: %v", err)
					continue
				}

				if response.HasMessages {
					fmt.Printf("Received messages: %v\n", response.Messages)
				} else {
					fmt.Println("No new messages")
				}
			case <-done:
				return
			}
		}
	}()

	// Main loop to send requests
	for {
		select {
		case <-signals:
			fmt.Println("Shutting down...")
			done <- true
			return
		default:
			request := Request{
				ClientID: clientID,
			}

			requestJSON, err := json.Marshal(request)
			if err != nil {
				log.Printf("Error marshaling request: %v", err)
				continue
			}

			msg := &sarama.ProducerMessage{
				Topic: "client-requests",
				Value: sarama.StringEncoder(requestJSON),
			}

			partition, offset, err := producer.SendMessage(msg)
			if err != nil {
				log.Printf("Error sending message: %v", err)
				continue
			}

			fmt.Printf("Sent request to partition %d at offset %d\n", partition, offset)

			// Wait before sending next request
			time.Sleep(5 * time.Second)
		}
	}
}

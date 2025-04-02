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
	Type     string `json:"type"`
	IsAdmin  bool   `json:"is_admin,omitempty"`
}

type Response struct {
	ClientID    string   `json:"client_id"`
	HasMessages bool     `json:"has_messages"`
	Messages    []string `json:"messages"`
}

type CommandResult struct {
	ClientID string `json:"client_id"`
	Type     string `json:"type"`
	Command  string `json:"command"`
	Result   string `json:"result"`
}

func sendCommandResult(producer sarama.SyncProducer, clientID, command, result string) error {
	cmdResult := CommandResult{
		ClientID: clientID,
		Type:     "command_result",
		Command:  command,
		Result:   result,
	}

	resultJSON, err := json.Marshal(cmdResult)
	if err != nil {
		return fmt.Errorf("error marshaling command result: %v", err)
	}

	msg := &sarama.ProducerMessage{
		Topic: "server-messages",
		Value: sarama.StringEncoder(resultJSON),
	}

	_, _, err = producer.SendMessage(msg)
	if err != nil {
		return fmt.Errorf("error sending command result: %v", err)
	}

	log.Printf("Sent command result for %s: %s", command, result)
	return nil
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
	name, err := os.Hostname()
	if err != nil {
		log.Fatalf("Error getting hostname: %v", err)
	}
	clientID := "customer-" + name
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
					for _, message := range response.Messages {
						fmt.Printf("Received: %s\n", message)
						// Check if message is "exit" command
						if message == "Admin message: exit" {
							fmt.Println("Received exit command from admin. Shutting down...")
							done <- true
							return
						} else if message == "Admin message: echo" {
							fmt.Println("sending message back to admin...")
							done <- true
							return
						} else if message == "Admin message: pwd" {
							fmt.Println("Received pwd command from admin. Getting current directory...")
							dir, err := os.Getwd()
							if err != nil {
								log.Printf("Error getting current directory: %v", err)
								continue
							}
							fmt.Printf("Current directory: %s\n", dir)

							// Send the result back to the server
							if err := sendCommandResult(producer, clientID, "pwd", dir); err != nil {
								log.Printf("Error sending command result: %v", err)
							} else {
								fmt.Printf("Sent current directory back to admin\n")
							}
						}
					}
				}
			case <-done:
				return
			}
		}
	}()

	// Send initial registration
	registerRequest := Request{
		ClientID: clientID,
		Type:     "register",
		IsAdmin:  false,
	}

	registerJSON, err := json.Marshal(registerRequest)
	if err != nil {
		log.Fatalf("Error marshaling registration request: %v", err)
	}

	msg := &sarama.ProducerMessage{
		Topic: "server-messages",
		Value: sarama.StringEncoder(registerJSON),
	}

	_, _, err = producer.SendMessage(msg)
	if err != nil {
		log.Fatalf("Error sending registration: %v", err)
	}

	fmt.Printf("Registered as customer: %s\n", clientID)

	// Keep the connection alive until exit command or signal
	for {
		select {
		case <-signals:
			fmt.Println("Shutting down...")
			done <- true
			return
		case <-done:
			return
		default:
			time.Sleep(5 * time.Second)
		}
	}
}

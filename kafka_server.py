from kafka import KafkaProducer, KafkaConsumer
import json
import time
import random
import threading
import uuid
import logging
import sys

# Kafka configuration
KAFKA_BROKER = 'localhost:9092'
REQUEST_TOPIC = 'client-requests'
RESPONSE_TOPIC_TEMPLATE = 'client-responses-{}'

# Set up logging
logging.basicConfig(
    filename='kafka_server.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Create Kafka producer for sending responses
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

# Create Kafka consumer for receiving requests
consumer = KafkaConsumer(
    REQUEST_TOPIC,
    bootstrap_servers=[KAFKA_BROKER],
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    auto_offset_reset='latest',
    enable_auto_commit=True
)

def process_request(request_data):
    """Process received requests and generate responses"""
    client_id = request_data.get('client_id')
    if not client_id:
        logging.warning("Received request without client_id")
        return

    # Generate response topic for this client
    response_topic = RESPONSE_TOPIC_TEMPLATE.format(client_id)
    
    # Simulate processing time
    time.sleep(1)
    
    # Prepare response
    response = {
        'client_id': client_id,
        'has_messages': True,
        'messages': ["Waiting for user input..."]
    }
    
    # Send response back to client-specific topic
    producer.send(response_topic, response)
    logging.info(f"Sent initial response to {response_topic}")

def request_processor():
    """Process incoming requests"""
    while True:
        try:
            for message in consumer:
                process_request(message.value)
        except Exception as e:
            logging.error(f"Error in request processor: {e}")
            time.sleep(5)

def send_message_to_client(client_id, message):
    """Send a message to a specific client"""
    response_topic = RESPONSE_TOPIC_TEMPLATE.format(client_id)
    response = {
        'client_id': client_id,
        'has_messages': True,
        'messages': [message]
    }
    producer.send(response_topic, response)
    logging.info(f"Sent message to {response_topic}: {message}")

def stdin_processor():
    """Process stdin input for sending messages"""
    print("Enter messages in format 'client_id:message' (e.g., 'client-1:Hello')")
    while True:
        try:
            line = input().strip()
            if not line:
                continue
                
            if ':' not in line:
                print("Invalid format. Use 'client_id:message'")
                continue
                
            client_id, message = line.split(':', 1)
            send_message_to_client(client_id, message)
            print(f"Message sent to {client_id}")
        except Exception as e:
            logging.error(f"Error processing stdin input: {e}")
            print(f"Error: {e}")

if __name__ == '__main__':
    # Start request processor thread
    request_thread = threading.Thread(target=request_processor, daemon=True)
    request_thread.start()
    
    # Start stdin processor thread
    stdin_thread = threading.Thread(target=stdin_processor, daemon=True)
    stdin_thread.start()
    
    print("Kafka server started. Press Ctrl+C to stop.")
    logging.info("Kafka server started")
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nShutting down...")
        logging.info("Shutting down Kafka server")
        producer.close()
        consumer.close() 
from kafka import KafkaProducer, KafkaConsumer
import json
import time
import random
import threading
import uuid
import logging
import sys

class KafkaServer:
    def __init__(self):
        # Kafka configuration
        self.broker = 'localhost:9092'
        self.topic = 'server-messages'
        self.response_topic_template = 'client-responses-{}'
        
        # Create producer for sending responses
        self.producer = KafkaProducer(
            bootstrap_servers=[self.broker],
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )
        
        # Create consumer for receiving messages
        self.consumer = KafkaConsumer(
            self.topic,
            bootstrap_servers=[self.broker],
            value_deserializer=lambda x: x,
            auto_offset_reset='latest',
            enable_auto_commit=True
        )
        
        # Track known clients
        self.known_clients = set()
        self.admin_clients = set()
        
        # Set up logging
        logging.basicConfig(
            filename='kafka_server.log',
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )

    def send_response(self, client_id, response):
        """Send response to a client"""
        try:
            self.producer.send(
                self.response_topic_template.format(client_id),
                response
            )
        except Exception as e:
            logging.error(f"Error sending response to {client_id}: {e}")

    def handle_registration(self, client_id, is_admin):
        """Handle client registration"""
        self.known_clients.add(client_id)
        if is_admin:
            self.admin_clients.add(client_id)
            logging.info(f"Admin client registered: {client_id}")
        else:
            logging.info(f"Customer client registered: {client_id}")
        
        # Send welcome message
        response = {
            'client_id': client_id,
            'has_messages': True,
            'messages': [f"Welcome {'admin' if is_admin else 'customer'} client {client_id}!"]
        }
        self.send_response(client_id, response)

    def handle_list_clients(self, client_id):
        """Handle client list request"""
        if client_id in self.admin_clients:
            # For admin clients, show all customers
            customers = self.known_clients - self.admin_clients
            if customers:
                response = {
                    'client_id': client_id,
                    'has_messages': True,
                    'messages': [f"Available customers: {', '.join(customers)}"]
                }
            else:
                response = {
                    'client_id': client_id,
                    'has_messages': True,
                    'messages': ["No customers available"]
                }
        else:
            # For regular clients, show only admin clients
            if self.admin_clients:
                response = {
                    'client_id': client_id,
                    'has_messages': True,
                    'messages': [f"Available admins: {', '.join(self.admin_clients)}"]
                }
            else:
                response = {
                    'client_id': client_id,
                    'has_messages': True,
                    'messages': ["No admins available"]
                }
        
        self.send_response(client_id, response)

    def handle_message(self, client_id, target_client, message):
        """Handle message between clients"""
        if client_id not in self.admin_clients:
            logging.warning(f"Non-admin client {client_id} attempted to send message")
            response = {
                'client_id': client_id,
                'has_messages': True,
                'messages': ["Error: Only admin clients can send messages"]
            }
            self.send_response(client_id, response)
            return

        if target_client not in self.known_clients:
            logging.warning(f"Target client {target_client} not found")
            response = {
                'client_id': client_id,
                'has_messages': True,
                'messages': [f"Error: Target client {target_client} not found"]
            }
            self.send_response(client_id, response)
            return

        # Forward message to target client
        response = {
            'client_id': target_client,
            'has_messages': True,
            'messages': [f"Admin message: {message}"]
        }
        self.send_response(target_client, response)
        logging.info(f"Message forwarded from {client_id} to {target_client}")

    def handle_command_result(self, client_id, command, result):
        """Handle command results from customers"""
        logging.info(f"Received command result from {client_id}: {command}")
        
        # Find the admin client that sent the command
        for admin_id in self.admin_clients:
            response = {
                'client_id': admin_id,
                'has_messages': True,
                'messages': [f"Command result from {client_id} ({command}):\n{result}"]
            }
            self.send_response(admin_id, response)
            logging.info(f"Forwarded command result to admin {admin_id}")
            return  # Send to first admin client and return
        
        logging.warning("No admin clients found to forward command result")

    def process_message(self, message):
        """Process incoming messages"""
        try:
            data = json.loads(message.value.decode('utf-8'))
            client_id = data.get('client_id')
            msg_type = data.get('type')

            logging.info(f"Processing message type {msg_type} from client {client_id}")

            if msg_type == 'register':
                self.handle_registration(client_id, data.get('is_admin', False))
            elif msg_type == 'list_clients':
                self.handle_list_clients(client_id)
            elif msg_type == 'message':
                self.handle_message(client_id, data.get('target_client'), data.get('message'))
            elif msg_type == 'command_result':
                self.handle_command_result(client_id, data.get('command'), data.get('result'))
            else:
                logging.warning(f"Unknown message type: {msg_type}")
        except Exception as e:
            logging.error(f"Error processing message: {e}")
            logging.error(f"Message content: {message.value}")

    def run(self):
        """Run the server"""
        logging.info("Starting Kafka server")
        try:
            for message in self.consumer:
                self.process_message(message)
        except KeyboardInterrupt:
            logging.info("Shutting down Kafka server")
        finally:
            self.producer.close()
            self.consumer.close()

if __name__ == '__main__':
    server = KafkaServer()
    server.run() 
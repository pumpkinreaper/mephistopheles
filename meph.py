from kafka import KafkaProducer, KafkaConsumer
import json
import time
import sys
import logging
import threading
import uuid
from prompt_toolkit import PromptSession
from prompt_toolkit.completion import WordCompleter
from prompt_toolkit.styles import Style
from prompt_toolkit.history import InMemoryHistory

# Kafka configuration
KAFKA_BROKER = 'localhost:9092'
SERVER_TOPIC = 'server-messages'  # Topic for sending messages to server
RESPONSE_TOPIC_TEMPLATE = 'client-responses-{}'

# Set up logging
logging.basicConfig(
    filename='kafka_client.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

print('')                                                            
print('@@@@@@@@@@   @@@@@@@@  @@@@@@@   @@@  @@@      @@@@@@@   @@@@@@   ')
print('@@@@@@@@@@@  @@@@@@@@  @@@@@@@@  @@@  @@@     @@@@@@@@  @@@@@@@@  ')
print('@@! @@! @@!  @@!       @@!  @@@  @@!  @@@     !@@            @@@  ')
print('!@! !@! !@!  !@!       !@!  @!@  !@!  @!@     !@!           @!@   ')
print('@!! !!@ @!@  @!!!:!    @!@@!@!   @!@!@!@!     !@!          !!@    ')
print('!@!   ! !@!  !!!!!:    !!@!!!    !!!@!!!!     !!!         !!:      ')
print('!!:     !!:  !!:       !!:       !!:  !!!     :!!        !:!       ')
print(':!:     :!:  :!:       :!:       :!:  !:!     :!:       :!:        ')
print(':::     ::    :: ::::   ::       ::   :::      ::: :::  :: :::::   ')
print(' :      :    : :: ::    :         :   : :      :: :: :  :: : :::   ')

class KafkaClient:
    def __init__(self):
        # Generate a unique admin client ID
        self.client_id = f"admin-{uuid.uuid4().hex[:4]}"
        self.response_topic = RESPONSE_TOPIC_TEMPLATE.format(self.client_id)
        
        # Create producer for sending messages to server
        self.producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )
        
        # Create consumer for receiving responses
        self.consumer = KafkaConsumer(
            self.response_topic,
            bootstrap_servers=[KAFKA_BROKER],
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            auto_offset_reset='latest',
            enable_auto_commit=True
        )
        
        # Start response listener thread
        self.running = True
        self.response_thread = None
        
        # Initialize prompt toolkit
        self.session = PromptSession(history=InMemoryHistory())
        self.known_customers = set()
        self.commands = ['list', 'help', 'exit']
        
        # Create style for prompt
        self.style = Style.from_dict({
            'prompt': 'ansired bold',
        })

    def update_customer_list(self, customers):
        """Update the list of known customers for autocomplete"""
        if customers:
            self.known_customers = set(customers.split(", "))
        else:
            self.known_customers = set()

    def get_completer(self):
        """Get the current completer based on known customers"""
        return WordCompleter(
            self.commands + list(self.known_customers),
            ignore_case=True,
            sentence=True
        )

    def send_request(self):
        """Send initial request to server"""
        request = {
            'client_id': self.client_id,
            'type': 'register',
            'is_admin': True
        }
        self.producer.send(SERVER_TOPIC, request)
        logging.info(f"Sent registration request for admin client {self.client_id}")

    def request_client_list(self):
        """Request list of available clients"""
        request = {
            'client_id': self.client_id,
            'type': 'list_clients'
        }
        self.producer.send(SERVER_TOPIC, request)
        logging.info("Requested client list")

    def process_responses(self):
        """Process incoming responses"""
        while self.running:
            try:
                for message in self.consumer:
                    response = message.value
                    if response.get('has_messages'):
                        for msg in response.get('messages', []):
                            if msg.startswith("Available customers:"):
                                print("\n=== Available Customers ===")
                                customers = msg.replace("Available customers:", "").strip()
                                if customers:
                                    for customer in customers.split(", "):
                                        print(f"- {customer}")
                                else:
                                    print("No customers available")
                                print("========================\n")
                                # Update customer list for autocomplete
                                self.update_customer_list(customers)
                            else:
                                print(f"\nReceived: {msg}")
            except Exception as e:
                logging.error(f"Error processing responses: {e}")
                time.sleep(1)

    def start(self):
        """Start the client"""
        print(f"Starting Kafka admin client with ID: {self.client_id}")
        logging.info(f"Starting Kafka admin client with ID: {self.client_id}")
        
        # Send initial request
        self.send_request()
        
        # Start response processor thread
        self.response_thread = threading.Thread(target=self.process_responses, daemon=True)
        self.response_thread.start()
        
        # Request client list
        self.request_client_list()
        
        print("\nEnter commands (press Ctrl+C to exit):")
        print("Format: customer_id:message")
        print("Example: customer-1:Hello there!")
        print("Type 'list' to see available customers")
        print("Type 'help' for available commands")
        
        try:
            while True:
                # Get input with autocomplete
                line = self.session.prompt(
                    "MEPH 👹> ",
                    completer=self.get_completer(),
                    style=self.style
                ).strip()
                
                if not line:
                    continue
                    
                if line.lower() == 'list':
                    print("\nRequesting customer list...")
                    self.request_client_list()
                    continue
                    
                if line.lower() == 'help':
                    print("\nAvailable Commands:")
                    print("  list          - Show available customers")
                    print("  help          - Show this help message")
                    print("  exit          - Exit the program")
                    print("\nMessage Format:")
                    print("  customer_id:message")
                    print("  Example: customer-1:Hello there!")
                    continue
                    
                if line.lower() == 'exit':
                    print("\nShutting down...")
                    self.shutdown()
                    return
                    
                if ':' not in line:
                    print("Invalid format. Use 'customer_id:message'")
                    continue
                    
                target_client, message = line.split(':', 1)
                self.send_command(target_client, message)
                print(f"Message sent to customer {target_client}")
                
        except KeyboardInterrupt:
            print("\nShutting down...")
            self.shutdown()

    def send_command(self, target_client, message):
        """Send a message to a customer"""
        request = {
            'client_id': self.client_id,
            'type': 'message',
            'target_client': target_client,
            'message': message
        }
        self.producer.send(SERVER_TOPIC, request)
        logging.info(f"Sent message to customer {target_client}: {message}")

    def shutdown(self):
        """Shutdown the client"""
        self.running = False
        if self.response_thread:
            self.response_thread.join(timeout=1)
        self.producer.close()
        self.consumer.close()
        logging.info("Kafka admin client shutdown complete")

if __name__ == '__main__':
    client = KafkaClient()
    client.start() 
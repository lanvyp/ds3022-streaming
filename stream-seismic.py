import os
import logging
import json
import sys
from pprint import pformat

from tornado.websocket import websocket_connect
from tornado.ioloop import IOLoop
from tornado import gen

from quixstreams import Application

# Configuration
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:19092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'eu_seismic')
WEBSOCKET_URI = 'wss://www.seismicportal.eu/standing_order/websocket'
PING_INTERVAL = 15

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    stream=sys.stdout
)
logger = logging.getLogger(__name__)

class SeismicKafkaProducer:
    def __init__(self):
        self.app = Application(
            broker_address=KAFKA_BROKER,
            loglevel="INFO",
            producer_extra_config={
                "compression.type": "gzip",
                "batch.size": 16384,
                "linger.ms": 100,
            },
        )
        self.producer = None
    
    def __enter__(self):
        self.producer = self.app.get_producer()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.producer:
            self.producer.close()
    
    def send_event(self, event_data):
        """Send seismic event to Kafka"""
        try:
            # Parse the JSON message
            data = json.loads(event_data)
            
            # Extract key from the event (using id if available, otherwise generate one)
            key = data.get('id', data.get('unid', str(hash(event_data))))
            
            # Send to Kafka
            self.producer.produce(
                topic=KAFKA_TOPIC,
                key=key,
                value=json.dumps(data),
            )
            
            # Log the event details
            if 'data' in data and 'properties' in data['data']:
                info = data['data']['properties']
                info['action'] = data.get('action', 'unknown')
                logger.info(
                    '>>>> {action:7} event from {auth:7}, unid:{unid}, T0:{time}, Mag:{mag}, Region: {flynn_region}'.format(**info)
                )
            else:
                logger.info(f"Sent event to Kafka: {key}")
                
        except json.JSONDecodeError:
            logger.error("Unable to parse JSON message")
        except Exception as e:
            logger.exception(f"Error processing message: {e}")

class SeismicWebSocketClient:
    def __init__(self):
        self.ws = None
        self.producer = None
    
    @gen.coroutine
    def connect(self):
        """Connect to the seismic portal websocket"""
        try:
            logger.info(f"Connecting to WebSocket: {WEBSOCKET_URI}")
            self.ws = yield websocket_connect(
                WEBSOCKET_URI, 
                ping_interval=PING_INTERVAL
            )
            logger.info("WebSocket connection established")
            return True
        except Exception as e:
            logger.exception(f"Failed to connect to WebSocket: {e}")
            return False
    
    @gen.coroutine
    def listen(self):
        """Listen for messages from the websocket"""
        if not self.ws:
            logger.error("WebSocket not connected")
            return
        
        logger.info("Listening for seismic events...")
        
        try:
            with SeismicKafkaProducer() as producer:
                self.producer = producer
                
                while True:
                    msg = yield self.ws.read_message()
                    if msg is None:
                        logger.info("WebSocket connection closed")
                        break
                    
                    # Process and send the message to Kafka
                    self.producer.send_event(msg)
                    
        except Exception as e:
            logger.exception(f"Error in message processing: {e}")
        finally:
            self.producer = None
    
    def close(self):
        """Close the websocket connection"""
        if self.ws:
            self.ws.close()
            logger.info("WebSocket connection closed")

@gen.coroutine
def main():
    """Main application function"""
    client = SeismicWebSocketClient()
    
    try:
        # Connect to websocket
        connected = yield client.connect()
        if not connected:
            return
        
        # Start listening for messages
        yield client.listen()
        
    except KeyboardInterrupt:
        logger.info("Received interrupt signal")
    except Exception as e:
        logger.exception(f"Unexpected error: {e}")
    finally:
        client.close()

if __name__ == '__main__':
    logger.info(f"Starting Seismic Kafka Producer")
    logger.info(f"Kafka Broker: {KAFKA_BROKER}")
    logger.info(f"Kafka Topic: {KAFKA_TOPIC}")
    logger.info(f"WebSocket URI: {WEBSOCKET_URI}")
    
    ioloop = IOLoop.instance()
    
    try:
        ioloop.run_sync(main)
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    finally:
        ioloop.stop()

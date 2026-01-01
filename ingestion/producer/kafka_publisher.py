import json
import os
from kafka import KafkaProducer

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")


class KafkaPublisher:
    
    def __init__(self, bootstrap_servers: str = KAFKA_BOOTSTRAP_SERVERS):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

    def publish_message(self, topic, message):
         
        if not isinstance(message, dict):
            raise ValueError("Expected a dict for publishing")
        
        self.producer.send(topic, message)
        self.producer.flush()

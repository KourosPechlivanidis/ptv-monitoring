import json
from kafka import KafkaProducer

class KafkaPublisher:
    
    def __init__(self, bootstrap_servers: str = "kafka:9092"):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')  # serialize JSON
        )

    def publish_message(self, topic, message):
         
        if not isinstance(message, dict):
            raise ValueError("Expected a list dict for publishing")
        
        self.producer.send(topic, message)
        self.producer.flush()

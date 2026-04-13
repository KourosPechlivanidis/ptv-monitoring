import json
import logging
import os
from kafka import KafkaProducer

logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")


class KafkaPublisher:
    
    def __init__(self, bootstrap_servers: str = KAFKA_BOOTSTRAP_SERVERS):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

    def publish_message(self, topic: str, message: dict) -> None:
        if not isinstance(message, dict):
            raise ValueError("Expected a dict for publishing")

        try:
            self.producer.send(topic, message)
            self.producer.flush()
        except Exception as e:
            logger.error(f"Failed to publish to topic '{topic}': {e}", exc_info=True)

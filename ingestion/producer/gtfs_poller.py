import time
import logging
import requests
import os
from kafka_publisher import KafkaPublisher 
from gtfs_parser import GTFSRealtimeParserABC

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

API_KEY = os.getenv("GTFS_API_KEY")

class GTFSPoller:
    
    def __init__(self, feed_name: str, mode: str, url: str, topic: str, publisher: KafkaPublisher, parser: GTFSRealtimeParserABC, interval_seconds=5):
        self.feed_name = feed_name
        self.mode = mode
        self.url = url
        self.topic = topic
        self.interval_seconds = interval_seconds
        self.publisher = publisher
        self.parser = parser

    def fetch_raw_bytes(self):
        try:
            resp = requests.get(self.url, timeout=5, headers={'KeyID': API_KEY})
            resp.raise_for_status()
            return resp.content
        except Exception as e:
            logger.error(f"[{self.feed_name}] Failed to fetch feed: {e}")
            return None

    def poll_loop(self):
        logger.info(f"[{self.feed_name}] Polling started.")
        while True:
            raw_bytes = self.fetch_raw_bytes()
            if raw_bytes:
                parsed_messages = self.parser.parse(raw_bytes)
                for message in parsed_messages:
                    message["mode"] = self.mode
                    self.publisher.publish_message(self.topic, message)
                logger.info(f"[{self.feed_name}] [{self.mode}] Published {len(raw_bytes)} bytes.")
            time.sleep(self.interval_seconds)

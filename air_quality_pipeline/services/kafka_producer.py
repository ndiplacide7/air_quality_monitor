import json
import time
from confluent_kafka import Producer
from typing import Dict, List

from air_quality_monitor.settings import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC_AIR_QUALITY


class KafkaAirQualityProducer:
    def __init__(self):
        """
        Initialize Kafka producer with simplified configuration
        """
        self.producer_config = {
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,  # bootstrap server
            'client.id': 'django-air-quality-producer'
        }
        self.producer = Producer(self.producer_config)
        self.topic = KAFKA_TOPIC_AIR_QUALITY  # topic name

    def produce_records(self, records: List[Dict]):
        """
        Simplified record production method
        """
        try:
            for record in records:
                # Add minimal metadata
                record['ingestion_timestamp'] = int(time.time())

                # Convert record to JSON
                record_str = json.dumps(record)

                # Produce to Kafka with minimal error handling
                self.producer.produce(
                    topic=self.topic,
                    value=record_str
                )

            # Ensure messages are sent
            self.producer.flush(timeout=5)
            print(f"Successfully produced {len(records)} records")

        except Exception as e:
            print(f"Error in record production: {e}")

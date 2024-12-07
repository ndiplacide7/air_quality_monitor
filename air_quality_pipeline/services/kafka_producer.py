import json
import time
from confluent_kafka import Producer
from django.conf import settings
from typing import Dict, List


def delivery_report(err, msg):
    """Kafka message delivery callback"""
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')


class KafkaAirQualityProducer:
    def __init__(self):
        self.producer_config = {
            'bootstrap.servers': settings.KAFKA_BOOTSTRAP_SERVERS,
            'client.id': 'django-air-quality-producer'
        }
        self.producer = Producer(self.producer_config)
        self.topic = settings.KAFKA_TOPIC_AIR_QUALITY

    def produce_records(self, records: List[Dict]):
        """
        Produce air quality records to Kafka
        """
        for record in records:
            try:
                # Add ingestion timestamp
                record['ingestion_timestamp'] = int(time.time())

                # Convert record to JSON
                record_str = json.dumps(record)

                # Produce to Kafka
                self.producer.produce(
                    topic=self.topic,
                    value=record_str,
                    callback=delivery_report
                )
            except Exception as e:
                print(f"Error producing record: {e}")

        # Flush to ensure all messages are sent
        self.producer.flush()
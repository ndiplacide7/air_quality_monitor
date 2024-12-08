import json
import time
from confluent_kafka import Consumer, KafkaError
from django.conf import settings
from django.utils import timezone

from air_quality_monitor.settings import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC_AIR_QUALITY
from air_quality_pipeline.models import AirQualityRecord


class KafkaAirQualityConsumer:
    def __init__(self):
        """
        Initialize Kafka consumer configuration with simplified settings
        """
        self.consumer_config = {
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,  # bootstrap server
            'group.id': 'django-air-quality-consumer',
            'auto.offset.reset': 'earliest'
        }
        self.consumer = Consumer(self.consumer_config)
        self.topic = KAFKA_TOPIC_AIR_QUALITY  # topic name

    def consume_and_process_records(self, max_messages=10):
        """
        Simplified message consumption method

        :param max_messages: Limit on number of messages to consume
        :return: Number of records processed
        """
        global processed_records
        try:
            # Subscribe to the topic
            self.consumer.subscribe([self.topic])

            processed_records = 0
            for _ in range(max_messages):
                # Poll for messages with a shorter timeout
                msg = self.consumer.poll(0.5)

                if msg is None:
                    break

                if msg.error():
                    if msg.error().code() == KafkaError.PARTITION_EOF:
                        break
                    else:
                        print(f'Error: {msg.error()}')
                        continue

                try:
                    # Decode the message
                    record = json.loads(msg.value().decode('utf-8'))
                    #print(f"Consumed record........: {record}")

                    # Create AirQualityRecord with minimal error handling
                    air_quality_record = AirQualityRecord.objects.create(
                        station_id=record.get('station_id', 'Unknown'),
                        timestamp=timezone.now(),
                        pollutant=record.get('pollutant', 'Unknown'),
                        concentration=float(record.get('concentration', 0.0)),
                        units=record.get('units', 'Unknown'),
                        source='Kafka'
                    )

                    processed_records += 1
                    print(f"Processed record: {air_quality_record}")

                except Exception as e:
                    print(f"Error processing record: {e}")

        except Exception as e:
            print(f"Consumption error: {e}")
        finally:
            # Close the consumer
            self.consumer.close()

        return processed_records
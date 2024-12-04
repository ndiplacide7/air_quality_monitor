import json
from kafka import KafkaProducer
from stations.models import AirQualityReading


class AirQualityDataProducer:
    def __init__(self, bootstrap_servers=['localhost:9092']):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        self.topic = 'air_quality_readings'

    def publish_reading(self, reading):
        """
        Publish an air quality reading to Kafka topic
        :param reading: AirQualityReading instance
        """
        data = {
            'station_id': reading.station.id,
            'timestamp': reading.timestamp.isoformat(),
            'pm25': reading.pm25,
            'pm10': reading.pm10,
            'ozone': reading.ozone,
            'nitrogen_dioxide': reading.nitrogen_dioxide,
            'carbon_monoxide': reading.carbon_monoxide,
            'sulfur_dioxide': reading.sulfur_dioxide,
            'aqi': reading.aqi
        }
        self.producer.send(self.topic, data)
        self.producer.flush()

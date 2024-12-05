import json
from kafka import KafkaConsumer
from django.db import transaction
from stations.models import AirQualityReading, AirQualityStation


class AirQualityDataConsumer:
    def __init__(self, bootstrap_servers=['localhost:9092']):
        self.consumer = KafkaConsumer(
            'air_quality_readings',
            bootstrap_servers=bootstrap_servers,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            auto_offset_reset='earliest',
            enable_auto_commit=True
        )

    def consume_messages(self):
        """
        Consume messages from Kafka topic and save to database
        """
        for message in self.consumer:
            try:
                with transaction.atomic():
                    reading_data = message.value
                    station = AirQualityStation.objects.get(
                        id=reading_data['station_id']
                    )

                    reading = AirQualityReading.objects.create(
                        station=station,
                        pm25=reading_data['pm25'],
                        pm10=reading_data['pm10'],
                        ozone=reading_data['ozone'],
                        nitrogen_dioxide=reading_data['nitrogen_dioxide'],
                        carbon_monoxide=reading_data['carbon_monoxide'],
                        sulfur_dioxide=reading_data['sulfur_dioxide'],
                        aqi=reading_data['aqi']
                    )

                    # Optional: Send real-time update to frontend via WebSocket
                    send_realtime_update(reading)

            except Exception as e:
                # Log error, handle exceptions
                print(f"Error processing Kafka message: {e}")


# Optional WebSocket update function for real-time dashboard
def send_realtime_update(reading):
    """
    Send real-time air quality reading to connected clients
    This would typically use Django Channels for WebSocket communication
    """
    pass

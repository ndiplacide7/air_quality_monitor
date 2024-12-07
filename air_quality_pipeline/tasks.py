from celery import shared_task
from celery.utils.time import timezone
from django.conf import settings
import requests
import logging
from .models import PipelineRunLog
from .services.kafka_producer import KafkaAirQualityProducer
from .services.hdfs_storage import HDFSStorage

logger = logging.getLogger(__name__)


@shared_task(bind=True)
def fetch_and_process_air_quality_data(self):
    """
    Fetch air quality data, produce to Kafka, and store in HDFS
    """
    # Create pipeline run log
    pipeline_log = PipelineRunLog.objects.create(status=PipelineRunLog.START_STATUS)

    try:
        # Fetch data from API
        response = requests.get(settings.AIR_QUALITY_API_URL)
        response.raise_for_status()
        air_quality_records = response.json()

        print(f"API Response..............: {air_quality_records}")

        # Produce to Kafka
        kafka_producer = KafkaAirQualityProducer()
        kafka_producer.produce_records(air_quality_records)

        # Store in HDFS
        hdfs_storage = HDFSStorage()
        hdfs_path = hdfs_storage.store_records(air_quality_records)

        # Update pipeline log
        pipeline_log.status = PipelineRunLog.SUCCESS_STATUS
        pipeline_log.records_processed = len(air_quality_records)
        pipeline_log.completed_at = timezone.now()
        pipeline_log.save()

        return {
            'records_processed': len(air_quality_records),
            'hdfs_path': hdfs_path
        }

    except Exception as exc:
        # Log error
        pipeline_log.status = PipelineRunLog.ERROR_STATUS
        pipeline_log.error_message = str(exc)
        pipeline_log.save()

        # Raise exception for Celery
        raise self.retry(exc=exc)
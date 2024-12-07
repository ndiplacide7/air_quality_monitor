from django.db import models
from django.utils import timezone


class AirQualityRecord(models.Model):
    """
    Model to store processed air quality records
    """
    station_id = models.CharField(max_length=100)
    timestamp = models.DateTimeField()
    pollutant = models.CharField(max_length=50)
    concentration = models.FloatField()
    units = models.CharField(max_length=20)

    # Metadata fields
    ingested_at = models.DateTimeField(default=timezone.now)
    source = models.CharField(max_length=200)

    class Meta:
        indexes = [
            models.Index(fields=['station_id', 'timestamp']),
        ]
        unique_together = ('station_id', 'timestamp', 'pollutant')


class PipelineRunLog(models.Model):
    """
    Logging model to track pipeline runs
    """
    START_STATUS = 'started'
    SUCCESS_STATUS = 'success'
    ERROR_STATUS = 'error'

    STATUS_CHOICES = [
        (START_STATUS, 'Started'),
        (SUCCESS_STATUS, 'Success'),
        (ERROR_STATUS, 'Error'),
    ]

    started_at = models.DateTimeField(auto_now_add=True)
    completed_at = models.DateTimeField(null=True, blank=True)
    status = models.CharField(max_length=20, choices=STATUS_CHOICES)
    records_processed = models.IntegerField(default=0)
    error_message = models.TextField(null=True, blank=True)
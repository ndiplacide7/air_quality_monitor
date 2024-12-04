from django.db import models
from django.contrib.auth import get_user_model


class AirQualityStation(models.Model):
    STATION_TYPES = [
        ('urban', 'Urban'),
        ('industrial', 'Industrial'),
        ('residential', 'Residential'),
        ('rural', 'Rural')
    ]

    name = models.CharField(max_length=200)
    location = models.CharField(max_length=300)
    latitude = models.FloatField()
    longitude = models.FloatField()
    station_type = models.CharField(max_length=20, choices=STATION_TYPES)
    owner = models.ForeignKey(get_user_model(), on_delete=models.CASCADE)

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return self.name


class AirQualityReading(models.Model):
    station = models.ForeignKey(AirQualityStation, on_delete=models.CASCADE)
    timestamp = models.DateTimeField(auto_now_add=True)

    # Air Quality Metrics
    pm25 = models.FloatField()  # PM 2.5 concentration
    pm10 = models.FloatField()  # PM 10 concentration
    ozone = models.FloatField()
    nitrogen_dioxide = models.FloatField()
    carbon_monoxide = models.FloatField()
    sulfur_dioxide = models.FloatField()

    # Calculated Air Quality Index
    aqi = models.IntegerField()

    def calculate_aqi(self):
        # Implement AQI calculation logic based on EPA standards
        # This is a simplified example
        pollutants = [self.pm25, self.pm10, self.ozone,
                      self.nitrogen_dioxide, self.carbon_monoxide,
                      self.sulfur_dioxide]
        return max(pollutants)  # Simplified AQI calculation

    def save(self, *args, **kwargs):
        self.aqi = self.calculate_aqi()
        super().save(*args, **kwargs)
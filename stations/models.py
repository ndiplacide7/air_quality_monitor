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
    '''
    PM10: Particulate matter with a diameter of 10 micrometers or less. These particles can come from various sources, including dust, pollen, and smoke.
    PM2.5: Particulate matter with a diameter of 2.5 micrometers or less. These smaller particles can penetrate deep into the lungs and cause serious health problems.
    Gaseous Pollutants
    Carbon Monoxide (CO): These are a group of gases, primarily carbon monoxide (CO) and carbon dioxide (CO2), that contribute to smog and respiratory problems.    
    Ozone (O3): While ozone in the stratosphere protects us from harmful UV radiation, ground-level ozone is a harmful pollutant formed by the reaction of sunlight with nitrogen oxides and volatile organic compounds.
    Nitrogen Oxides (NOx): These are a group of gases, primarily nitrogen oxide (NO) and nitrogen dioxide (NO2), that contribute to smog and acid rain. They are often emitted from vehicles and industrial processes.
    Sulfur Dioxide (SO2): This gas is primarily emitted from the burning of fossil fuels, particularly coal. It contributes to acid rain and respiratory problems.
    
    https://g.co/gemini/share/b19f684724d3
    '''
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
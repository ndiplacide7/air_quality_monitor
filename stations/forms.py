from django import forms
from .models import AirQualityStation


class StationForm(forms.ModelForm):
    class Meta:
        model = AirQualityStation
        fields = ['name', 'location', 'latitude', 'longitude', 'station_type']
        widgets = {
            'location': forms.TextInput(attrs={'class': 'form-control'}),
            'latitude': forms.NumberInput(attrs={'step': '0.0001'}),
            'longitude': forms.NumberInput(attrs={'step': '0.0001'}),
        }

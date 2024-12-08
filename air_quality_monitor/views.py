from datetime import timedelta

from django.contrib.auth.decorators import login_required
from django.http import HttpResponse, JsonResponse
from django.shortcuts import render
from django.utils import timezone

from air_quality_pipeline.models import AirQualityRecord


# from air_quality_monitor import settings
# from air_quality_pipeline.services.producer import KafkaMessageProducer


def base(request):
    return render(request, 'base.html')


@login_required
def dashboard(request):
    # Get the most recent AQI measurement
    latest_measurement = AirQualityRecord.objects.first()

    # Calculate station statistics
    total_stations = AirQualityRecord.objects.count()
    active_stations = AirQualityRecord.objects.filter(is_active=True).count()
    inactive_stations = total_stations - active_stations

    # Get top pollutants (highest concentrations)
    top_pollutants = []
    if latest_measurement:
        # Get top 3 pollutants by concentration
        pollutant_measurements = AirQualityRecord.objects.filter(
            measurement=latest_measurement
        ).order_by('-concentration')[:3]

        top_pollutants = [
            {
                'name': pm.get_pollutant_display(),
                'level': f"{pm.concentration:.2f}"
            } for pm in pollutant_measurements
        ]

    # Prepare AQI trend data (last 7 days)
    seven_days_ago = timezone.now() - timedelta(days=7)
    aqi_trend = AirQualityRecord.objects.filter(
        timestamp__gte=seven_days_ago
    ).order_by('timestamp')

    aqi_trend_dates = [m.timestamp.strftime('%Y-%m-%d') for m in aqi_trend]
    aqi_trend_values = [m.aqi for m in aqi_trend]

    # Prepare pollutant levels
    pollutant_levels = AirQualityRecord.objects.filter(
        measurement=latest_measurement
    )
    pollutant_levels_labels = [
        pm.get_pollutant_display() for pm in pollutant_levels
    ]
    pollutant_levels_values = [pm.concentration for pm in pollutant_levels]

    # Get recent alerts (high concentration measurements)
    recent_alerts = []
    high_concentration_measurements = AirQualityRecord.objects.filter(
        concentration__gt='threshold_high'  # Assuming you add a threshold field
    ).select_related('measurement__station')[:5]

    for alert in high_concentration_measurements:
        recent_alerts.append({
            'id': alert.id,
            'station': alert.measurement.station.name,
            'pollutant': alert.get_pollutant_display(),
            'level': 'High' if alert.concentration > alert.threshold_critical else 'Medium',
            'timestamp': alert.measurement.timestamp.strftime('%Y-%m-%d %H:%M')
        })

        context = {
            'current_aqi': latest_measurement.aqi if latest_measurement else 0,
            'aqi_description': _get_aqi_description(latest_measurement.aqi) if latest_measurement else 'N/A',
            'active_stations': active_stations,
            'inactive_stations': inactive_stations,
            'top_pollutants': top_pollutants,
            'recent_alerts': recent_alerts,
            'aqi_trend_dates': aqi_trend_dates,
            'aqi_trend_values': aqi_trend_values,
            'pollutant_levels_labels': pollutant_levels_labels,
            'pollutant_levels_values': pollutant_levels_values,
        }

    return render(request, 'dashboard.html')


def _get_aqi_description(aqi):
    if aqi <= 50:
        return 'Good'
    elif aqi <= 100:
        return 'Moderate'
    elif aqi <= 150:
        return 'Unhealthy for Sensitive Groups'
    elif aqi <= 200:
        return 'Unhealthy'
    else:
        return 'Very Unhealthy'


def about(request):
    import pandas as pd
    air_df = pd.read_json("https://www.data.act.gov.au/resource/94a5-zqnn.json")
    print(f"Size......: {air_df.size}")
    print(f"Columns...: {air_df.columns}")
    print(f"Missing...: {air_df.isna().sum().sum()}")

    print(air_df.head(5))

    return HttpResponse('About')


def privacy_policy(request):
    # # Fetch data from API
    # import requests
    # response = requests.get(settings.AIR_QUALITY_API_URL)
    # response.raise_for_status()
    # air_quality_records = response.json()
    #
    # print(f"API Response..............: {air_quality_records}")
    # Fetch data from API

    # import requests
    # response = requests.get(settings.AIR_QUALITY_API_URL)
    # response.raise_for_status()
    # air_quality_records = response.json()
    #
    # print(f"API Response..............: {air_quality_records}")
    # producer = KafkaMessageProducer()
    # result = producer.send_message(air_quality_records)

    # return JsonResponse({
    #     'status': 'success' if result else 'failed',
    #     'message': air_quality_records
    # })

    return HttpResponse('Privacy Policy')


def contact(request):
    import pandas as pd
    import kagglehub

    # Download latest version
    path = kagglehub.dataset_download("fedesoriano/air-quality-data-set")

    print("Path to dataset files:", path)
    df = pd.read_csv(path + "/AirQuality.csv")

    print("Size....................: ", len(df))
    print("Columns.................: ", df.columns)
    print("Missing values..........: ", df.isna().sum().sum())
    print("Missing values Ration...: ", (df.isna().sum() / len(df)).sum().sum())

    return HttpResponse('Contact')


def datasetapi(request):
    import pandas as pd
    air_df = pd.read_json("https://www.data.act.gov.au/resource/94a5-zqnn.json")
    print(f"Size......: {air_df.size}")

    return HttpResponse('datasetapi')

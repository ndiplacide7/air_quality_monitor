from django.contrib.auth.decorators import login_required
from django.http import HttpResponse, JsonResponse
from django.shortcuts import render

# from air_quality_monitor import settings
# from air_quality_pipeline.services.producer import KafkaMessageProducer


def base(request):
    return render(request, 'base.html')


@login_required
def dashboard(request):
    return render(request, 'dashboard.html')


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

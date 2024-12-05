from django.http import HttpResponse
from django.shortcuts import render


def base(request):
    return render(request, 'base.html')


def dashboard(request):
    return render(request, 'dashboard.html')


def login(request):
    return render(request, 'auth/login.html')


def register(request):
    return render(request, 'auth/signup.html')


def logout(request):
    return render(request, 'auth/login.html')


def about(request):

    import pandas as pd
    air_df = pd.read_json("https://www.data.act.gov.au/resource/94a5-zqnn.json")
    print(f"Size......: {air_df.size}")

    return HttpResponse('About')


def privacy_policy(request):
    return HttpResponse('Privacy Policy')


def contact(request):

    import pandas as pd
    import kagglehub

    # Download latest version
    path = kagglehub.dataset_download("fedesoriano/air-quality-data-set")

    print("Path to dataset files:", path)
    df = pd.read_csv(path + "/AirQuality.csv")

    print((df.isna().sum() / len(df)).sum().sum())

    return HttpResponse('Contact')


def datasetapi(request):

    import pandas as pd
    air_df = pd.read_json("https://www.data.act.gov.au/resource/94a5-zqnn.json")
    print(f"Size......: {air_df.size}")

    return HttpResponse('datasetapi')

from django.shortcuts import render, redirect, get_object_or_404
from django.contrib.auth.decorators import login_required
from .models import AirQualityStation
from .forms import StationForm


@login_required
def station_list(request):
    stations = AirQualityStation.objects.filter(owner=request.user)
    return render(request, 'stations/list.html', {'stations': stations})


@login_required
def station_create(request):
    if request.method == 'POST':
        form = StationForm(request.POST)
        if form.is_valid():
            station = form.save(commit=False)
            station.owner = request.user
            station.save()
            return redirect('station_list')
    else:
        form = StationForm()
    return render(request, 'stations/create.html', {'form': form})


@login_required
def station_update(request, pk):
    station = get_object_or_404(AirQualityStation, pk=pk, owner=request.user)
    if request.method == 'POST':
        form = StationForm(request.POST, instance=station)
        if form.is_valid():
            form.save()
            return redirect('station_list')
    else:
        form = StationForm(instance=station)
    return render(request, 'stations/update.html', {'form': form})


@login_required
def station_delete(request, pk):
    station = get_object_or_404(AirQualityStation, pk=pk, owner=request.user)
    if request.method == 'POST':
        station.delete()
        return redirect('station_list')
    return render(request, 'stations/delete.html', {'station': station})

from django.urls import path
from . import views

urlpatterns = [
    path('', views.station_list, name='station_list'),
    path('create/', views.station_create, name='station_create'),
    path('update/<int:pk>/', views.station_update, name='station_update'),
    path('delete/<int:pk>/', views.station_delete, name='station_delete'),
]
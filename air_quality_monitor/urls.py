"""
URL configuration for air_quality_monitor project.

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/5.1/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""
from django.contrib import admin
from django.urls import path, include

from air_quality_monitor import views

urlpatterns = [
    path('', views.base, name='base'),
    path('dashboard/', views.dashboard, name='dashboard'),
    # path('login/', views.login, name='login'),
    # path('register/', views.register, name='register'),
    # path('logout/', views.logout, name='logout'),
    path('about/', views.about, name='about'),
    path('privacy_policy/', views.privacy_policy, name='privacy_policy'),
    path('contact/', views.contact, name='contact'),


    path('admin/', admin.site.urls),
    path('accounts/', include('accounts.urls')),
    path('stations/', include('stations.urls')),
]

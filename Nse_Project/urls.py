"""
URL configuration for MyProject2 project.

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/6.0/topics/http/urls/
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
from django.views.generic import RedirectView  # 🔴 ఇది కొత్తగా ఇంపోర్ట్ చేయండి
from Nse_Project.Live_dashboard.views import live_signals_view
from Nse_Project.Swing_stocks.views import swing_stocks_view
from Nse_Project.Swing_ML.views import swing_ml_predictions_view

urlpatterns = [
    path('admin/', admin.site.urls),
    
    # 🔴 హోమ్ పేజీ (Root URL) ఓపెన్ చేయగానే ఆటోమేటిక్ గా live-dashboard/ కి వెళ్తుంది
    path('', RedirectView.as_view(url='/live-signals/', permanent=False)),
    path('live-signals/', live_signals_view, name='live_signals'),
    path('swing-stocks/', swing_stocks_view, name='swing_stocks'),
    path('swing-ml/', swing_ml_predictions_view, name='swing_ml'),
    
    path('swing/', include('Nse_Project.Swing_stocks.urls')),
    path('live-dashboard/', include('Nse_Project.Live_dashboard.urls')),
    path('gamma/', include('Nse_Project.GAMMA.urls')),
    path('trend-setups/', include('Nse_Project.funda_fetch.urls')),

    # 52-Week-Low SMC queue ticks. Included with an empty prefix so the URL
    # resolves to /queue-ticks/ and the sidebar can use ``{% url 'queue_ticks' %}``
    # without a namespace.
    path('', include('Nse_Project.52_Week_Low.urls')),
]
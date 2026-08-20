from django.urls import path
from .views import get_live_alerts, swing_dashboard

app_name = 'swing_stocks'

urlpatterns = [
    path('dashboard/', swing_dashboard, name='swing_dashboard'),
    path('api/live-alerts/', get_live_alerts, name='get_live_alerts'),
]

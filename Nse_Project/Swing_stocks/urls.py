from django.urls import path
from .views import get_live_alerts, swing_dashboard, swing_stocks_view

app_name = 'swing_stocks'

urlpatterns = [
    path('dashboard/', swing_dashboard, name='swing_dashboard'),
    path('swing-stocks/', swing_stocks_view, name='swing_stocks'),
    path('api/live-alerts/', get_live_alerts, name='get_live_alerts'),
]

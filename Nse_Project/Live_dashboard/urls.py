from django.urls import path
from . import views

app_name = 'live_dashboard'

urlpatterns = [
    path('', views.dashboard_view, name='dashboard'),
    path('api/live-options/', views.live_option_chain_api, name='live_option_chain_api'),
]
from django.urls import path
from . import views

app_name = 'live_dashboard'

urlpatterns = [
    path('', views.dashboard_view, name='dashboard'),
    path('live-signals/', views.live_signals_view, name='live_signals'),
    path('api/live-options/', views.live_option_chain_api, name='live_option_chain_api'),
    path('api/preopen-trap/', views.preopen_trap_api, name='preopen_trap_api'),
    path('upstox-login/', views.upstox_login, name='upstox_login'),
    path('upstox-callback/', views.upstox_callback, name='upstox_callback'),
]
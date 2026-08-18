from django.urls import path
from . import views

app_name = 'swing_ml'

urlpatterns = [
    # మీ డాష్‌బోర్డ్ పేజీ లింక్ 
    path('dashboard/', views.swing_dashboard_view, name='swing_dashboard'),
]
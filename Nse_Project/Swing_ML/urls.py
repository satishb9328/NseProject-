from django.urls import path
from . import views

app_name = 'swing_ml'

urlpatterns = [
    path('dashboard/', views.swing_ml_dashboard, name='swing_ml_dashboard'),
]
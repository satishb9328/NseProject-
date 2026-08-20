from django.urls import path
from . import views

app_name = 'swing_stocks'

urlpatterns = [
    path('dashboard/', views.swing_dashboard, name='swing_dashboard'),
]

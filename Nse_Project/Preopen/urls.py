from django.urls import path
from .views import preopen_price_range
from . import views

urlpatterns = [
    path('price-range/', preopen_price_range, name='preopen_price_range'),
    
]

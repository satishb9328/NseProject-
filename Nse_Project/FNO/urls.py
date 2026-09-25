from django.urls import path

from .views import fno_performance_view


urlpatterns = [
	path("performance/", fno_performance_view, name="performance"),
]

from django.urls import path

from .views import fno_performance_view, run_fno_update


urlpatterns = [
	path("performance/", fno_performance_view, name="performance"),
	path("update/", run_fno_update, name="run_fno_update"),
]

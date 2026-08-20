from django.urls import path

from . import views

app_name = "gamma"

urlpatterns = [
	path("start/", views.start_gamma_bot, name="start"),
	path("stop/", views.stop_gamma_bot, name="stop"),
	path("status/", views.gamma_bot_status, name="status"),
]

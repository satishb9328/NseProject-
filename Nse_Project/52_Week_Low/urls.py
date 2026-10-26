from django.urls import path

from . import views

# NOTE: This app directory is named ``52_Week_Low`` (leading digit), so it is
# wired into the project via ``include('Nse_Project.52_Week_Low.urls')`` in the
# root ``Nse_Project/urls.py``. Keeping this urlconf *un-namespaced* lets the
# sidebar use ``{% url 'queue_ticks' %}`` directly.
urlpatterns = [
    path("queue-ticks/", views.queue_ticks_smc_view, name="queue_ticks"),
    path("watchlist/", views.watchlist_view, name="watchlist"),
]


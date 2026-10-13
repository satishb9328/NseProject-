from django.urls import path

from . import views

# NOTE: funda_fetch is intentionally kept un-namespaced (like 52_Week_Low) so the
# sidebar can reference the view directly with ``{% url 'trend_setups' %}``.
urlpatterns = [
    path("", views.trend_setups_view, name="trend_setups"),
    path("live-signals/", views.live_signals_today, name="live_signals_today"),
]

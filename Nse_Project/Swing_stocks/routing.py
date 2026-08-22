"""Django Channels WebSocket routing for the Swing live feed."""
from django.urls import re_path

from . import consumers

websocket_urlpatterns = [
    re_path(r"ws/swing/sepa/$", consumers.SwingSepaConsumer.as_asgi()),
]

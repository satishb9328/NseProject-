"""Django Channels WebSocket routing for the live dashboard."""
from django.urls import re_path

from . import consumers

websocket_urlpatterns = [
    re_path(r"ws/cvd-live/$", consumers.CvdLiveConsumer.as_asgi()),
    re_path(r"ws/preopen-live/$", consumers.PreopenTrapConsumer.as_asgi()),
    re_path(r"ws/ai-trade-triggers/$", consumers.AITradeTriggerConsumer.as_asgi()),
]

import os
from django.core.asgi import get_asgi_application
from channels.routing import ProtocolTypeRouter, URLRouter

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'Nse_Project.settings')

# Routing ఇంపోర్ట్స్ పైన కాకుండా ఇక్కడ లోపల ఉండాలి (App Registry ఎర్రర్ రాకుండా)
django_asgi_app = get_asgi_application()

from Nse_Project.Live_dashboard.routing import websocket_urlpatterns as live_ws_patterns
from Nse_Project.Swing_stocks.routing import websocket_urlpatterns as swing_ws_patterns

application = ProtocolTypeRouter({
    "http": django_asgi_app,
    "websocket": URLRouter(
        swing_ws_patterns + live_ws_patterns
    ),
})
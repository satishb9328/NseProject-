"""ASGI config for MyProject2 project."""
import os

from django.core.asgi import get_asgi_application

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'Nse_Project.settings')

try:
    from channels.routing import ProtocolTypeRouter, URLRouter

    from Nse_Project.Live_dashboard.routing import websocket_urlpatterns as live_ws_patterns
    from Nse_Project.Swing_stocks.routing import websocket_urlpatterns as swing_ws_patterns

    application = ProtocolTypeRouter(
        {
            "http": get_asgi_application(),
            "websocket": URLRouter(swing_ws_patterns + live_ws_patterns),
        }
    )
except Exception:
    application = get_asgi_application()
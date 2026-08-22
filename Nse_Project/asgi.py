"""
ASGI config for MyProject2 project.
"""
import os

from django.core.asgi import get_asgi_application

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'Nse_Project.settings')

# Prefer the Channels ASGI stack (WebSocket) when it is installed; otherwise fall
# back to the plain Django ASGI app so the WSGI ``runserver`` keeps working unchanged.
try:
    from channels.routing import ProtocolTypeRouter, URLRouter

    from Nse_Project.Swing_stocks.routing import websocket_urlpatterns

    application = ProtocolTypeRouter(
        {
            "http": get_asgi_application(),
            "websocket": URLRouter(websocket_urlpatterns),
        }
    )
except Exception:
    application = get_asgi_application()


"""
Django Channels consumer + Redis bridge for the live Cumulative Volume Delta feed.

The background CVD worker publishes JSON to the raw Redis Pub/Sub channel
``cvd:2m_candle_closed``.  Django Channels cannot subscribe a channel layer to an
*arbitrary* raw Redis channel (its groups use their own internal key format), so this
module runs ONE lightweight bridge thread that subscribes to ``cvd:2m_candle_closed``
and re-broadcasts every payload into the Channels group ``cvd_live_updates``.
``CvdLiveConsumer`` joins that group and forwards each payload to every connected
browser.
"""

import json
import logging
import os
import threading
import time

from asgiref.sync import async_to_sync, sync_to_async
from channels.generic.websocket import AsyncWebsocketConsumer
from channels.layers import get_channel_layer

from .ai_candle_confirmation import AI_TRADE_GROUP

try:
    import redis
except ImportError:  # keeps the module importable if redis-py is missing
    redis = None

logger = logging.getLogger(__name__)

REDIS_URL = os.getenv('REDIS_URL', 'redis://127.0.0.1:6379/0')

# Keep these in sync with cvd_engine.py (env-overridable, same style as the Gamma feed).
CVD_REDIS_URL = os.getenv("CVD_REDIS_URL", "redis://127.0.0.1:6379/0")
CVD_REDIS_CHANNEL = os.getenv("CVD_REDIS_CHANNEL", "cvd:2m_candle_closed")
CVD_REDIS_SNAPSHOT_KEY = os.getenv("CVD_REDIS_SNAPSHOT_KEY", "cvd_2m_snapshot")

# Channels group that browser sockets join.
CVD_GROUP = "cvd_live_updates"

_bridge_started = False
_bridge_lock = threading.Lock()

# Redis resilience: timeouts + health checks so a stale/dead connection is detected
# and replaced rather than blocking the event loop or a worker thread.
REDIS_SOCKET_TIMEOUT = float(os.getenv("CVD_REDIS_SOCKET_TIMEOUT", "10"))
REDIS_SOCKET_CONNECT_TIMEOUT = float(os.getenv("CVD_REDIS_SOCKET_CONNECT_TIMEOUT", "5"))
REDIS_HEALTH_CHECK_INTERVAL = int(os.getenv("CVD_REDIS_HEALTH_CHECK_INTERVAL", "30"))

_snapshot_client = None
_snapshot_client_lock = threading.Lock()


def _get_snapshot_client():
    """Lazily build a single, hardened, process-lifetime sync Redis client."""
    global _snapshot_client
    if _snapshot_client is None:
        with _snapshot_client_lock:
            if _snapshot_client is None:
                _snapshot_client = redis.Redis.from_url(
                    CVD_REDIS_URL,
                    decode_responses=True,
                    health_check_interval=REDIS_HEALTH_CHECK_INTERVAL,
                    socket_timeout=REDIS_SOCKET_TIMEOUT,
                    socket_connect_timeout=REDIS_SOCKET_CONNECT_TIMEOUT,
                    socket_keepalive=True,
                )
    return _snapshot_client


def _dispatch_to_group(payload: dict) -> None:
    """Forward a parsed CVD payload into the Channels broadcast group."""
    layer = get_channel_layer()
    if layer is None:
        return
    async_to_sync(layer.group_send)(
        CVD_GROUP, {"type": "cvd.update", "payload": payload}
    )


def _bridge_loop() -> None:
    """Subscribe to the raw Redis channel and re-publish into the Channels group."""
    if redis is None:
        logger.warning("redis-py is not installed; CVD bridge disabled.")
        return

    while True:
        client = None
        try:
            client = redis.Redis.from_url(
                CVD_REDIS_URL,
                decode_responses=True,
                health_check_interval=REDIS_HEALTH_CHECK_INTERVAL,
                socket_timeout=REDIS_SOCKET_TIMEOUT,
                socket_connect_timeout=REDIS_SOCKET_CONNECT_TIMEOUT,
                socket_keepalive=True,
            )
            pubsub = client.pubsub(ignore_subscribe_messages=True)
            pubsub.subscribe(CVD_REDIS_CHANNEL)
            logger.info("CVD Redis bridge subscribed to '%s'.", CVD_REDIS_CHANNEL)

            for message in pubsub.listen():
                if message.get("type") != "message":
                    continue
                raw = message.get("data")
                try:
                    payload = json.loads(raw) if isinstance(raw, str) else {}
                except (TypeError, ValueError):
                    continue
                if payload:
                    _dispatch_to_group(payload)
        except redis.exceptions.TimeoutError:
            logger.warning("CVD Redis bridge timed out; reconnecting in 2s...")
        except redis.exceptions.RedisError as exc:
            logger.warning("CVD Redis bridge error (%s); reconnecting in 2s...", exc)
        except Exception:
            logger.exception("CVD Redis bridge unexpected error; reconnecting in 2s...")
        finally:
            if client is not None:
                try:
                    client.close()
                except Exception:
                    pass

        time.sleep(2)


def start_cvd_bridge() -> None:
    """Start the bridge thread once per process (idempotent)."""
    global _bridge_started
    with _bridge_lock:
        if _bridge_started:
            return
        _bridge_started = True

    thread = threading.Thread(target=_bridge_loop, name="cvd-redis-bridge", daemon=True)
    thread.start()
    logger.info("CVD Redis bridge thread started.")


def _read_snapshot() -> dict:
    """Read the last published CVD snapshot (the key the worker SETs on each candle)."""
    if redis is None:
        return {}
    try:
        raw = _get_snapshot_client().get(CVD_REDIS_SNAPSHOT_KEY)
        if raw:
            return json.loads(raw)
    except redis.exceptions.TimeoutError:
        logger.warning("CVD snapshot read timed out; serving an empty snapshot.")
    except redis.exceptions.RedisError as exc:
        logger.warning("CVD snapshot read failed: %s", exc)
    except Exception:
        logger.debug("CVD snapshot read failed.", exc_info=True)
    return {}


class CvdLiveConsumer(AsyncWebsocketConsumer):
    """Streams live CVD updates to the dashboard over WebSocket."""

    async def connect(self):
        # Ensure the raw-Redis -> group bridge is running (single instance per process).
        start_cvd_bridge()

        if self.channel_layer is not None:
            await self.channel_layer.group_add(CVD_GROUP, self.channel_name)
        await self.accept()

        # Send the latest snapshot immediately so the widget is never empty while
        # waiting up to 2 minutes for the next candle close.
        snapshot = await sync_to_async(_read_snapshot)()
        if snapshot:
            try:
                await self.send(
                    text_data=json.dumps({"type": "cvd_snapshot", "payload": snapshot})
                )
            except Exception:
                logger.debug("CVD snapshot send failed; client disconnected.", exc_info=True)

    async def disconnect(self, close_code):
        if self.channel_layer is not None:
            try:
                await self.channel_layer.group_discard(CVD_GROUP, self.channel_name)
            except Exception:
                logger.debug("CVD group_discard failed on disconnect.", exc_info=True)

    async def cvd_update(self, event):
        """Handler for the ``cvd.update`` group message sent by the bridge."""
        payload = event.get("payload")
        if payload is None:
            return
        try:
            await self.send(
                text_data=json.dumps({"type": "cvd_update", "payload": payload})
            )
        except Exception:
            logger.debug("CVD update send failed; client disconnected.", exc_info=True)


# ---------------------------------------------------------------------------
# PRE-OPEN GAMMA TRAP bridge + consumer
# ---------------------------------------------------------------------------
# The 09:10 publisher (``Live_dashboard.views.publish_preopen_trap_to_redis``)
# writes the final pre-open snapshot to Redis (SET ``preopen_trap_data`` + PUBLISH
# ``preopen:trap_updated``). This mirrors the CVD bridge above: one lightweight
# thread subscribes to the raw Redis channel and re-broadcasts every payload into
# the Channels group ``preopen_updates``, which ``PreopenTrapConsumer`` joins and
# forwards to every connected browser.
PREOPEN_REDIS_CHANNEL = os.getenv("PREOPEN_REDIS_CHANNEL", "preopen:trap_updated")
PREOPEN_REDIS_SNAPSHOT_KEY = os.getenv("PREOPEN_REDIS_DATA_KEY", "preopen_trap_data")

# Channels group that browser sockets join for the pre-open feed.
PREOPEN_GROUP = "preopen_updates"

_preopen_bridge_started = False
_preopen_bridge_lock = threading.Lock()
_preopen_snapshot_client = None
_preopen_snapshot_lock = threading.Lock()


def _get_preopen_snapshot_client():
    """Lazily build a single, hardened, process-lifetime sync Redis client."""
    global _preopen_snapshot_client
    if _preopen_snapshot_client is None:
        with _preopen_snapshot_lock:
            if _preopen_snapshot_client is None:
                _preopen_snapshot_client = redis.Redis.from_url(
                    REDIS_URL,
                    decode_responses=True,
                    health_check_interval=REDIS_HEALTH_CHECK_INTERVAL,
                    socket_timeout=REDIS_SOCKET_TIMEOUT,
                    socket_connect_timeout=REDIS_SOCKET_CONNECT_TIMEOUT,
                    socket_keepalive=True,
                )
    return _preopen_snapshot_client


def _dispatch_preopen_to_group(payload: dict) -> None:
    """Forward a parsed pre-open payload into the Channels broadcast group."""
    layer = get_channel_layer()
    if layer is None:
        return
    async_to_sync(layer.group_send)(
        PREOPEN_GROUP, {"type": "preopen.update", "payload": payload}
    )


def _preopen_bridge_loop() -> None:
    """Subscribe to the raw Redis channel and re-publish into the Channels group."""
    if redis is None:
        logger.warning("redis-py is not installed; pre-open bridge disabled.")
        return

    while True:
        client = None
        try:
            client = redis.Redis.from_url(
                REDIS_URL,
                decode_responses=True,
                health_check_interval=REDIS_HEALTH_CHECK_INTERVAL,
                socket_timeout=REDIS_SOCKET_TIMEOUT,
                socket_connect_timeout=REDIS_SOCKET_CONNECT_TIMEOUT,
                socket_keepalive=True,
            )
            pubsub = client.pubsub(ignore_subscribe_messages=True)
            pubsub.subscribe(PREOPEN_REDIS_CHANNEL)
            logger.info("Pre-open Redis bridge subscribed to '%s'.", PREOPEN_REDIS_CHANNEL)

            for message in pubsub.listen():
                if message.get("type") != "message":
                    continue
                raw = message.get("data")
                try:
                    payload = json.loads(raw) if isinstance(raw, str) else {}
                except (TypeError, ValueError):
                    continue
                if payload:
                    _dispatch_preopen_to_group(payload)
        except redis.exceptions.TimeoutError:
            logger.warning("Pre-open Redis bridge timed out; reconnecting in 2s...")
        except redis.exceptions.RedisError as exc:
            logger.warning("Pre-open Redis bridge error (%s); reconnecting in 2s...", exc)
        except Exception:
            logger.exception("Pre-open Redis bridge unexpected error; reconnecting in 2s...")
        finally:
            if client is not None:
                try:
                    client.close()
                except Exception:
                    pass

        time.sleep(2)


def start_preopen_bridge() -> None:
    """Start the pre-open bridge thread once per process (idempotent)."""
    global _preopen_bridge_started
    with _preopen_bridge_lock:
        if _preopen_bridge_started:
            return
        _preopen_bridge_started = True

    thread = threading.Thread(target=_preopen_bridge_loop, name="preopen-redis-bridge", daemon=True)
    thread.start()
    logger.info("Pre-open Redis bridge thread started.")


def _read_preopen_snapshot() -> dict:
    """Read the last published pre-open snapshot (the key the 09:10 publisher SETs)."""
    if redis is None:
        return {}
    try:
        raw = _get_preopen_snapshot_client().get(PREOPEN_REDIS_SNAPSHOT_KEY)
        if raw:
            return json.loads(raw)
    except redis.exceptions.TimeoutError:
        logger.warning("Pre-open snapshot read timed out; serving an empty snapshot.")
    except redis.exceptions.RedisError as exc:
        logger.warning("Pre-open snapshot read failed: %s", exc)
    except Exception:
        logger.debug("Pre-open snapshot read failed.", exc_info=True)
    return {}


class PreopenTrapConsumer(AsyncWebsocketConsumer):
    """Streams the final PRE-OPEN GAMMA TRAP snapshot to the dashboard over WebSocket."""

    async def connect(self):
        # Ensure the raw-Redis -> group bridge is running (single instance per process).
        start_preopen_bridge()

        if self.channel_layer is not None:
            await self.channel_layer.group_add(PREOPEN_GROUP, self.channel_name)
        await self.accept()

        # Push the latest snapshot immediately so a page opened after 09:10 never has
        # to wait for a re-broadcast of the (already fired) settle event.
        snapshot = await sync_to_async(_read_preopen_snapshot)()
        if snapshot:
            try:
                await self.send(
                    text_data=json.dumps({"type": "preopen_update", "payload": snapshot})
                )
            except Exception:
                logger.debug("Pre-open snapshot send failed; client disconnected.", exc_info=True)

    async def disconnect(self, close_code):
        if self.channel_layer is not None:
            try:
                await self.channel_layer.group_discard(PREOPEN_GROUP, self.channel_name)
            except Exception:
                logger.debug("Pre-open group_discard failed on disconnect.", exc_info=True)

    async def preopen_update(self, event):
        """Handler for the ``preopen.update`` group message sent by the bridge."""
        payload = event.get("payload")
        if payload is None:
            return
        try:
            await self.send(
                text_data=json.dumps({"type": "preopen_update", "payload": payload})
            )
        except Exception:
            logger.debug("Pre-open update send failed; client disconnected.", exc_info=True)


# ---------------------------------------------------------------------------
# AI TRADE SETUP bridge + consumer
# ---------------------------------------------------------------------------
# The AI candle-confirmation engine (wired into the standalone feed worker
# ``upstox_live_feed.py``) publishes each BUY_NOW trigger to the raw Redis channel
# ``ai:trade_trigger``. This mirrors the CVD / pre-open bridges: one lightweight
# thread subscribes to that channel and re-broadcasts every payload into the
# Channels group ``ai_trade_setups``, which ``AITradeTriggerConsumer`` joins.
AI_TRADE_REDIS_CHANNEL = os.getenv("AI_TRADE_REDIS_CHANNEL", "ai:trade_trigger")

_ai_trade_bridge_started = False
_ai_trade_bridge_lock = threading.Lock()


def _dispatch_ai_trade_to_group(trigger: dict) -> None:
    """Forward a parsed AI trade trigger into the Channels broadcast group."""
    layer = get_channel_layer()
    if layer is None:
        return
    async_to_sync(layer.group_send)(
        AI_TRADE_GROUP, {"type": "trade.trigger", "trigger": trigger}
    )


def _ai_trade_bridge_loop() -> None:
    """Subscribe to the raw Redis channel and re-publish into the Channels group."""
    if redis is None:
        logger.warning("redis-py is not installed; AI trade bridge disabled.")
        return
    while True:
        client = None
        try:
            client = redis.Redis.from_url(
                REDIS_URL,
                decode_responses=True,
                health_check_interval=REDIS_HEALTH_CHECK_INTERVAL,
                socket_timeout=REDIS_SOCKET_TIMEOUT,
                socket_connect_timeout=REDIS_SOCKET_CONNECT_TIMEOUT,
                socket_keepalive=True,
            )
            pubsub = client.pubsub(ignore_subscribe_messages=True)
            pubsub.subscribe(AI_TRADE_REDIS_CHANNEL)
            for message in pubsub.listen():
                if message.get("type") != "message":
                    continue
                raw = message.get("data")
                try:
                    trigger = json.loads(raw) if isinstance(raw, str) else {}
                except (TypeError, ValueError):
                    continue
                if trigger:
                    _dispatch_ai_trade_to_group(trigger)
        except redis.exceptions.TimeoutError:
            logger.warning("AI trade Redis bridge timed out; reconnecting in 2s...")
        except redis.exceptions.RedisError as exc:
            logger.warning("AI trade Redis bridge error (%s); reconnecting in 2s...", exc)
        except Exception:
            logger.exception("AI trade Redis bridge unexpected error; reconnecting in 2s...")
        finally:
            if client is not None:
                try:
                    client.close()
                except Exception:
                    pass
        time.sleep(2)


def start_ai_trade_bridge() -> None:
    """Start the AI trade bridge thread once per process (idempotent)."""
    global _ai_trade_bridge_started
    with _ai_trade_bridge_lock:
        if _ai_trade_bridge_started:
            return
        _ai_trade_bridge_started = True
    thread = threading.Thread(target=_ai_trade_bridge_loop, name="ai-trade-redis-bridge", daemon=True)
    thread.start()
    logger.info("AI trade Redis bridge thread started.")


class AITradeTriggerConsumer(AsyncWebsocketConsumer):
    """Streams AI trade-setup BUY_NOW triggers to the dashboard over WebSocket."""

    async def connect(self):
        # Ensure the raw-Redis -> group bridge is running (single instance per process).
        start_ai_trade_bridge()

        if self.channel_layer is not None:
            await self.channel_layer.group_add(AI_TRADE_GROUP, self.channel_name)
        await self.accept()

    async def disconnect(self, close_code):
        if self.channel_layer is not None:
            try:
                await self.channel_layer.group_discard(AI_TRADE_GROUP, self.channel_name)
            except Exception:
                logger.debug("AI trade group_discard failed on disconnect.", exc_info=True)

    async def trade_trigger(self, event):
        """Handler for the ``trade.trigger`` group message dispatched by the module."""
        trigger = event.get("trigger")
        if trigger is None:
            return
        try:
            await self.send(text_data=json.dumps(trigger))
        except Exception:
            logger.debug("AI trade trigger send failed; client disconnected.", exc_info=True)

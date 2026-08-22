"""Django Channels consumer for the Swing "STRONG SEPA" live feed.

This consumer owns the browser WebSocket connection for the Swing dashboard
``STRONG SEPA STOCKS`` table. It:

* joins the ``swing_sepa_updates`` broadcast group so the background SEPA worker
  can push live rows to every connected browser, and
* immediately sends the current STRONG SEPA snapshot on connect so the table is
  never stuck on stale/empty data.
"""
import json
import math
from pathlib import Path

import pandas as pd
from asgiref.sync import sync_to_async
from channels.generic.websocket import AsyncWebsocketConsumer

from .combo_filter import filter_combo_setups

SWING_SEPA_GROUP = "swing_sepa_updates"
STRONG_SEPA_CSV = Path(__file__).resolve().parent / "nifty200_smc_precision_screen.csv"
STRONG_SEPA_FIELDS = ("symbol", "rs_vs_index", "vcp", "proximity", "smc_signal", "score")


def _json_safe(value):
    """Convert non-finite floats (NaN/inf) to None so the payload parses in browsers."""
    if value is None:
        return None
    if isinstance(value, float):
        try:
            if math.isnan(value) or math.isinf(value):
                return None
        except (TypeError, ValueError):
            pass
    return value


def build_strong_sepa_snapshot() -> list:
    """Return the current STRONG_SEPA rows (only the columns the table renders)."""
    if not STRONG_SEPA_CSV.exists():
        return []
    try:
        df = pd.read_csv(STRONG_SEPA_CSV)
        df = df.where(pd.notnull(df), None)
        rows = filter_combo_setups(df).get("STRONG_SEPA", [])
        return [
            {key: _json_safe(row.get(key)) for key in STRONG_SEPA_FIELDS}
            for row in rows
        ]
    except Exception:
        return []


class SwingSepaConsumer(AsyncWebsocketConsumer):
    """Streams STRONG SEPA stock updates to the swing dashboard over WebSocket."""

    async def connect(self):
        # Join the broadcast group (skip gracefully if no channel layer is configured).
        if self.channel_layer is not None:
            await self.channel_layer.group_add(SWING_SEPA_GROUP, self.channel_name)
        await self.accept()
        await self._send_snapshot()

    async def disconnect(self, close_code):
        if self.channel_layer is not None:
            await self.channel_layer.group_discard(SWING_SEPA_GROUP, self.channel_name)

    async def receive(self, text_data=None, bytes_data=None):
        if not text_data:
            return
        try:
            payload = json.loads(text_data)
        except Exception:
            payload = {}
        if payload.get("action") == "refresh":
            await self._send_snapshot()

    async def swing_sepa_update(self, event):
        """Group broadcast handler — forward a worker update to this socket."""
        await self.send(
            text_data=json.dumps(
                {
                    "type": "swing_sepa_update",
                    "strong_sepa": event.get("strong_sepa", []),
                    "updated_at": event.get("updated_at"),
                }
            )
        )

    async def _send_snapshot(self):
        snapshot = await sync_to_async(build_strong_sepa_snapshot)()
        await self.send(
            text_data=json.dumps(
                {
                    "type": "swing_sepa_update",
                    "strong_sepa": snapshot,
                    "updated_at": None,
                }
            )
        )

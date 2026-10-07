"""
1-Minute Candle Confirmation Module for AI Trade Setups (CVD-confirmed).

Implements the dual-confirmation gate that fires a ``BUY_NOW`` trade trigger for
the AI Trade Setups card:

    1. ENTRY ZONE — LTP within ``ENTRY_ZONE_PCT`` (1.5%) of the AI entry price.
    2. CANDLE      — the just-closed candle prints a Bullish Engulfing (2 bars)
       or a Bullish Pin Bar / hammer (long lower wick).
    3. CVD         — the candle's Volume Delta is positive (buyers dominated),
       OR the recent 3-candle CVD trend is ticking upward.

When all gates pass, this exact payload is dispatched to the ``ai_trade_setups``
Channels group::

    {"action": "trade_trigger", "status": "BUY_NOW", "setup_type": "<setup_name>"}

Integration with the existing CVD engine (which already emits OHLC + ``delta`` +
``cvd`` per closed candle) is a one-liner::

    from Nse_Project.Brokers.cvd_engine import CVDEngine
    from Nse_Project.Live_dashboard.ai_candle_confirmation import CandleConfirmationEngine

    cvd = CVDEngine(candle_seconds=60, trend_periods=3, ...)   # 1-minute bars
    confirm = CandleConfirmationEngine("Momentum Pullback (15% Dip)", 24150.0)
    cvd.on_candle = confirm.on_candle

The pure detectors are stdlib-only and safe to unit-test without Django.
"""

from __future__ import annotations

import logging
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Sequence

logger = logging.getLogger(__name__)

# ── Tuning constants ──────────────────────────────────────────────────────────
ENTRY_ZONE_PCT = 0.015           # LTP within ±1.5% of the AI entry price
TREND_WINDOW = 3                 # "3-minute" CVD trend = last 3 closed candles
PIN_BAR_WICK_RATIO = 2.0         # lower wick >= 2x the real body
PIN_BAR_LOWER_WICK_FRAC = 0.5    # lower wick >= 50% of the whole range
PIN_BAR_UPPER_WICK_FRAC = 0.35   # upper wick <= 35% of the whole range

# Django Channels group + message type (must match the consumer + routing).
AI_TRADE_GROUP = "ai_trade_setups"
TRIGGER_MESSAGE_TYPE = "trade.trigger"

__all__ = [
    "ConfirmationResult",
    "CandleConfirmationEngine",
    "AI_TRADE_GROUP",
    "within_entry_zone",
    "is_bullish_pin_bar",
    "is_bullish_engulfing",
    "has_bullish_pattern",
    "cvd_positive",
    "cvd_trend_rising",
    "cvd_confirmed",
    "evaluate_confirmation",
    "build_trigger_payload",
    "dispatch_trade_trigger",
]


def _to_float(value: Any) -> Optional[float]:
    """Coerce ``value`` to float, returning None for missing/NaN/unparseable."""
    if value is None:
        return None
    try:
        f = float(value)
    except (TypeError, ValueError):
        return None
    return f if f == f else None


def _candle_ohlc(candle: Dict[str, Any]) -> tuple:
    """Return ``(open, high, low, close)``, raising on missing/invalid fields."""
    o = _to_float(candle.get("open"))
    h = _to_float(candle.get("high"))
    l = _to_float(candle.get("low"))
    c = _to_float(candle.get("close"))
    if None in (o, h, l, c):
        raise ValueError(f"Candle missing OHLC fields: {candle}")
    return o, h, l, c


# ── Gate 1: entry zone ────────────────────────────────────────────────────────
def within_entry_zone(ltp, entry_price, zone_pct: float = ENTRY_ZONE_PCT) -> bool:
    """True when ``ltp`` is within ``zone_pct`` of ``entry_price``."""
    ltp_f = _to_float(ltp)
    entry_f = _to_float(entry_price)
    if ltp_f is None or entry_f is None or entry_f <= 0:
        return False
    return abs(ltp_f - entry_f) / entry_f <= zone_pct


# ── Gate 2: candle pattern ────────────────────────────────────────────────────
def is_bullish_pin_bar(
    candle: Dict[str, Any],
    *,
    wick_ratio: float = PIN_BAR_WICK_RATIO,
    lower_frac: float = PIN_BAR_LOWER_WICK_FRAC,
    upper_frac: float = PIN_BAR_UPPER_WICK_FRAC,
) -> bool:
    """True when the candle is a bullish pin bar / hammer (long lower wick)."""
    o, h, l, c = _candle_ohlc(candle)
    total_range = h - l
    if total_range <= 0:
        return False

    body = abs(c - o)
    lower_wick = min(o, c) - l
    upper_wick = h - max(o, c)

    long_lower = lower_wick >= wick_ratio * body and lower_wick >= lower_frac * total_range
    small_upper = upper_wick <= upper_frac * total_range
    closed_high = c >= (h + l) / 2.0
    return long_lower and small_upper and closed_high


def is_bullish_engulfing(candle: Dict[str, Any], prev_candle: Optional[Dict[str, Any]]) -> bool:
    """True when ``candle`` bullishly engulfs the previous (bearish) candle body."""
    if not isinstance(prev_candle, dict):
        return False
    o, _h, _l, c = _candle_ohlc(candle)
    po, _ph, _pl, pc = _candle_ohlc(prev_candle)

    prev_bearish = pc < po
    curr_bullish = c > o
    engulfs = o <= pc and c >= po and (c - o) > (po - pc)
    return prev_bearish and curr_bullish and engulfs


def has_bullish_pattern(
    candle: Dict[str, Any], prev_candle: Optional[Dict[str, Any]]
) -> Optional[str]:
    """Return the detected bullish pattern name, or ``None``."""
    if is_bullish_engulfing(candle, prev_candle):
        return "bullish_engulfing"
    if is_bullish_pin_bar(candle):
        return "bullish_pin_bar"
    return None


# ── Gate 3: CVD confirmation ──────────────────────────────────────────────────
def cvd_positive(candle: Dict[str, Any]) -> bool:
    """True when this candle's volume delta is positive (buyers dominated)."""
    delta = _to_float(candle.get("delta"))
    return delta is not None and delta > 0


def cvd_trend_rising(cvd_series: Sequence, window: int = TREND_WINDOW) -> bool:
    """True when cumulative CVD is higher than it was ``window`` candles ago."""
    series = [v for v in (_to_float(v) for v in cvd_series) if v is not None]
    if len(series) < 2:
        return False
    lookback = min(window, len(series) - 1)
    return series[-1] > series[-1 - lookback]


def cvd_confirmed(
    candle: Dict[str, Any], cvd_series: Sequence, window: int = TREND_WINDOW
) -> bool:
    """CVD gate: positive bar delta OR an upward recent CVD trend."""
    return cvd_positive(candle) or cvd_trend_rising(cvd_series, window)


# ── Result + evaluation ───────────────────────────────────────────────────────
@dataclass
class ConfirmationResult:
    triggered: bool
    in_zone: bool
    pattern: Optional[str]
    cvd: bool
    reasons: List[str] = field(default_factory=list)
    trigger: Optional[Dict[str, Any]] = None


def build_trigger_payload(setup_name: str) -> Dict[str, Any]:
    """The exact WebSocket trigger payload the frontend expects."""
    return {"action": "trade_trigger", "status": "BUY_NOW", "setup_type": setup_name}


def evaluate_confirmation(
    setup_name: str,
    entry_price: float,
    ltp: float,
    candles: Sequence[Dict[str, Any]],
    *,
    zone_pct: float = ENTRY_ZONE_PCT,
    trend_window: int = TREND_WINDOW,
) -> ConfirmationResult:
    """Run the full dual-confirmation gate over the just-closed candle.

    ``candles`` is ordered oldest -> newest; ``candles[-1]`` is the just-closed
    candle and ``candles[-2]`` its predecessor. ``trigger`` is non-None only when
    every gate passes.
    """
    candles = list(candles or [])
    reasons: List[str] = []

    in_zone = within_entry_zone(ltp, entry_price, zone_pct)
    if in_zone:
        reasons.append("ltp_within_entry_zone")

    candle = candles[-1] if candles else None
    prev_candle = candles[-2] if len(candles) >= 2 else None

    pattern = has_bullish_pattern(candle, prev_candle) if isinstance(candle, dict) else None
    if pattern:
        reasons.append(f"pattern:{pattern}")

    cvd_ok = isinstance(candle, dict) and cvd_confirmed(
        candle, [c.get("cvd") for c in candles], trend_window
    )
    if cvd_ok:
        reasons.append("cvd_confirmed")

    triggered = in_zone and bool(pattern) and cvd_ok
    return ConfirmationResult(
        triggered=triggered,
        in_zone=in_zone,
        pattern=pattern,
        cvd=cvd_ok,
        reasons=reasons,
        trigger=build_trigger_payload(setup_name) if triggered else None,
    )


# ── WebSocket dispatch ────────────────────────────────────────────────────────
def dispatch_trade_trigger(trigger: Dict[str, Any]) -> bool:
    """Push a trade trigger to every browser in the ``ai_trade_setups`` group."""
    try:
        from channels.layers import get_channel_layer
        from asgiref.sync import async_to_sync
    except ImportError:
        logger.warning("Django Channels unavailable; trigger not dispatched: %s", trigger)
        return False

    layer = get_channel_layer()
    if layer is None:
        logger.warning("No channel layer configured; trigger not dispatched: %s", trigger)
        return False

    try:
        async_to_sync(layer.group_send)(
            AI_TRADE_GROUP, {"type": TRIGGER_MESSAGE_TYPE, "trigger": trigger}
        )
        return True
    except Exception:
        logger.exception("Failed to dispatch AI trade trigger")
        return False


# ── Stateful integration wrapper ──────────────────────────────────────────────
class CandleConfirmationEngine:
    """Rolling-candle wrapper that evaluates every closed candle and fires triggers.

    Register an instance as the CVD engine's ``on_candle`` callback (see module
    docstring). It keeps a bounded history so the engulfing comparison and the
    3-candle CVD trend always have context.
    """

    def __init__(
        self,
        setup_name: str,
        entry_price: float,
        *,
        symbol: Optional[str] = None,
        zone_pct: float = ENTRY_ZONE_PCT,
        trend_window: int = TREND_WINDOW,
        history_size: int = 8,
        dispatch: bool = True,
    ) -> None:
        self.setup_name = setup_name
        self.entry_price = float(entry_price)
        self.symbol = symbol
        self.zone_pct = zone_pct
        self.trend_window = trend_window
        self.should_dispatch = dispatch
        self._history: deque = deque(maxlen=max(3, int(history_size)))

    def on_candle(self, candle: Dict[str, Any]) -> Optional[ConfirmationResult]:
        """Handle a closed candle: evaluate + dispatch when all gates pass."""
        if not isinstance(candle, dict):
            return None
        if self.symbol and candle.get("symbol") != self.symbol:
            return None

        self._history.append(candle)
        result = evaluate_confirmation(
            self.setup_name,
            self.entry_price,
            candle.get("close"),
            list(self._history),
            zone_pct=self.zone_pct,
            trend_window=self.trend_window,
        )

        if result.triggered:
            logger.info(
                "AI trade trigger fired: setup=%s reasons=%s",
                self.setup_name,
                result.reasons,
            )
            if self.should_dispatch:
                dispatch_trade_trigger(result.trigger)

        return result
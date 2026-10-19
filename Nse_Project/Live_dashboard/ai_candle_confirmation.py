"""
1-Minute Candle Confirmation Module for AI Trade Setups (pattern-confirmed).

Implements a bidirectional confirmation gate that fires a ``BUY_NOW`` trade trigger
for the AI Trade Setups card. The direction is chosen by ``option_side``:

    * "CE" — BULLISH: Bullish Engulfing / Bullish Pin Bar.
    * "PE" — BEARISH: Bearish Engulfing / Shooting Star.

    1. ENTRY ZONE — LTP within ``ENTRY_ZONE_PCT`` (1.5%) of the AI entry price.
    2. CANDLE      — a directional reversal pattern matching ``option_side``.

CVD is recorded on the result for diagnostics but is ADVISORY ONLY — it no longer
gates the trigger (its feed can freeze when bid/ask data is missing).

When the gates pass, this payload is dispatched to the ``ai_trade_setups``
Channels group::

    {"action": "trade_trigger", "status": "BUY_NOW",
     "setup_type": "<setup_name>", "option_side": "<CE|PE>"}

Integration with the existing CVD engine (which already emits OHLC + ``delta`` +
``cvd`` per closed candle) is a one-liner::

    from Nse_Project.Brokers.cvd_engine import CVDEngine
    from Nse_Project.Live_dashboard.ai_candle_confirmation import CandleConfirmationEngine

    cvd = CVDEngine(candle_seconds=60, trend_periods=3, ...)   # 1-minute bars
    confirm = CandleConfirmationEngine("Momentum Pullback (15% Dip)", 24150.0, option_side="CE")
    cvd.on_candle = confirm.on_candle

The pure detectors are stdlib-only and safe to unit-test without Django.
"""

from __future__ import annotations

import json
import logging
import os
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
BEAR_PIN_BAR_WICK_RATIO = 2.0        # shooting-star upper wick >= 2x the real body
BEAR_PIN_BAR_UPPER_WICK_FRAC = 0.5   # upper wick >= 50% of the whole range
BEAR_PIN_BAR_LOWER_WICK_FRAC = 0.35  # lower wick <= 35% of the whole range

# Django Channels group + message type (must match the consumer + routing).
AI_TRADE_GROUP = "ai_trade_setups"
TRIGGER_MESSAGE_TYPE = "trade.trigger"
# Redis Pub/Sub the feed worker publishes trade triggers to. A bridge thread in
# ``Live_dashboard.consumers`` re-broadcasts them into the AI_TRADE_GROUP group.
AI_TRADE_REDIS_URL = os.getenv("AI_TRADE_REDIS_URL", "redis://127.0.0.1:6379/0")
AI_TRADE_REDIS_CHANNEL = os.getenv("AI_TRADE_REDIS_CHANNEL", "ai:trade_trigger")

__all__ = [
    "ConfirmationResult",
    "CandleConfirmationEngine",
    "AI_TRADE_GROUP",
    "within_entry_zone",
    "is_bullish_pin_bar",
    "is_bullish_engulfing",
    "has_bullish_pattern",
    "is_bearish_pin_bar",
    "is_bearish_engulfing",
    "has_bearish_pattern",
    "cvd_positive",
    "cvd_negative",
    "cvd_trend_rising",
    "cvd_trend_falling",
    "cvd_confirmed",
    "cvd_bear_confirmed",
    "evaluate_confirmation",
    "build_trigger_payload",
    "dispatch_trade_trigger",
    "is_no_trade_zone",
    "set_market_no_trade_probe",
    "market_in_no_trade_zone",
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


def _candle_ohlc(candle: Dict[str, Any]) -> Optional[tuple]:
    """Return ``(open, high, low, close)``, or ``None`` when fields are missing/invalid."""
    if not isinstance(candle, dict):
        return None
    o = _to_float(candle.get("open"))
    h = _to_float(candle.get("high"))
    l = _to_float(candle.get("low"))
    c = _to_float(candle.get("close"))
    if None in (o, h, l, c):
        return None
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
    ohlc = _candle_ohlc(candle)
    if ohlc is None:
        return False
    o, h, l, c = ohlc
    total_range = h - l
    if total_range <= 0:
        return False

    mn = min(o, c)
    mx = max(o, c)
    body = mx - mn
    lower_wick = mn - l
    upper_wick = h - mx

    long_lower = lower_wick >= wick_ratio * body and lower_wick >= lower_frac * total_range
    small_upper = upper_wick <= upper_frac * total_range
    closed_high = c >= (h + l) / 2.0
    return long_lower and small_upper and closed_high


def is_bullish_engulfing(candle: Dict[str, Any], prev_candle: Optional[Dict[str, Any]]) -> bool:
    """True when ``candle`` bullishly engulfs the previous (bearish) candle body."""
    if not isinstance(prev_candle, dict):
        return False
    curr = _candle_ohlc(candle)
    if curr is None:
        return False
    prev = _candle_ohlc(prev_candle)
    if prev is None:
        return False
    o, _h, _l, c = curr
    po, _ph, _pl, pc = prev

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


def is_bearish_pin_bar(
    candle: Dict[str, Any],
    *,
    wick_ratio: float = BEAR_PIN_BAR_WICK_RATIO,
    upper_frac: float = BEAR_PIN_BAR_UPPER_WICK_FRAC,
    lower_frac: float = BEAR_PIN_BAR_LOWER_WICK_FRAC,
) -> bool:
    """True when the candle is a bearish shooting star (long upper wick)."""
    ohlc = _candle_ohlc(candle)
    if ohlc is None:
        return False
    o, h, l, c = ohlc
    total_range = h - l
    if total_range <= 0:
        return False

    mn = min(o, c)
    mx = max(o, c)
    body = mx - mn
    upper_wick = h - mx
    lower_wick = mn - l

    long_upper = upper_wick >= wick_ratio * body and upper_wick >= upper_frac * total_range
    small_lower = lower_wick <= lower_frac * total_range
    closed_low = c <= (h + l) / 2.0
    return long_upper and small_lower and closed_low


def is_bearish_engulfing(candle: Dict[str, Any], prev_candle: Optional[Dict[str, Any]]) -> bool:
    """True when ``candle`` bearishly engulfs the previous (bullish) candle body."""
    if not isinstance(prev_candle, dict):
        return False
    curr = _candle_ohlc(candle)
    if curr is None:
        return False
    prev = _candle_ohlc(prev_candle)
    if prev is None:
        return False
    o, _h, _l, c = curr
    po, _ph, _pl, pc = prev

    prev_bullish = pc > po
    curr_bearish = c < o
    engulfs = o >= pc and c <= po and (o - c) > (pc - po)
    return prev_bullish and curr_bearish and engulfs


def has_bearish_pattern(
    candle: Dict[str, Any], prev_candle: Optional[Dict[str, Any]]
) -> Optional[str]:
    """Return the detected bearish pattern name, or ``None``."""
    if is_bearish_engulfing(candle, prev_candle):
        return "bearish_engulfing"
    if is_bearish_pin_bar(candle):
        return "bearish_pin_bar"
    return None


# ── Gate 3: CVD confirmation ──────────────────────────────────────────────────
def cvd_positive(candle: Dict[str, Any]) -> bool:
    """True when this candle's volume delta is positive (buyers dominated)."""
    if not isinstance(candle, dict):
        return False
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


def cvd_negative(candle: Dict[str, Any]) -> bool:
    """True when this candle's volume delta is negative (sellers dominated)."""
    if not isinstance(candle, dict):
        return False
    delta = _to_float(candle.get("delta"))
    return delta is not None and delta < 0


def cvd_trend_falling(cvd_series: Sequence, window: int = TREND_WINDOW) -> bool:
    """True when cumulative CVD is lower than it was ``window`` candles ago."""
    series = [v for v in (_to_float(v) for v in cvd_series) if v is not None]
    if len(series) < 2:
        return False
    lookback = min(window, len(series) - 1)
    return series[-1] < series[-1 - lookback]


def cvd_bear_confirmed(
    candle: Dict[str, Any], cvd_series: Sequence, window: int = TREND_WINDOW
) -> bool:
    """Bearish CVD gate: negative bar delta OR a downward recent CVD trend."""
    return cvd_negative(candle) or cvd_trend_falling(cvd_series, window)


# ── Result + evaluation ───────────────────────────────────────────────────────
@dataclass
class ConfirmationResult:
    triggered: bool
    in_zone: bool
    pattern: Optional[str]
    cvd: bool
    side: str = "CE"
    reasons: List[str] = field(default_factory=list)
    trigger: Optional[Dict[str, Any]] = None


def is_no_trade_zone(score: Any, momentum: Any, net_oi_bias: Any) -> bool:
    """Mirror of the frontend tri-state in ``live_signals.html#updateAITrades``.

    Returns True when the score / momentum / whole-chain Net-OI bias land in the
    same dead-band the dashboard renders as ``🚫 NO TRADE ZONE``. Thresholds are
    kept in lock-step with the frontend:
      * bullish  = score > 55
      * bearish  = score < 45, or (net_oi_bias < 0 and score < 50)
      * middle-zone tie-break: momentum > +20 -> bullish, momentum < -20 -> bearish
      * otherwise -> NO TRADE ZONE.
    """
    def _num(value: Any, default: float) -> float:
        if value is None:
            return default
        try:
            parsed = float(value)
        except (TypeError, ValueError):
            return default
        if parsed != parsed or parsed in (float("inf"), float("-inf")):
            return default
        return parsed

    score = _num(score, 50.0)
    momentum = _num(momentum, 0.0)
    macro_bias = _num(net_oi_bias, 0.0)

    bullish = score > 55
    bearish = (score < 45) or (macro_bias < 0 and score < 50)

    if not bullish and not bearish:
        bullish = momentum > 20
        bearish = momentum < -20

    return not bullish and not bearish


def build_trigger_payload(setup_name: str, option_side: str = "CE") -> Dict[str, Any]:
    """The exact WebSocket trigger payload the frontend expects."""
    return {
        "action": "trade_trigger",
        "status": "BUY_NOW",
        "setup_type": setup_name,
        "option_side": (option_side or "CE").upper(),
    }


def evaluate_confirmation(
    setup_name: str,
    entry_price: float,
    ltp: float,
    candles: Sequence[Dict[str, Any]],
    *,
    zone_pct: float = ENTRY_ZONE_PCT,
    trend_window: int = TREND_WINDOW,
    option_side: str = "CE",
    no_trade_zone: bool = False,
) -> ConfirmationResult:
    """Run the confirmation gate over the just-closed candle.

    ``candles`` is ordered oldest -> newest; ``candles[-1]`` is the just-closed
    candle and ``candles[-2]`` its predecessor. ``option_side`` selects the pattern
    direction: ``"CE"`` requires a BULLISH pattern, ``"PE"`` requires a BEARISH
    pattern. CVD is recorded for diagnostics but is ADVISORY only (its feed can
    freeze), so it no longer gates the trigger. ``trigger`` is non-None when the
    entry-zone and pattern gates pass.
    """
    candles = list(candles or [])
    side = (option_side or "CE").upper()
    reasons: List[str] = []

    in_zone = within_entry_zone(ltp, entry_price, zone_pct)
    if in_zone:
        reasons.append("ltp_within_entry_zone")

    candle = candles[-1] if candles else None
    prev_candle = candles[-2] if len(candles) >= 2 else None

    if side == "PE":
        pattern = has_bearish_pattern(candle, prev_candle) if isinstance(candle, dict) else None
    else:
        pattern = has_bullish_pattern(candle, prev_candle) if isinstance(candle, dict) else None
    if pattern:
        reasons.append(f"pattern:{pattern}")

    cvd_series = [c.get("cvd") if isinstance(c, dict) else None for c in candles]
    if side == "PE":
        cvd_ok = isinstance(candle, dict) and cvd_bear_confirmed(candle, cvd_series, trend_window)
    else:
        cvd_ok = isinstance(candle, dict) and cvd_confirmed(candle, cvd_series, trend_window)
    if cvd_ok:
        reasons.append("cvd_confirmed")

    # CVD is ADVISORY only: a frozen CVD feed (missing bid/ask) must never block a
    # directional candle confirmation, so the trigger now ignores it entirely.
    triggered = in_zone and bool(pattern)
    # NO TRADE ZONE gate: when the frontend tri-state would render "NO TRADE ZONE",
    # the BUY_NOW trigger must NOT be published at all — suppress it here.
    if triggered and no_trade_zone:
        reasons.append("suppressed:no_trade_zone")
        triggered = False
    return ConfirmationResult(
        triggered=triggered,
        in_zone=in_zone,
        pattern=pattern,
        cvd=cvd_ok,
        side=side,
        reasons=reasons,
        trigger=build_trigger_payload(setup_name, option_side=side) if triggered else None,
    )


# ── WebSocket dispatch ────────────────────────────────────────────────────────
# Lazily built, process-wide Redis client for trade-trigger dispatch. Building a new
# client (and its connection pool) on every trigger leaked a socket per publish and
# paid a fresh connect each time; reusing one client removes both.
_ai_trade_redis_client = None


def _get_trade_redis_client():
    """Return the cached trade-trigger Redis client, or None when redis-py is absent."""
    global _ai_trade_redis_client
    if _ai_trade_redis_client is None:
        try:
            import redis as _redis
        except ImportError:
            return None
        try:
            _ai_trade_redis_client = _redis.Redis.from_url(
                AI_TRADE_REDIS_URL,
                decode_responses=True,
                socket_connect_timeout=1,
                socket_timeout=1,
            )
        except Exception:
            logger.exception("Could not build the AI trade-trigger Redis client.")
            return None
    return _ai_trade_redis_client


def dispatch_trade_trigger(trigger: Dict[str, Any]) -> bool:
    """Publish a trade trigger for the AI Trade Setups card.

    The feed worker (``upstox_live_feed.py``) is a STANDALONE process, so it cannot
    use Django Channels' in-process ``group_send`` (``get_channel_layer`` would
    raise ``ImproperlyConfigured`` there). It instead publishes to a raw Redis
    Pub/Sub channel; a bridge thread inside the Django ASGI process
    (``Live_dashboard.consumers``) re-broadcasts the payload into the
    ``ai_trade_setups`` group, which ``AITradeTriggerConsumer`` forwards to every
    connected browser as ``{action: trade_trigger, status: BUY_NOW, ...}``.
    """
    client = _get_trade_redis_client()
    if client is None:
        logger.warning("redis-py unavailable; trigger not dispatched: %s", trigger)
        return False
    try:
        client.publish(AI_TRADE_REDIS_CHANNEL, json.dumps(trigger, default=str))
        return True
    except Exception:
        logger.exception("Failed to dispatch AI trade trigger")
        return False


# ── Live no-trade gate (injected by the feed worker) ──────────────────────────
# The feed worker (upstox_live_feed.py) owns the authoritative score/momentum/
# net-OI-bias and injects a probe here so evaluate_confirmation() can suppress
# BUY_NOW triggers while the dashboard is in a NO TRADE ZONE. The probe is
# intentionally optional: without it the engine behaves exactly as before.
_market_no_trade_probe = None


def set_market_no_trade_probe(probe) -> None:
    """Inject the feed worker's ``() -> bool`` no-trade-zone probe (idempotent)."""
    global _market_no_trade_probe
    _market_no_trade_probe = probe


def market_in_no_trade_zone() -> bool:
    """Return True when the injected probe reports the market is in a no-trade zone."""
    probe = _market_no_trade_probe
    if probe is None:
        return False
    try:
        return bool(probe())
    except Exception:
        logger.exception("Market no-trade probe failed; defaulting to NOT suppressing.")
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
        option_side: str = "CE",
    ) -> None:
        self.setup_name = setup_name
        self.entry_price = float(entry_price)
        self.symbol = symbol
        self.zone_pct = zone_pct
        self.trend_window = trend_window
        self.should_dispatch = dispatch
        self.option_side = (option_side or "CE").upper()
        self._history: deque = deque(maxlen=max(3, int(history_size)))

    def update_entry(self, entry_price: float, *, option_side: Optional[str] = None) -> None:
        """Refresh the entry reference WITHOUT discarding the rolling candle history.

        The feed re-arms engines on ATM strike rolls / side flips; recreating the
        engine there would empty ``_history`` and starve the engulfing + 3-candle
        CVD-trend gates of their ``prev_candle`` context.
        """
        self.entry_price = float(entry_price)
        if option_side:
            self.option_side = (option_side or "CE").upper()

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
            option_side=self.option_side,
            no_trade_zone=market_in_no_trade_zone(),
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
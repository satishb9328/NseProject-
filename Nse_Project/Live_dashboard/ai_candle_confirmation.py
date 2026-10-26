"""
1-Minute Candle Confirmation Module for AI Trade Setups (pattern-confirmed).

Implements a bidirectional confirmation gate that fires a ``BUY_NOW`` trade trigger
for the AI Trade Setups card. The direction is chosen by ``option_side``:

    * "CE" — BULLISH: Morning Star / Tweezer Bottoms / Bullish Engulfing / Hammer.
    * "PE" — BEARISH: Evening Star / Tweezer Tops / Hanging Man / Bearish Engulfing /
      Shooting Star.

    1. ENTRY ZONE — LTP within an ATR-derived buffer (``ATR * ATR_MULTIPLIER``),
       falling back to ``ENTRY_ZONE_PCT`` (1.5%) when < ``ATR_PERIOD`` candles exist.
    2. CANDLE      — a directional reversal pattern matching ``option_side``.
    3. MTF 2-MIN   — the synthesized 2-minute candle must agree with ``option_side``.

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

import atexit
import json
import logging
import os
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Sequence

logger = logging.getLogger(__name__)

# ── Tuning constants (env-overridable for production tuning without redeploy) ─
ENTRY_ZONE_PCT = float(os.getenv("AI_TRADE_ENTRY_ZONE_PCT", "0.015"))
TREND_WINDOW = int(os.getenv("AI_TRADE_TREND_WINDOW", "3"))
PIN_BAR_WICK_RATIO = float(os.getenv("AI_TRADE_PIN_BAR_WICK_RATIO", "2.0"))
PIN_BAR_LOWER_WICK_FRAC = float(os.getenv("AI_TRADE_PIN_BAR_LOWER_WICK_FRAC", "0.5"))
PIN_BAR_UPPER_WICK_FRAC = float(os.getenv("AI_TRADE_PIN_BAR_UPPER_WICK_FRAC", "0.35"))
BEAR_PIN_BAR_WICK_RATIO = float(os.getenv("AI_TRADE_BEAR_PIN_BAR_WICK_RATIO", "2.0"))
BEAR_PIN_BAR_UPPER_WICK_FRAC = float(os.getenv("AI_TRADE_BEAR_PIN_BAR_UPPER_WICK_FRAC", "0.5"))
BEAR_PIN_BAR_LOWER_WICK_FRAC = float(os.getenv("AI_TRADE_BEAR_PIN_BAR_LOWER_WICK_FRAC", "0.35"))

# ── Dynamic ATR entry zone ────────────────────────────────────────────────────
ATR_PERIOD = int(os.getenv("AI_TRADE_ATR_PERIOD", "14"))
ATR_MULTIPLIER = float(os.getenv("AI_TRADE_ATR_MULT", "1.0"))

# ── Multi-timeframe (2-minute sync) ───────────────────────────────────────────
MTF_SYNTH_PERIOD = int(os.getenv("AI_TRADE_MTF_SYNTH_PERIOD", "2"))
# ARCHITECTURAL UPGRADE: the original MTF filter only checked the synthesized
# 2m candle's close-vs-open direction. A 2m candle that spiked up then crashed
# to just-above-open would still "confirm" a CE trade - false signal. Now the
# synth must ALSO have a real body (>= MTF_MIN_BODY_FRAC of its total range)
# before the direction is even checked. 0.15 filters out doji-heavy
# consolidations while not rejecting tight-but-real trend candles.
MTF_MIN_BODY_FRAC = float(os.getenv("AI_TRADE_MTF_MIN_BODY_FRAC", "0.15"))

# ── Setup auto-expiry ────────────────────────────────────────────────────────
# ARCHITECTURAL UPGRADE: a setup generated N seconds ago is considered stale -
# LTP has drifted, IV has shifted, the original edge is gone. Suppressing the
# trigger after this window avoids chasing a faded signal. 15 minutes matches
# a typical intraday momentum horizon; set to 0 (or None on the engine) to
# disable. The clock is refreshed on ``update_entry`` so a re-arm (ATM strike
# roll, side flip) resets the window.
SETUP_EXPIRY_SECONDS = float(os.getenv("AI_TRADE_SETUP_EXPIRY_SECONDS", "900"))

# ── Additional candlestick-pattern tolerances ─────────────────────────────────
TWEEZER_TOLERANCE = float(os.getenv("AI_TRADE_TWEEZER_TOLERANCE", "0.001"))
HANGING_MAN_BODY_FRAC = float(os.getenv("AI_TRADE_HANGING_MAN_BODY_FRAC", "0.3"))
# FIX: was 0.5, too loose - a "star" could be half the size of the prior candle
# (a continuation candle, not a star). Textbook star body is < 30% of the prior
# body.
STAR_BODY_FRAC = float(os.getenv("AI_TRADE_STAR_BODY_FRAC", "0.3"))

# ── Dispatch retry / durable-log config ──────────────────────────────────────
DISPATCH_MAX_RETRIES = int(os.getenv("AI_TRADE_DISPATCH_RETRIES", "3"))
DISPATCH_BACKOFF_BASE = float(os.getenv("AI_TRADE_DISPATCH_BACKOFF", "0.1"))
DISPATCH_DURABLE_KEY = os.getenv("AI_TRADE_DURABLE_KEY", "ai:trade_trigger_log")
DISPATCH_DURABLE_MAX = int(os.getenv("AI_TRADE_DURABLE_MAX", "1000"))

# Django Channels group (must match the consumer + routing).
AI_TRADE_GROUP = "ai_trade_setups"
# REMOVED: TRIGGER_MESSAGE_TYPE = "trade.trigger" - dead export, never used.
# Redis Pub/Sub the feed worker publishes trade triggers to. A bridge thread in
# ``Live_dashboard.consumers`` re-broadcasts them into the AI_TRADE_GROUP group.
REDIS_URL = os.getenv('REDIS_URL', 'redis://127.0.0.1:6379/0')
AI_TRADE_REDIS_CHANNEL = os.getenv("AI_TRADE_REDIS_CHANNEL", "ai:trade_trigger")

# ── Timezone (consistent with gamma_blast_engine) ────────────────────────────
# Trigger payloads now carry an ISO timestamp in LOCAL_TZ so the dashboard can
# show when the BUY_NOW fired regardless of the host's wall clock.
_DEFAULT_TZ_NAME = os.getenv("AI_TRADE_TIMEZONE", "Asia/Kolkata").strip() or "Asia/Kolkata"
try:
    from zoneinfo import ZoneInfo  # Python 3.9+
    LOCAL_TZ = ZoneInfo(_DEFAULT_TZ_NAME)
except (ImportError, ValueError, KeyError):
    LOCAL_TZ = timezone(timedelta(hours=5, minutes=30), name="IST")

__all__ = [
    "ConfirmationResult",
    "CandleConfirmationEngine",
    "AI_TRADE_GROUP",
    "within_entry_zone",
    "within_atr_zone",
    "calculate_atr",
    "synthesize_2m_candle",
    "mtf_2m_confirmed",
    "is_bullish_pin_bar",
    "is_bullish_engulfing",
    "is_morning_star",
    "is_tweezer_bottoms",
    "has_bullish_pattern",
    "is_bearish_pin_bar",
    "is_bearish_engulfing",
    "is_evening_star",
    "is_tweezer_tops",
    "is_hanging_man",
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
    """Return ``(open, high, low, close)``, or ``None`` when fields are missing/invalid.

    FIX vs original: now validates the OHLC ordering (low <= open <= high,
    low <= close <= high, low <= high). A malformed candle where ``low > high``
    (a feed glitch, partial write, or instrument-token collision) used to pass
    through silently and corrupt every downstream pattern detector (negative
    total_range, impossible wick values, etc.).
    """
    if not isinstance(candle, dict):
        return None
    o = _to_float(candle.get("open"))
    h = _to_float(candle.get("high"))
    l = _to_float(candle.get("low"))
    c = _to_float(candle.get("close"))
    if None in (o, h, l, c):
        return None
    # Sanity: low must be the minimum, high the maximum.
    if not (l <= h and l <= o <= h and l <= c <= h):
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


def within_atr_zone(ltp, entry_price, atr_buffer) -> bool:
    """True when ``ltp`` is within ``atr_buffer`` (absolute price) of ``entry_price``."""
    ltp_f = _to_float(ltp)
    entry_f = _to_float(entry_price)
    if ltp_f is None or entry_f is None or entry_f <= 0:
        return False
    buffer = _to_float(atr_buffer)
    if buffer is None or buffer < 0:
        return False
    return abs(ltp_f - entry_f) <= buffer


def calculate_atr(
    candles: Sequence[Dict[str, Any]], period: int = ATR_PERIOD
) -> Optional[float]:
    """Standard ``period``-period (Wilder) Average True Range over ``candles``.

    ``candles`` is ordered oldest -> newest and each candle needs ``high``, ``low``
    and ``close``. Returns ``None`` when fewer than ``period`` valid OHLC candles
    exist, so callers can gracefully fall back to the default percentage zone.

    FIX vs original: each True Range is now clamped to ``max(0, ...)``. A
    malformed candle that slipped past ``_candle_ohlc`` (e.g., ``h < l`` from a
    feed glitch) used to produce a negative TR, which would silently corrupt the
    Wilder smoothing for every subsequent ATR. Now non-negative by construction.
    """
    period = max(1, int(period))
    ohlcs = [
        ohlc
        for ohlc in (_candle_ohlc(c) for c in (candles or []) if isinstance(c, dict))
        if ohlc is not None
    ]
    if len(ohlcs) < period:
        return None

    trs: List[float] = []
    prev_close: Optional[float] = None
    for _o, h, l, c in ohlcs:
        if prev_close is None:
            tr = max(0.0, h - l)
        else:
            tr = max(0.0, h - l, abs(h - prev_close), abs(l - prev_close))
        trs.append(tr)
        prev_close = c

    atr = sum(trs[:period]) / float(period)
    for tr in trs[period:]:
        atr = (atr * (period - 1) + tr) / float(period)
    return atr


# ── Gate 2b: 2-minute MTF trend filter ────────────────────────────────────────
def synthesize_2m_candle(c1: Dict[str, Any], c2: Dict[str, Any]) -> Optional[tuple]:
    """Merge two 1-minute candles (``c2`` older, ``c1`` newer) into one 2-minute OHLC.

    Returns ``(open, high, low, close)`` or ``None`` when either candle is malformed.
    """
    ohlc1 = _candle_ohlc(c1)
    ohlc2 = _candle_ohlc(c2)
    if ohlc1 is None or ohlc2 is None:
        return None
    _o1, h1, l1, c1c = ohlc1
    o2, h2, l2, _c2 = ohlc2
    return o2, max(h1, h2), min(l1, l2), c1c


def mtf_2m_confirmed(
    candles: Sequence[Dict[str, Any]], option_side: str = "CE"
) -> bool:
    """2-minute trend filter: the synthesized 2m candle must match ``option_side``.

    The synth merges ``candles[-2]`` (older) and ``candles[-1]`` (newer), so a CE
    signal requires ``newer_close > older_open`` and a PE signal requires
    ``newer_close < older_open``.

    FIX vs original: returns ``False`` when fewer than two candles exist. The
    original returned ``True`` (filter skipped) on the very first 1-minute
    candle, which let a stale feed resuming fire a trigger on a single noisy
    candle without any MTF confirmation. For a real-money trade trigger that's
    risky; now the gate refuses until it has a real 2-minute pair to evaluate.

    ARCHITECTURAL UPGRADE: the original only checked ``c > o`` (CE) or ``c < o``
    (PE). A 2m candle that spiked up then crashed to just-above-open would
    still pass for CE - directional close but no real trend conviction. Now the
    synth must ALSO have a body of at least ``MTF_MIN_BODY_FRAC`` of its total
    range, so doji-heavy consolidations can't confirm a trade. This eliminates
    the classic "whipsaw confirmation" where price spikes 30pts up then
    retraces 28pts but the 2m close is still 2pts above open.
    """
    candles = list(candles or [])
    if len(candles) < MTF_SYNTH_PERIOD:
        return False
    synth = synthesize_2m_candle(candles[-1], candles[-2])
    if synth is None:
        return False
    # Unpack h, l too - the original left them as _h, _l (unused).
    o, h, l, c = synth
    total_range = h - l
    if total_range <= 0:
        # Zero-range 2m candle (rare but possible on illiquid instruments)
        # has no directional information - refuse rather than divide by zero.
        return False
    body = abs(c - o)
    if body < MTF_MIN_BODY_FRAC * total_range:
        # Body too small relative to range - this is a doji or near-doji 2m
        # candle, not a trend confirmation.
        return False
    side = (option_side or "CE").upper()
    return c < o if side == "PE" else c > o


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
    # FIX: a doji (body == 0) is NOT a pin bar. The original used
    # ``lower_wick >= wick_ratio * body`` which trivially passed when body == 0
    # (RHS == 0), letting any doji with any lower wick count as a hammer.
    if body <= 0:
        return False

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


def _near_equal(a: float, b: float, tolerance: float = TWEEZER_TOLERANCE) -> bool:
    """True when two prices match within ``tolerance`` (relative to their magnitude)."""
    return abs(a - b) <= tolerance * max(abs(a), abs(b), 1e-9)


def is_morning_star(candles: Sequence[Dict[str, Any]]) -> bool:
    """True for a 3-candle Morning Star: bearish -> small star gap-down -> bullish.

    The third candle must close above the midpoint of the first candle's body.
    """
    if len(candles) < 3:
        return False
    first = _candle_ohlc(candles[-3])
    star = _candle_ohlc(candles[-2])
    third = _candle_ohlc(candles[-1])
    if None in (first, star, third):
        return False
    o1, _h1, _l1, c1 = first
    o2, _h2, _l2, c2 = star
    o3, _h3, _l3, c3 = third

    if not (c1 < o1 and c3 > o3):
        return False
    body1 = abs(c1 - o1)
    body2 = abs(c2 - o2)
    if body1 <= 0 or body2 <= 0:
        return False
    star_small = body2 <= STAR_BODY_FRAC * body1
    star_gap_down = max(o2, c2) < min(o1, c1)
    third_reclaims = c3 > (o1 + c1) / 2.0
    return star_small and star_gap_down and third_reclaims


def is_tweezer_bottoms(candles: Sequence[Dict[str, Any]]) -> bool:
    """True for a 2-candle Tweezer Bottom: matching lows + second candle bullish."""
    if len(candles) < 2:
        return False
    first = _candle_ohlc(candles[-2])
    second = _candle_ohlc(candles[-1])
    if first is None or second is None:
        return False
    _o1, _h1, l1, _c1 = first
    o2, _h2, l2, c2 = second
    return _near_equal(l1, l2) and c2 > o2


def has_bullish_pattern(
    candle: Dict[str, Any],
    prev_candle: Optional[Dict[str, Any]],
    candles: Optional[Sequence[Dict[str, Any]]] = None,
) -> Optional[str]:
    """Return the detected bullish pattern name, or ``None``.

    ``candles`` (oldest -> newest) supplies the multi-candle context for Morning
    Star / Tweezer Bottoms; it defaults to ``[prev_candle, candle]`` when omitted
    so legacy two-argument callers keep working.
    """
    ctx = list(candles) if candles else []
    if not ctx:
        ctx = [prev_candle, candle] if isinstance(prev_candle, dict) else ([candle] if isinstance(candle, dict) else [])
    if is_morning_star(ctx):
        return "morning_star"
    if is_tweezer_bottoms(ctx):
        return "tweezer_bottoms"
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
    # FIX: a doji (body == 0) is NOT a shooting star. Same reasoning as the
    # bullish pin bar - the wick_ratio test trivially passed when body == 0.
    if body <= 0:
        return False

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


def is_evening_star(candles: Sequence[Dict[str, Any]]) -> bool:
    """True for a 3-candle Evening Star: bullish -> small star gap-up -> bearish.

    The third candle must close below the midpoint of the first candle's body.
    """
    if len(candles) < 3:
        return False
    first = _candle_ohlc(candles[-3])
    star = _candle_ohlc(candles[-2])
    third = _candle_ohlc(candles[-1])
    if None in (first, star, third):
        return False
    o1, _h1, _l1, c1 = first
    o2, _h2, _l2, c2 = star
    o3, _h3, _l3, c3 = third

    if not (c1 > o1 and c3 < o3):
        return False
    body1 = abs(c1 - o1)
    body2 = abs(c2 - o2)
    if body1 <= 0 or body2 <= 0:
        return False
    star_small = body2 <= STAR_BODY_FRAC * body1
    star_gap_up = min(o2, c2) > max(o1, c1)
    third_reclaims = c3 < (o1 + c1) / 2.0
    return star_small and star_gap_up and third_reclaims


def is_tweezer_tops(candles: Sequence[Dict[str, Any]]) -> bool:
    """True for a 2-candle Tweezer Top: matching highs + second candle bearish."""
    if len(candles) < 2:
        return False
    first = _candle_ohlc(candles[-2])
    second = _candle_ohlc(candles[-1])
    if first is None or second is None:
        return False
    _o1, h1, _l1, _c1 = first
    o2, h2, _l2, c2 = second
    return _near_equal(h1, h2) and c2 < o2


def is_hanging_man(
    candle: Dict[str, Any],
    prev_candle: Dict[str, Any],
) -> bool:
    """True for a 1-candle Hanging Man evaluated after an up-move.

    The candle is hammer-shaped (small upper body, long lower wick) but appears at
    the top of an uptrend (``prev_candle`` bullish), which is what makes it a
    bearish reversal setup in the PE context.

    FIX vs original: ``prev_candle`` is now REQUIRED (was Optional with default
    None). Without it the function silently degenerated to "any hammer-shaped
    candle" - which is a bullish hammer, not a bearish hanging man. A direct
    caller doing ``is_hanging_man(candle)`` would get a wrong-direction signal.
    """
    if not isinstance(prev_candle, dict):
        return False
    prev = _candle_ohlc(prev_candle)
    if prev is None or prev[3] <= prev[0]:
        return False  # prev must be bullish (uptrend) - this is the defining context
    ohlc = _candle_ohlc(candle)
    if ohlc is None:
        return False
    o, h, l, c = ohlc
    total_range = h - l
    if total_range <= 0:
        return False
    body = abs(c - o)
    lower_wick = min(o, c) - l
    upper_wick = h - max(o, c)
    small_body = body <= HANGING_MAN_BODY_FRAC * total_range
    long_lower = lower_wick >= PIN_BAR_WICK_RATIO * body and lower_wick >= PIN_BAR_LOWER_WICK_FRAC * total_range
    small_upper = upper_wick <= PIN_BAR_UPPER_WICK_FRAC * total_range
    return small_body and long_lower and small_upper


def has_bearish_pattern(
    candle: Dict[str, Any],
    prev_candle: Optional[Dict[str, Any]],
    candles: Optional[Sequence[Dict[str, Any]]] = None,
) -> Optional[str]:
    """Return the detected bearish pattern name, or ``None``.

    ``candles`` (oldest -> newest) supplies the multi-candle context for Evening
    Star / Tweezer Tops; it defaults to ``[prev_candle, candle]`` when omitted so
    legacy two-argument callers keep working.
    """
    ctx = list(candles) if candles else []
    if not ctx:
        ctx = [prev_candle, candle] if isinstance(prev_candle, dict) else ([candle] if isinstance(candle, dict) else [])
    if is_evening_star(ctx):
        return "evening_star"
    if is_tweezer_tops(ctx):
        return "tweezer_tops"
    if is_bearish_engulfing(candle, prev_candle):
        return "bearish_engulfing"
    if is_hanging_man(candle, prev_candle):
        return "hanging_man"
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
    # Latest 14-period ATR used to build the dynamic entry zone (None when the
    # candle history is too short to compute it).
    atr: Optional[float] = None


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


def build_trigger_payload(
    setup_name: str,
    option_side: str = "CE",
    *,
    entry_price: Optional[float] = None,
    ltp: Optional[float] = None,
    atr: Optional[float] = None,
    pattern: Optional[str] = None,
    symbol: Optional[str] = None,
    reasons: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """The WebSocket trigger payload the frontend expects, with full context.

    FIX vs original: the payload used to ship only ``action``/``status``/
    ``setup_type``/``option_side`` - the dashboard got a BUY_NOW with no idea
    at what price, what pattern fired, when, or which symbol. Now includes
    ``timestamp`` (tz-aware), ``symbol``, ``entry_price``, ``ltp``, ``atr``,
    ``pattern``, and ``reasons`` so the frontend can render the full context.
    """
    payload: Dict[str, Any] = {
        "action": "trade_trigger",
        "status": "BUY_NOW",
        "setup_type": setup_name,
        "option_side": (option_side or "CE").upper(),
        "timestamp": datetime.now(LOCAL_TZ).isoformat(timespec="seconds"),
        "reasons": list(reasons or []),
    }
    if symbol is not None:
        payload["symbol"] = symbol
    if entry_price is not None:
        payload["entry_price"] = entry_price
    if ltp is not None:
        payload["ltp"] = ltp
    if atr is not None:
        payload["atr"] = atr
    if pattern is not None:
        payload["pattern"] = pattern
    return payload


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
    atr_period: int = ATR_PERIOD,
    atr_multiplier: float = ATR_MULTIPLIER,
    symbol: Optional[str] = None,
    setup_age_seconds: Optional[float] = None,
    setup_expiry_seconds: Optional[float] = SETUP_EXPIRY_SECONDS,
) -> ConfirmationResult:
    """Run the confirmation gate over the just-closed candle.

    ``candles`` is ordered oldest -> newest; ``candles[-1]`` is the just-closed
    candle and ``candles[-2]`` its predecessor. ``option_side`` selects the pattern
    direction: ``"CE"`` requires a BULLISH pattern, ``"PE"`` requires a BEARISH
    pattern. CVD is recorded for diagnostics but is ADVISORY only (its feed can
    freeze), so it no longer gates the trigger. Gates that must ALL pass:

      0. setup not expired (``setup_age_seconds`` < ``setup_expiry_seconds``)
      1. dynamic ATR entry zone (fallback ``zone_pct`` when ATR is unavailable)
      2. a directional reversal pattern matching ``option_side``
      3. the synthesized 2-minute candle agreeing with ``option_side``
         (now with a real-body requirement, see ``mtf_2m_confirmed``)

    ``trigger`` is non-None when all gates pass (and no-trade-zone is clear).

    FIX vs original: every gate now appends a reason (pass OR fail) to
    ``reasons``, so debugging a refused trigger no longer requires guessing which
    gate blocked it. ``symbol`` is forwarded into the trigger payload.

    ARCHITECTURAL UPGRADE: Gate 0 (setup expiry). A setup generated more than
    ``setup_expiry_seconds`` ago is considered stale - LTP has drifted, IV has
    shifted, the original edge is gone. Pass ``setup_age_seconds`` to evaluate
    it here; the ``CandleConfirmationEngine`` wrapper computes it from
    ``setup_created_at`` and refreshes it on ``update_entry``. Set
    ``setup_expiry_seconds=None`` (or 0) to disable the gate entirely.
    """
    candles = list(candles or [])
    side = (option_side or "CE").upper()
    reasons: List[str] = []

    # Gate 0: setup expiry. Short-circuit BEFORE the other gates run, because
    # a stale setup shouldn't even attempt to confirm - the LTP/IV context that
    # produced the entry_price is gone. ``setup_age_seconds=None`` skips the
    # gate (used by stateless callers who don't track creation time).
    if (
        setup_age_seconds is not None
        and setup_expiry_seconds is not None
        and setup_expiry_seconds > 0
        and setup_age_seconds > setup_expiry_seconds
    ):
        reasons.append(
            f"suppressed:setup_expired({int(setup_age_seconds)}s > "
            f"{int(setup_expiry_seconds)}s)"
        )
        return ConfirmationResult(
            triggered=False,
            in_zone=False,
            pattern=None,
            cvd=False,
            side=side,
            reasons=reasons,
            trigger=None,
            atr=None,
        )

    # Gate 1: dynamic ATR entry zone, falling back to the legacy percentage zone.
    atr = calculate_atr(candles, atr_period)
    if atr is not None and atr > 0:
        in_zone = within_atr_zone(ltp, entry_price, atr * atr_multiplier)
        # FIX: append a reason whether the gate passed OR failed so the
        # caller can debug a refused trigger from the reasons list alone.
        reasons.append("ltp_within_atr_zone" if in_zone else "ltp_outside_atr_zone")
    else:
        in_zone = within_entry_zone(ltp, entry_price, zone_pct)
        reasons.append("ltp_within_entry_zone" if in_zone else "ltp_outside_entry_zone")

    candle = candles[-1] if candles else None
    prev_candle = candles[-2] if len(candles) >= 2 else None

    if side == "PE":
        pattern = has_bearish_pattern(candle, prev_candle, candles) if isinstance(candle, dict) else None
    else:
        pattern = has_bullish_pattern(candle, prev_candle, candles) if isinstance(candle, dict) else None
    reasons.append(f"pattern:{pattern}" if pattern else "pattern:none")

    # Gate 3: 2-minute trend confirmation (MTF sync). Now requires a real body
    # on the synthesized 2m candle, not just a directional close.
    mtf_ok = mtf_2m_confirmed(candles, side)
    reasons.append("mtf_2m_confirmed" if mtf_ok else "mtf_2m_unconfirmed")

    cvd_series = [c.get("cvd") if isinstance(c, dict) else None for c in candles]
    if side == "PE":
        cvd_ok = isinstance(candle, dict) and cvd_bear_confirmed(candle, cvd_series, trend_window)
    else:
        cvd_ok = isinstance(candle, dict) and cvd_confirmed(candle, cvd_series, trend_window)
    if cvd_ok:
        reasons.append("cvd_confirmed")

    # CVD is ADVISORY only: a frozen CVD feed (missing bid/ask) must never block a
    # directional candle confirmation, so the trigger now ignores it entirely.
    # The 2-minute MTF filter IS a hard gate (like the entry zone + pattern).
    triggered = in_zone and bool(pattern) and mtf_ok
    # NO TRADE ZONE gate: when the frontend tri-state would render "NO TRADE ZONE",
    # the BUY_NOW trigger must NOT be published at all — suppress it here.
    if triggered and no_trade_zone:
        reasons.append("suppressed:no_trade_zone")
        triggered = False

    ltp_f = _to_float(ltp)
    entry_f = _to_float(entry_price)
    return ConfirmationResult(
        triggered=triggered,
        in_zone=in_zone,
        pattern=pattern,
        cvd=cvd_ok,
        side=side,
        reasons=reasons,
        trigger=build_trigger_payload(
            setup_name,
            option_side=side,
            entry_price=entry_f if entry_f and entry_f > 0 else None,
            ltp=ltp_f,
            atr=atr,
            pattern=pattern,
            symbol=symbol,
            reasons=reasons,
        ) if triggered else None,
        atr=atr,
    )


# ── WebSocket dispatch ────────────────────────────────────────────────────────
# Lazily built, process-wide Redis client for trade-trigger dispatch. Building a new
# client (and its connection pool) on every trigger leaked a socket per publish and
# paid a fresh connect each time; reusing one client removes both. A Lock +
# double-check guards the build so concurrent ASGI workers can't create two clients
# and orphan one (socket-pool leak).
_ai_trade_redis_client = None
_ai_trade_redis_lock = threading.Lock()


def _get_trade_redis_client():
    """Return the cached trade-trigger Redis client, or None when redis-py is absent.

    FIX vs original: race condition. Two threads could both see ``_ai_trade_redis_client
    is None``, both build a client, and one would be orphaned (socket pool leak,
    FD creep). Now guarded by a Lock + double-check. Also wires
    ``health_check_interval=30`` for auto-reconnect on dead sockets.
    """
    global _ai_trade_redis_client
    if _ai_trade_redis_client is not None:
        return _ai_trade_redis_client
    with _ai_trade_redis_lock:
        if _ai_trade_redis_client is not None:  # double-check after acquiring the lock
            return _ai_trade_redis_client
        try:
            import redis as _redis
        except ImportError:
            return None
        try:
            _ai_trade_redis_client = _redis.Redis.from_url(
                REDIS_URL,
                decode_responses=True,
                socket_connect_timeout=1,
                socket_timeout=1,
                health_check_interval=30,
            )
        except Exception:
            logger.exception("Could not build the AI trade-trigger Redis client.")
            return None
    return _ai_trade_redis_client


@atexit.register
def _close_ai_trade_redis() -> None:
    """Best-effort close of the cached client on process shutdown."""
    global _ai_trade_redis_client
    if _ai_trade_redis_client is not None:
        try:
            _ai_trade_redis_client.close()
        except Exception:
            pass


def dispatch_trade_trigger(trigger: Dict[str, Any]) -> bool:
    """Publish a trade trigger for the AI Trade Setups card.

    The feed worker (``upstox_live_feed.py``) is a STANDALONE process, so it cannot
    use Django Channels' in-process ``group_send`` (``get_channel_layer`` would
    raise ``ImproperlyConfigured`` there). It instead publishes to a raw Redis
    Pub/Sub channel; a bridge thread inside the Django ASGI process
    (``Live_dashboard.consumers``) re-broadcasts the payload into the
    ``ai_trade_setups`` group, which ``AITradeTriggerConsumer`` forwards to every
    connected browser as ``{action: trade_trigger, status: BUY_NOW, ...}``.

    FIX vs original: a single transient Redis blip silently dropped the BUY_NOW
    message, leaving the dashboard out of sync with whatever trade may have
    fired at the broker. Now retries up to ``DISPATCH_MAX_RETRIES`` times with
    exponential backoff, AND pushes the payload to a durable LIST
    (``DISPATCH_DURABLE_KEY``) so a consumer restart can replay missed triggers.
    """
    client = _get_trade_redis_client()
    if client is None:
        logger.critical("redis-py unavailable; trigger LOST: %s", trigger)
        return False
    payload = json.dumps(trigger, default=str)
    last_exc: Optional[Exception] = None
    for attempt in range(DISPATCH_MAX_RETRIES):
        try:
            client.publish(AI_TRADE_REDIS_CHANNEL, payload)
            # Also push to a durable list so a consumer restart can replay.
            client.lpush(DISPATCH_DURABLE_KEY, payload)
            client.ltrim(DISPATCH_DURABLE_KEY, 0, DISPATCH_DURABLE_MAX - 1)
            return True
        except Exception as exc:
            last_exc = exc
            if attempt < DISPATCH_MAX_RETRIES - 1:
                time.sleep(DISPATCH_BACKOFF_BASE * (2 ** attempt))
    logger.critical(
        "Trigger LOST after %d retries: %s (last error: %s)",
        DISPATCH_MAX_RETRIES, payload, last_exc,
    )
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
    docstring). It keeps a bounded history so the engulfing comparison, the
    3-candle CVD trend and the 14-period ATR always have context.
    """

    def __init__(
        self,
        setup_name: str,
        entry_price: float,
        *,
        symbol: Optional[str] = None,
        zone_pct: float = ENTRY_ZONE_PCT,
        trend_window: int = TREND_WINDOW,
        history_size: int = 30,
        dispatch: bool = True,
        option_side: str = "CE",
        atr_period: int = ATR_PERIOD,
        atr_multiplier: float = ATR_MULTIPLIER,
        setup_expiry_seconds: Optional[float] = SETUP_EXPIRY_SECONDS,
        setup_created_at: Optional[datetime] = None,
    ) -> None:
        self.setup_name = setup_name
        self.entry_price = float(entry_price)
        self.symbol = symbol
        self.zone_pct = zone_pct
        self.trend_window = trend_window
        self.should_dispatch = dispatch
        self.option_side = (option_side or "CE").upper()
        self.atr_period = max(1, int(atr_period))
        self.atr_multiplier = float(atr_multiplier)
        # ARCHITECTURAL UPGRADE: setup auto-expiry. None (or 0) disables the
        # gate; otherwise the engine refuses to fire once the setup has aged
        # past this many seconds. ``setup_created_at`` defaults to NOW in
        # LOCAL_TZ (tz-aware so a UTC host doesn't shift the expiry window);
        # callers can pass an explicit value to rehydrate an engine from a
        # persisted state (e.g. after a worker restart).
        self.setup_expiry_seconds = setup_expiry_seconds
        self.setup_created_at: datetime = (
            setup_created_at if setup_created_at is not None else datetime.now(LOCAL_TZ)
        )
        # Latest computed ATR (and hence the entry-zone buffer), exposed to the
        # feed worker so it can be forwarded to the dashboard.
        self.latest_atr: Optional[float] = None
        # Default history keeps enough 1-minute candles for the 14-period ATR.
        self._history: deque = deque(maxlen=max(3, int(history_size)))

    def update_entry(self, entry_price: float, *, option_side: Optional[str] = None) -> None:
        """Refresh the entry reference WITHOUT discarding the rolling candle history.

        The feed re-arms engines on ATM strike rolls / side flips; recreating the
        engine there would empty ``_history`` and starve the engulfing + 3-candle
        CVD-trend gates of their ``prev_candle`` context.

        ARCHITECTURAL UPGRADE: a re-arm ALSO refreshes ``setup_created_at``, so
        the new setup (rolled strike / flipped side) gets a fresh expiry window.
        Without this, a re-arm 16 minutes after the original setup would
        immediately expire the new signal too.
        """
        self.entry_price = float(entry_price)
        if option_side:
            self.option_side = (option_side or "CE").upper()
        # Re-arm = new setup; reset the expiry clock.
        self.setup_created_at = datetime.now(LOCAL_TZ)

    def is_setup_expired(self) -> bool:
        """True when the setup has aged past ``setup_expiry_seconds``.

        Returns False when the gate is disabled (``setup_expiry_seconds`` is
        None or <= 0). Used by ``on_candle`` to short-circuit stale setups, and
        is also exposed for the dashboard to render a "setup age" countdown.
        """
        if self.setup_expiry_seconds is None or self.setup_expiry_seconds <= 0:
            return False
        elapsed = (datetime.now(LOCAL_TZ) - self.setup_created_at).total_seconds()
        return elapsed > self.setup_expiry_seconds

    def setup_age_seconds(self) -> float:
        """Return the elapsed seconds since ``setup_created_at`` (for dashboards)."""
        return (datetime.now(LOCAL_TZ) - self.setup_created_at).total_seconds()

    def on_candle(self, candle: Dict[str, Any]) -> Optional[ConfirmationResult]:
        """Handle a closed candle: evaluate + dispatch when all gates pass."""
        if not isinstance(candle, dict):
            return None
        if self.symbol and candle.get("symbol") != self.symbol:
            # FIX: original silently dropped mismatched-symbol candles. Now
            # logs at DEBUG so a misconfigured feed (instrument-token
            # collision, multi-symbol CVD engine) is at least visible.
            logger.debug(
                "Dropped candle: symbol mismatch (got %s, want %s)",
                candle.get("symbol"), self.symbol,
            )
            return None

        # ARCHITECTURAL UPGRADE: setup expiry. Compute age now (once, in LOCAL_TZ)
        # so the gate inside evaluate_confirmation sees a consistent value and
        # the log line below shows the same age that decided the gate.
        age_seconds = self.setup_age_seconds()
        if self.is_setup_expired():
            logger.info(
                "Setup '%s' expired (age=%ds > %ds); skipping candle.",
                self.setup_name,
                int(age_seconds),
                int(self.setup_expiry_seconds or 0),
            )
            # Still append the candle so history is continuous once the setup is
            # re-armed (update_entry refreshes setup_created_at and the next
            # candle will be evaluated fresh).
            self._history.append(candle)
            return ConfirmationResult(
                triggered=False,
                in_zone=False,
                pattern=None,
                cvd=False,
                side=self.option_side,
                reasons=[
                    f"suppressed:setup_expired({int(age_seconds)}s > "
                    f"{int(self.setup_expiry_seconds or 0)}s)"
                ],
                trigger=None,
                atr=None,
            )

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
            atr_period=self.atr_period,
            atr_multiplier=self.atr_multiplier,
            symbol=self.symbol,
            setup_age_seconds=age_seconds,
            setup_expiry_seconds=self.setup_expiry_seconds,
        )
        self.latest_atr = result.atr

        if result.triggered:
            logger.info(
                "AI trade trigger fired: setup=%s reasons=%s",
                self.setup_name,
                result.reasons,
            )
            if self.should_dispatch:
                dispatch_trade_trigger(result.trigger)

        return result
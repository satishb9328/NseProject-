"""
Directional Fibonacci Bounce — live intraday engine (Index/Spot, 3-minute synthesis).

Independent from the option-premium ``CandleConfirmationEngine``. This module is fed
ONLY the live Index/Spot tick (``NSE_INDEX|Nifty 50``) and owns:

  1. BASE ANCHOR      — exact High/Low of the first 3-minute candle of the session
     (09:15:00 .. 09:17:59 IST).
  2. DIRECTIONAL FIB  — the 0.0/0.236/0.5/0.786/1.0/1.618/2.618/3.618 grid drawn
     *bottom->top* for CE (bullish, 0.0 = Low) and *top->bottom* for PE
     (bearish, 0.0 = High).
  3. TRIGGER          — a live index touch within ``FIB_TOUCH_TOLERANCE_PCT`` (0.05%)
     of a level starts a pattern check on the SYNTHESIZED 3-minute candle (merge of
     the last three 1-minute candles).
  4. STRICT 15% DIP   — option entry is ``ATM premium * PREMIUM_DIP_FACTOR`` (0.85),
     identical to the Momentum Pullback setup. Never bypassed.
  5. GAMMA-DRIVEN    — the CE/PE side is chosen from the live NIFTY DAY GAMMA
     (net_gex / gross_gex = ``net_gex_ratio``): positive -> CE (bottom->top),
     negative -> PE (top->bottom), neutral -> NO_TRADE (Fib suspended). The
     SCORE / Bias / Momentum no-trade zone is still applied as a defensive gate.

The BUY_NOW payload reuses ``ai_candle_confirmation.dispatch_trade_trigger`` so it
lands on the SAME ``ai:trade_trigger`` Redis channel the dashboard already listens to.
"""

from __future__ import annotations

import logging
import time
from collections import deque
from datetime import datetime, time as time_of_day, timedelta, timezone
from typing import Any, Dict, Optional, Sequence

from Nse_Project.Live_dashboard.ai_candle_confirmation import (
    build_trigger_payload,
    dispatch_trade_trigger,
)

logger = logging.getLogger(__name__)

IST = timezone(timedelta(hours=5, minutes=30))

# ── Tuning constants ──────────────────────────────────────────────────────────
FIB_RATIOS = (0.0, 0.236, 0.5, 0.786, 1.0, 1.618, 2.618, 3.618)
FIB_TOUCH_TOLERANCE_PCT = 0.0005   # 0.05% — tight touch tolerance
PREMIUM_DIP_FACTOR = 0.85          # STRICT 15% dip (entry = ATM premium * 0.85)
SYNTH_PERIOD = 3                   # synthesize a 3-minute candle from 3 x 1-minute
ANCHOR_START = time_of_day(9, 15)  # first 3-min candle opens 09:15:00
ANCHOR_END = time_of_day(9, 18)    # exclusive -> closes 09:17:59.999

# Reversal-pattern tolerances (same spirit as ai_candle_confirmation).
PIN_BAR_WICK_RATIO = 2.0
PIN_BAR_LOWER_WICK_FRAC = 0.5
PIN_BAR_UPPER_WICK_FRAC = 0.35
TWEEZER_TOLERANCE = 0.001          # 0.1% relative matching highs/lows

# Day-Gamma neutral band (relaxed "sweet spot"): loosened from gamma_blast_engine's
# 0.15 to 0.10 so slow-grinding / low-VIX trends flip directional sooner, while still
# filtering out flat / choppy markets.
GAMMA_DAY_NEUTRAL_FRACTION = 0.10

__all__ = [
    "FibBounceEngine",
    "FIB_RATIOS",
    "FIB_TOUCH_TOLERANCE_PCT",
    "PREMIUM_DIP_FACTOR",
    "directional_fib_levels",
    "synthesize_3m_candle",
    "nearest_fib_touch",
    "resolve_trade_side",
    "resolve_gamma_side",
    "GAMMA_DAY_NEUTRAL_FRACTION",
    "has_bullish_reversal_3m",
    "has_bearish_reversal_3m",
    "build_fib_trigger_payload",
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


def _coerce_timestamp(value: Any) -> datetime:
    """Normalise epoch (s/ms) / ISO string / datetime to an IST-aware datetime."""
    if value is None:
        return datetime.now(IST)
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=IST)
    if isinstance(value, (int, float)):
        v = float(value)
        if v > 1e12:  # milliseconds -> seconds
            v /= 1000.0
        return datetime.fromtimestamp(v, tz=IST)
    if isinstance(value, str):
        s = value.strip()
        if s.isdigit() or (s.startswith("-") and s[1:].isdigit()):
            v = float(s)
            if v > 1e12:
                v /= 1000.0
            return datetime.fromtimestamp(v, tz=IST)
        try:
            return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(IST)
        except Exception:
            return datetime.now(IST)
    return datetime.now(IST)

# ── Fibonacci mathematics ─────────────────────────────────────────────────────
def directional_fib_levels(anchor_low, anchor_high, side: str) -> Dict[float, float]:
    """Directional Fibonacci grid for one side.

    * CE (bullish): bottom -> top    -> price(ratio) = low  + ratio * range  (0.0=Low)
    * PE (bearish): top    -> bottom -> price(ratio) = high - ratio * range  (0.0=High)
    """
    low = _to_float(anchor_low)
    high = _to_float(anchor_high)
    if low is None or high is None or high <= low:
        return {}
    rng = high - low
    if str(side or "").upper() == "PE":
        return {r: round(high - r * rng, 4) for r in FIB_RATIOS}
    return {r: round(low + r * rng, 4) for r in FIB_RATIOS}


def nearest_fib_touch(price, levels, tolerance_pct: float = FIB_TOUCH_TOLERANCE_PCT):
    """Scan ALL Fib levels (no restriction) and return the closest touched level.

    Returns ``(ratio, level, distance_pct)`` for the closest level the price is
    within ``tolerance_pct`` of, or None when no level is touched. ``levels`` is the
    full ``FIB_RATIOS`` grid — 0.0, 0.236, 0.5, 0.786, 1.0, 1.618, 2.618, 3.618 —
    and every one of them is iterated on every tick.
    """
    p = _to_float(price)
    tol = _to_float(tolerance_pct)
    if p is None or p <= 0 or tol is None or tol < 0 or not levels:
        return None
    best = None
    for ratio, level in levels.items():
        lv = _to_float(level)
        if lv is None or lv <= 0:
            continue
        dist = abs(p - lv) / lv
        if dist <= tol and (best is None or dist < best[2]):
            best = (float(ratio), float(lv), float(dist))
    return best


def resolve_trade_side(score: Any, momentum: Any, net_oi_bias: Any) -> Optional[str]:
    """Authoritative CE/PE side from the master gates (SCORE / Bias / Momentum).

    Mirrors ``ai_candle_confirmation.is_no_trade_zone`` exactly — returns None (NO
    TRADE) whenever that function returns True:
      * bullish = score > 55
      * bearish = score < 45, or (net_oi_bias < 0 and score < 50)
      * middle tie-break: momentum > +20 -> CE ; momentum < -20 -> PE
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

    if bullish:
        return "CE"
    if bearish:
        return "PE"
    return None


def resolve_gamma_side(net_gex_ratio, neutral_fraction: float = GAMMA_DAY_NEUTRAL_FRACTION) -> Optional[str]:
    """Directional side from the live NIFTY DAY GAMMA (Day Bias) conviction.

    Mirrors ``gamma_blast_engine.day_gamma_regime_signal`` (ratio = net_gex / gross_gex):
      * ratio <= -neutral_fraction -> "PE"  (bearish, draw Fib top->bottom)
      * ratio >= +neutral_fraction -> "CE"  (bullish, draw Fib bottom->top)
      * otherwise                  -> None  (🟡 Range-bound: Choppy / NO_TRADE — suspend Fib)
    """
    r = _to_float(net_gex_ratio)
    frac = _to_float(neutral_fraction)
    if r is None or frac is None or frac < 0:
        return None
    if r <= -frac:
        return "PE"
    if r >= frac:
        return "CE"
    return None


# ── Synthesized 3-minute reversal patterns ────────────────────────────────────
def _ohlc(candle: Any) -> Optional[tuple]:
    if not isinstance(candle, dict):
        return None
    o = _to_float(candle.get("open"))
    h = _to_float(candle.get("high"))
    l = _to_float(candle.get("low"))
    c = _to_float(candle.get("close"))
    if None in (o, h, l, c):
        return None
    return o, h, l, c


def _is_bullish_pin_bar(o: float, h: float, l: float, c: float) -> bool:
    rng = h - l
    if rng <= 0:
        return False
    body = abs(c - o)
    lower = min(o, c) - l
    upper = h - max(o, c)
    if body == 0:
        return (lower >= PIN_BAR_LOWER_WICK_FRAC * rng
                and upper <= PIN_BAR_UPPER_WICK_FRAC * rng)
    return (lower >= PIN_BAR_WICK_RATIO * body
            and lower >= PIN_BAR_LOWER_WICK_FRAC * rng
            and upper <= PIN_BAR_UPPER_WICK_FRAC * rng)


def _is_bearish_pin_bar(o: float, h: float, l: float, c: float) -> bool:
    rng = h - l
    if rng <= 0:
        return False
    body = abs(c - o)
    lower = min(o, c) - l
    upper = h - max(o, c)
    if body == 0:
        return (upper >= PIN_BAR_LOWER_WICK_FRAC * rng
                and lower <= PIN_BAR_UPPER_WICK_FRAC * rng)
    return (upper >= PIN_BAR_WICK_RATIO * body
            and upper >= PIN_BAR_LOWER_WICK_FRAC * rng
            and lower <= PIN_BAR_UPPER_WICK_FRAC * rng)


def has_bullish_reversal_3m(candle, prev_candle=None) -> Optional[str]:
    """Bullish reversal on the SYNTHESIZED 3-minute candle (vs prior synth candle)."""
    ohlc = _ohlc(candle)
    if ohlc is None:
        return None
    o, h, l, c = ohlc
    if _is_bullish_pin_bar(o, h, l, c):
        return "hammer_3m"
    prev = _ohlc(prev_candle)
    if prev is not None:
        po, _ph, pl, pc = prev
        if c > o and pc < po and o <= pc and c >= po:
            return "bullish_engulfing_3m"
        if abs(l - pl) <= TWEEZER_TOLERANCE * max(pl, 1.0):
            return "tweezer_bottoms_3m"
    return None


def has_bearish_reversal_3m(candle, prev_candle=None) -> Optional[str]:
    """Bearish reversal on the SYNTHESIZED 3-minute candle (vs prior synth candle)."""
    ohlc = _ohlc(candle)
    if ohlc is None:
        return None
    o, h, l, c = ohlc
    if _is_bearish_pin_bar(o, h, l, c):
        return "shooting_star_3m"
    prev = _ohlc(prev_candle)
    if prev is not None:
        po, ph, _pl, pc = prev
        if c < o and pc > po and o >= pc and c <= po:
            return "bearish_engulfing_3m"
        if abs(h - ph) <= TWEEZER_TOLERANCE * max(ph, 1.0):
            return "tweezer_tops_3m"
    return None


def synthesize_3m_candle(candles_1m: Sequence[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Merge the last three 1-minute candles into one 3-minute OHLC candle.

    Returns None when fewer than three valid 1-minute candles are available.
    """
    bars = [c for c in (candles_1m or []) if isinstance(c, dict) and _ohlc(c) is not None]
    last = bars[-SYNTH_PERIOD:]
    if len(last) < SYNTH_PERIOD:
        return None
    o = _ohlc(last[0])[0]
    c = _ohlc(last[-1])[3]
    h = max(_ohlc(b)[1] for b in last)
    l = min(_ohlc(b)[2] for b in last)
    return {
        "open": o,
        "high": h,
        "low": l,
        "close": c,
        "ts_open": last[0].get("ts_open"),
        "ts_close": last[-1].get("ts_close"),
        "bars": len(last),
    }


def build_fib_trigger_payload(
    setup_name: str,
    option_side: str,
    *,
    ratio: float,
    level: float,
    pattern: str,
    entry_price: Optional[float],
    premium: Optional[float],
    anchor_low: Optional[float],
    anchor_high: Optional[float],
) -> Dict[str, Any]:
    """The BUY_NOW payload (extends the shared trigger contract with Fib diagnostics)."""
    payload = build_trigger_payload(setup_name, option_side)
    payload.update({
        "strategy": "fib_bounce",
        "fib_ratio": ratio,
        "fib_level": level,
        "pattern": pattern,
        "entry_price": entry_price,
        "premium": premium,
        "anchor_high": anchor_high,
        "anchor_low": anchor_low,
    })
    return payload

# ── Stateful engine ───────────────────────────────────────────────────────────
class FibBounceEngine:
    """Owns anchor capture, directional Fib grids, 3-min synthesis and dispatch.

    ``on_index_tick`` is the single entry point. It is expected to be called from the
    feed worker's event loop (single-threaded), so no internal locking is required.
    """

    def __init__(
        self,
        setup_name: str = "fib_bounce",
        *,
        entry_factor: float = PREMIUM_DIP_FACTOR,
        touch_tolerance_pct: float = FIB_TOUCH_TOLERANCE_PCT,
        premium_provider=None,      # callable(side: str) -> float | None (ATM option LTP)
        gamma_side_provider=None,   # callable() -> "CE" | "PE" | None (live Day Gamma bias)
        no_trade_probe=None,        # callable() -> bool (defensive score/momentum gate)
        dispatch: bool = True,
        cooldown_seconds: float = 60.0,
        history_size: int = 120,
    ) -> None:
        self.setup_name = setup_name
        self.entry_factor = float(entry_factor)
        self.touch_tolerance_pct = float(touch_tolerance_pct)
        self.premium_provider = premium_provider
        self.gamma_side_provider = gamma_side_provider
        self.no_trade_probe = no_trade_probe
        self.should_dispatch = bool(dispatch)
        self.cooldown_seconds = float(cooldown_seconds)

        self._one_min_history: deque = deque(maxlen=max(SYNTH_PERIOD * 2, int(history_size)))
        self._cur_1m: Optional[Dict[str, Any]] = None
        self._anchor_high: Optional[float] = None
        self._anchor_low: Optional[float] = None
        self._anchor_captured = False
        self._levels: Dict[str, Dict[float, float]] = {}
        self._touch_armed = True
        self._last_fired: Dict[tuple, float] = {}
        self._active_side: Optional[str] = None
        self.last_touch: Optional[Dict[str, Any]] = None
        self.latest_synth: Optional[Dict[str, Any]] = None

    # ── Diagnostics (read-only) ───────────────────────────────────────────────
    @property
    def anchor_high(self) -> Optional[float]:
        return self._anchor_high

    @property
    def anchor_low(self) -> Optional[float]:
        return self._anchor_low

    @property
    def anchor_captured(self) -> bool:
        return self._anchor_captured

    @property
    def levels(self) -> Dict[str, Dict[float, float]]:
        return dict(self._levels)

    def snapshot(self) -> Dict[str, Any]:
        """Serialisable state for the dashboard (folded into live_state['fib_bounce'])."""
        return {
            "setup_type": self.setup_name,
            "anchor_captured": self._anchor_captured,
            "anchor_high": self._anchor_high,
            "anchor_low": self._anchor_low,
            "active_side": self._active_side,
            "levels_ce": self._levels.get("CE", {}),
            "levels_pe": self._levels.get("PE", {}),
            "last_touch": self.last_touch,
            "latest_synth_3m": self.latest_synth,
        }

    # ── Main entry ────────────────────────────────────────────────────────────
    def on_index_tick(self, ltp, ts=None) -> Optional[Dict[str, Any]]:
        """Feed a live index LTP; returns the trigger dict when BUY_NOW fires."""
        price = _to_float(ltp)
        if price is None or price <= 0:
            return None
        ts = _coerce_timestamp(ts)
        self._bucket_1m(price, ts)

        if not self._anchor_captured:
            self._capture_anchor(price, ts)
            if not self._anchor_captured:
                return None

        side = self._resolve_side()
        self._active_side = side
        if side is None:  # neutral / range-bound Day Gamma -> NO_TRADE (suspend Fib)
            self._touch_armed = True
            return None

        levels = self._levels.get(side) or {}
        touch = nearest_fib_touch(price, levels, self.touch_tolerance_pct)
        if touch is None:
            self._touch_armed = True
            self.last_touch = None
            return None

        ratio, level, dist = touch
        self.last_touch = {
            "side": side, "ratio": ratio, "level": level,
            "distance_pct": dist, "price": price,
        }
        if not self._touch_armed:
            return None

        synth = synthesize_3m_candle(list(self._one_min_history))
        prev = synthesize_3m_candle(list(self._one_min_history)[:-SYNTH_PERIOD])
        self.latest_synth = synth

        pattern = None
        if synth is not None:
            pattern = (
                has_bullish_reversal_3m(synth, prev) if side == "CE"
                else has_bearish_reversal_3m(synth, prev)
            )
        if not pattern:
            # Still inside the band: keep armed and re-check on the next tick, so a
            # reversal that completes a moment later still fires.
            return None

        if self._no_trade_blocked():
            self._touch_armed = False
            return None

        key = (side, ratio)
        now_mono = time.monotonic()
        if now_mono - self._last_fired.get(key, 0.0) < self.cooldown_seconds:
            self._touch_armed = False
            return None

        premium = self._fetch_premium(side)
        entry = round(premium * self.entry_factor, 2) if premium is not None else None
        trigger = build_fib_trigger_payload(
            self.setup_name,
            side,
            ratio=ratio,
            level=level,
            pattern=pattern,
            entry_price=entry,
            premium=premium,
            anchor_low=self._anchor_low,
            anchor_high=self._anchor_high,
        )
        self._last_fired[key] = now_mono
        self._touch_armed = False
        if self.should_dispatch:
            try:
                dispatch_trade_trigger(trigger)
            except Exception:
                logger.exception("Fib bounce dispatch failed")
        logger.info(
            "Fib bounce BUY_NOW: side=%s ratio=%s level=%s pattern=%s entry=%s",
            side, ratio, level, pattern, entry,
        )
        return trigger

    # ── Internals ─────────────────────────────────────────────────────────────
    @staticmethod
    def _new_1m(bucket: datetime, price: float) -> Dict[str, Any]:
        return {
            "ts_open": bucket,
            "ts_close": bucket + timedelta(minutes=1),
            "open": price, "high": price, "low": price, "close": price,
        }

    def _bucket_1m(self, price: float, ts: datetime) -> None:
        bucket = ts.replace(second=0, microsecond=0)
        cur = self._cur_1m
        if cur is None:
            self._cur_1m = self._new_1m(bucket, price)
            return
        if bucket == cur["ts_open"]:
            cur["high"] = max(cur["high"], price)
            cur["low"] = min(cur["low"], price)
            cur["close"] = price
        elif bucket > cur["ts_open"]:
            self._one_min_history.append(cur)
            self._cur_1m = self._new_1m(bucket, price)
        # else: out-of-order older tick — ignored for candle construction.

    def _capture_anchor(self, price: float, ts: datetime) -> None:
        t = ts.time()
        if ANCHOR_START <= t < ANCHOR_END:
            self._anchor_high = price if self._anchor_high is None else max(self._anchor_high, price)
            self._anchor_low = price if self._anchor_low is None else min(self._anchor_low, price)
        if t >= ANCHOR_END and self._anchor_high is not None and self._anchor_low is not None:
            self._anchor_captured = True
            self._recompute_levels()

    def _recompute_levels(self) -> None:
        if self._anchor_high is None or self._anchor_low is None:
            self._levels = {}
            return
        self._levels = {
            "CE": directional_fib_levels(self._anchor_low, self._anchor_high, "CE"),
            "PE": directional_fib_levels(self._anchor_low, self._anchor_high, "PE"),
        }

    def _resolve_side(self) -> Optional[str]:
        if self.gamma_side_provider is None:
            return None
        try:
            side = self.gamma_side_provider()
        except Exception:
            logger.exception("Fib bounce gamma_side_provider failed")
            return None
        return side if side in ("CE", "PE") else None

    def _no_trade_blocked(self) -> bool:
        if self.no_trade_probe is None:
            return False
        try:
            return bool(self.no_trade_probe())
        except Exception:
            logger.exception("Fib bounce no_trade_probe failed; defaulting to blocked")
            return True

    def _fetch_premium(self, side: str) -> Optional[float]:
        if self.premium_provider is None:
            return None
        try:
            premium = self.premium_provider(side)
        except Exception:
            logger.exception("Fib bounce premium_provider failed")
            return None
        p = _to_float(premium)
        return p if p and p > 0 else None

# ════════════════════════════════════════════════════════════════════════════
# SELF-TEST (no broker / no Redis required)
# ════════════════════════════════════════════════════════════════════════════
def _self_test() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")

    # 1) Directional Fib math
    ce = directional_fib_levels(100.0, 200.0, "CE")
    pe = directional_fib_levels(100.0, 200.0, "PE")
    assert ce[0.0] == 100.0 and ce[1.0] == 200.0 and abs(ce[0.5] - 150.0) < 1e-6
    assert pe[0.0] == 200.0 and pe[1.0] == 100.0 and abs(pe[0.5] - 150.0) < 1e-6
    assert abs(ce[1.618] - 261.8) < 0.01

    # 2) 3-minute synthesis (merge of 3 x 1-minute)
    synth = synthesize_3m_candle([
        {"open": 100, "high": 105, "low": 99, "close": 104},
        {"open": 104, "high": 107, "low": 103, "close": 106},
        {"open": 106, "high": 108, "low": 101, "close": 102},
    ])
    assert synth["open"] == 100 and synth["high"] == 108 and synth["low"] == 99
    assert synth["close"] == 102

    # 3) Reversal patterns on the synthesized 3m candle
    hammer = {"open": 123.6, "high": 124.5, "low": 120.0, "close": 124.0}
    star = {"open": 101.0, "high": 110.0, "low": 100.0, "close": 102.0}
    assert has_bullish_reversal_3m(hammer) == "hammer_3m"
    assert has_bearish_reversal_3m(star) == "shooting_star_3m"

    # 4) Gamma-driven direction (Day Bias) + universal level scan sanity
    assert resolve_gamma_side(0.40) == "CE"    # positive gamma -> bullish
    assert resolve_gamma_side(-0.40) == "PE"   # negative gamma -> bearish
    assert resolve_gamma_side(0.0) is None     # range-bound choppy -> NO_TRADE
    assert resolve_gamma_side(0.05) is None    # inside the relaxed ±0.10 neutral band
    assert resolve_gamma_side(0.12) == "CE"    # caught earlier than the old ±0.15 band
    assert resolve_trade_side(60, 0, 0) == "CE"
    assert resolve_trade_side(40, 0, 0) == "PE"
    assert resolve_trade_side(50, 0, 0) is None
    # Universal scanning: ALL 8 ratios exist in the grid and every one is scannable.
    for r in FIB_RATIOS:
        assert r in ce and r in pe

    # 5) Full engine: anchor -> levels -> touch -> hammer -> BUY_NOW
    engine = FibBounceEngine(
        "fib_bounce",
        dispatch=False,
        premium_provider=lambda side: 150.0,
        gamma_side_provider=lambda: "CE",
        cooldown_seconds=0.0,
    )
    t0 = datetime(2026, 10, 22, 9, 15, 0, tzinfo=IST)
    seq = [
        (0, 100.0),      # 09:15:00 anchor low
        (60, 200.0),     # 09:16:00 anchor high
        (120, 150.0),    # 09:17:00
        (180, 123.6),    # 09:18:00 -> finalizes anchor (window closed)
        (181, 123.8),
        (182, 123.0),
        (183, 123.5),    # 09:18 candle (open 123.6, high 123.8, low 123.0, close 123.5)
        (240, 123.5),    # 09:19:00
        (241, 124.5),
        (242, 120.0),
        (243, 121.0),    # 09:19 candle
        (300, 121.0),    # 09:20:00
        (301, 122.5),
        (302, 120.5),
        (303, 124.0),    # 09:20 candle -> 3m synth = hammer
    ]
    for sec, px in seq:
        engine.on_index_tick(px, t0 + timedelta(seconds=sec))

    assert engine.anchor_captured
    assert engine.anchor_high == 200.0 and engine.anchor_low == 100.0

    trigger = engine.on_index_tick(123.6, t0 + timedelta(seconds=360))  # 09:21:00 touch 0.236
    assert trigger is not None
    assert trigger["status"] == "BUY_NOW" and trigger["option_side"] == "CE"
    assert abs(trigger["fib_ratio"] - 0.236) < 1e-9
    assert trigger["pattern"] == "hammer_3m"
    assert trigger["premium"] == 150.0 and trigger["entry_price"] == round(150.0 * 0.85, 2)

    print("Fib bounce self-test PASSED")


if __name__ == "__main__":
    _self_test()







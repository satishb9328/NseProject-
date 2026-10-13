"""
First Candle Fibonacci Swing — algorithmic strategy engine.

Strategy
--------
The opening 1-minute candle (09:15-09:16 IST) of the trading day defines the day's
"base range". Every other price level is derived from that single candle:

    Fib 1.00  =  first-candle HIGH
    Fib 0.00  =  first-candle LOW

From that range the engine projects the standard swing levels
``0.00, 0.50, 0.786, 1.00, 1.618, 2.618`` where each price is::

    price(ratio) = base_low + ratio * (base_high - base_low)

Two setups fire from those levels:

Strategy A — "Day Low Rejection" (CE BUY)
    * Price dips back to the 0.00 fib (base low), within a ±0.1% buffer.
    * A BULLISH Pin Bar / Bullish Engulfing prints at that level.
    * Filter: the live "Math Console Score" > 45 (rules out a severe downtrend).
    * Entry = confirmation-candle HIGH breakout, SL = its LOW,
      T1 = 1.00 fib, T2 = 1.618 fib.

Strategy B — "Fib Extension Trap" (PE BUY)
    * Price spikes to the 1.618 or 2.618 extension, within a ±0.1% buffer.
    * A BEARISH Pin Bar / Bearish Engulfing prints at that level.
    * No momentum filter (the extension level + bearish reversal pattern suffice).
    * Entry = confirmation-candle LOW breakout, SL = its HIGH,
      T1 = next major fib below, T2 = the extension below that.

Strategy C — "Trend Retracement" (gamma-gated, CE/PE)
    * Combines the live Day Gamma trend with the internal fibs 0.236 / 0.50 / 0.786.
    * POSITIVE gamma  → CE only on dips   (Bullish bounce at 0.786 / 0.50 / 0.236).
    * NEGATIVE gamma  → PE only on rallies (Bearish reject at 0.236 / 0.50 / 0.786).
    * Targets step through the retracement ladder: a bounce at 0.50 → (0.786, 1.00);
      a reject at 0.50 → (0.236, 0.00).

When a signal fires, the engine publishes a ``trade_trigger`` through the existing
``ai_candle_confirmation.dispatch_trade_trigger()`` Redis Pub/Sub path, so the
frontend ``ws/ai-trade-triggers/`` WebSocket receives the BUY_NOW instantly.

The module is standalone and import-safe: the only runtime dependency it pulls in is
``ai_candle_confirmation`` (stdlib-only at import time; ``redis`` is imported lazily
inside ``dispatch_trade_trigger``).
"""

from __future__ import annotations

import logging
import sys
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

# Make ``Nse_Project.*`` importable when this file is run directly (``python
# fib_swing_engine.py``). Mirrors the bootstrap in ``upstox_live_feed.py``.
_ROOT_DIR = Path(__file__).resolve().parent.parent.parent
if str(_ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(_ROOT_DIR))

from Nse_Project.Live_dashboard.ai_candle_confirmation import (  # noqa: E402
    dispatch_trade_trigger,
    has_bearish_pattern,
    has_bullish_pattern,
)

logger = logging.getLogger(__name__)

# ── Fibonacci level ratios ──────────────────────────────────────────────────────
FIB_0_0 = 0.0
FIB_0_236 = 0.236
FIB_0_5 = 0.5
FIB_0_786 = 0.786
FIB_1_0 = 1.0
FIB_1_618 = 1.618
FIB_2_618 = 2.618

# Every level the engine projects (used for zone detection + reporting).
FIB_LEVELS: tuple[float, ...] = (0.0, 0.236, 0.5, 0.786, 1.0, 1.618, 2.618)

# The "major" structural levels used for Target 1 / Target 2. The intermediate
# 0.50 / 0.786 are zone markers, not target rungs, so a long from 0.00 targets
# 1.00 then 1.618 exactly as specified.
MAJOR_FIB_LEVELS: tuple[float, ...] = (0.0, 1.0, 1.618, 2.618)

# Trend-retracement levels: the "internal" fibs between the base (0.00) and the
# first-candle high (1.00). Ascending order; bullish scans reversed (deepest first).
RETRACEMENT_LEVELS: tuple[float, ...] = (0.236, 0.50, 0.786)

# Target ladder for the trend-retracement leg — steps through the internal fibs and
# then the extensions, so a bounce at 0.50 targets 0.786 then 1.00, and a reject at
# 0.50 targets 0.236 then 0.00.
RETRACEMENT_LADDER: tuple[float, ...] = (0.0, 0.236, 0.50, 0.786, 1.0, 1.618, 2.618)

# ── Tuning constants ────────────────────────────────────────────────────────────
ZONE_BUFFER_PCT = 0.001          # ±0.1% around a fib level
MIN_MATH_CONSOLE_SCORE = 45.0    # Strategy A data filter (not a severe downtrend)

# Setup names dispatched to the frontend (kept verbatim per spec).
SETUP_DAY_LOW_REJECTION = "Day Low Rejection"
SETUP_FIB_EXTENSION_TRAP = "Fib Extension Trap"

# Trend-retracement setup names, keyed by the retracement level ratio.
BULLISH_RETRACE_SETUPS: Dict[float, str] = {
    FIB_0_786: "Very Strong Bullish Bounce (0.786)",
    FIB_0_5: "Medium Bullish Bounce (0.50)",
    FIB_0_236: "Average Bullish Bounce (0.236)",
}
BEARISH_RETRACE_SETUPS: Dict[float, str] = {
    FIB_0_236: "Very Strong Bearish Reject (0.236)",
    FIB_0_5: "Medium Bearish Reject (0.50)",
    FIB_0_786: "Average Bearish Reject (0.786)",
}

__all__ = [
    "FibSignal",
    "FibonacciSwingEngine",
    "FIB_LEVELS",
    "MAJOR_FIB_LEVELS",
    "RETRACEMENT_LEVELS",
    "RETRACEMENT_LADDER",
    "ZONE_BUFFER_PCT",
    "MIN_MATH_CONSOLE_SCORE",
    "SETUP_DAY_LOW_REJECTION",
    "SETUP_FIB_EXTENSION_TRAP",
    "BULLISH_RETRACE_SETUPS",
    "BEARISH_RETRACE_SETUPS",
    "fib_price",
    "build_fib_levels",
    "within_fib_zone",
    "targets_for_ratio",
    "retracement_targets",
]

# ── Pure helpers (stdlib-only, unit-test friendly) ──────────────────────────────
def _to_float(value: Any) -> Optional[float]:
    """Coerce ``value`` to float, returning None for missing/NaN/unparseable."""
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    try:
        f = float(value)
    except (TypeError, ValueError):
        return None
    return f if f == f else None


def _candle_date(candle: Dict[str, Any]):
    """Return the trading date of a candle from its ``ts_open`` (ISO string)."""
    ts = candle.get("ts_open") if isinstance(candle, dict) else None
    if isinstance(ts, str) and ts.strip():
        try:
            return datetime.fromisoformat(ts.strip()).date()
        except ValueError:
            pass
    return None


def fib_price(base_low: float, base_high: float, ratio: float) -> float:
    """Project ``ratio`` onto the base range: ``low + ratio * (high - low)``."""
    return float(base_low) + float(ratio) * (float(base_high) - float(base_low))


def build_fib_levels(base_low: float, base_high: float) -> Dict[float, float]:
    """Return ``{ratio: price}`` for every level in ``FIB_LEVELS``."""
    return {ratio: fib_price(base_low, base_high, ratio) for ratio in FIB_LEVELS}


def within_fib_zone(price: Any, level: Any, buffer_pct: float = ZONE_BUFFER_PCT) -> bool:
    """True when ``price`` is within ``buffer_pct`` of ``level`` (default ±0.1%)."""
    p = _to_float(price)
    lv = _to_float(level)
    if p is None or lv is None or lv <= 0:
        return False
    return abs(p - lv) / lv <= float(buffer_pct)


def targets_for_ratio(
    base_low: float,
    base_high: float,
    entry_ratio: float,
    side: str,
) -> tuple:
    """Return ``(target_1, target_2)`` for a trade entered at ``entry_ratio``.

    Targets are the two adjacent *major* fib levels (``0.0, 1.0, 1.618, 2.618``)
    in the direction of the trade. A long from 0.00 → (1.00, 1.618); a short from
    1.618 → (1.00, 0.00); a short from 2.618 → (1.618, 1.00).
    """
    major = list(MAJOR_FIB_LEVELS)
    idx = min(range(len(major)), key=lambda i: abs(major[i] - float(entry_ratio)))
    if (side or "CE").upper() == "PE":
        t1_ratio = major[idx - 1] if idx - 1 >= 0 else None
        t2_ratio = major[idx - 2] if idx - 2 >= 0 else None
    else:  # long from the base
        t1_ratio = major[1] if len(major) > 1 else None
        t2_ratio = major[2] if len(major) > 2 else None

    t1 = fib_price(base_low, base_high, t1_ratio) if t1_ratio is not None else None
    t2 = fib_price(base_low, base_high, t2_ratio) if t2_ratio is not None else None
    return t1, t2


def retracement_targets(
    base_low: float,
    base_high: float,
    entry_ratio: float,
    side: str,
) -> tuple:
    """Return ``(target_1, target_2)`` for a trend-retracement entry at ``entry_ratio``.

    Uses the finer retracement ladder (0.00 → 0.236 → 0.50 → 0.786 → 1.00 → 1.618 →
    2.618). A bullish bounce at 0.50 → (0.786, 1.00); a bearish reject at 0.50 →
    (0.236, 0.00).
    """
    ladder = RETRACEMENT_LADDER
    idx = min(range(len(ladder)), key=lambda i: abs(ladder[i] - float(entry_ratio)))
    if (side or "CE").upper() == "PE":
        t1_ratio = ladder[idx - 1] if idx - 1 >= 0 else None
        t2_ratio = ladder[idx - 2] if idx - 2 >= 0 else None
    else:
        t1_ratio = ladder[idx + 1] if idx + 1 < len(ladder) else None
        t2_ratio = ladder[idx + 2] if idx + 2 < len(ladder) else None

    t1 = fib_price(base_low, base_high, t1_ratio) if t1_ratio is not None else None
    t2 = fib_price(base_low, base_high, t2_ratio) if t2_ratio is not None else None
    return t1, t2


# ── Signal result ───────────────────────────────────────────────────────────────
@dataclass
class FibSignal:
    """Outcome of evaluating one closed candle against the fib levels."""

    triggered: bool
    setup_name: Optional[str] = None
    option_side: str = "CE"
    fib_level: Optional[float] = None          # ratio (e.g. 0.0 / 1.618 / 2.618)
    fib_level_price: Optional[float] = None    # the projected price level
    entry_price: Optional[float] = None
    stop_loss: Optional[float] = None
    target_1: Optional[float] = None
    target_2: Optional[float] = None
    pattern: Optional[str] = None
    math_console_score: Optional[float] = None
    momentum: Optional[float] = None
    day_gamma_trend: Optional[str] = None
    reasons: List[str] = field(default_factory=list)
    trigger: Optional[Dict[str, Any]] = None

# ── Engine ──────────────────────────────────────────────────────────────────────
class FibonacciSwingEngine:
    """Stateful engine that captures the opening range and fires fib swing signals.

    Register an instance as a ``CVDEngine.on_candle`` callback (see the module
    docstring / ``upstox_live_feed.py``). It keeps a bounded candle history so the
    engulfing comparison always has its predecessor, captures the first candle of
    the session as the base range, and resets automatically on a new trading day.
    """

    def __init__(
        self,
        *,
        symbol: Optional[str] = None,
        context_provider: Optional[Callable[[], Any]] = None,
        zone_buffer_pct: float = ZONE_BUFFER_PCT,
        min_math_console_score: float = MIN_MATH_CONSOLE_SCORE,
        history_size: int = 8,
        dispatch: bool = True,
    ) -> None:
        self.symbol = symbol
        # ``context_provider()`` must return ``{"score": float, "momentum": float}``
        # (or a ``(score, momentum)`` tuple). Called from the CVD worker thread, so
        # the wiring in upstox_live_feed.py reads the market_state dict directly.
        self.context_provider = context_provider
        self.zone_buffer_pct = float(zone_buffer_pct)
        self.min_math_console_score = float(min_math_console_score)
        self.should_dispatch = bool(dispatch)

        self._history: deque = deque(maxlen=max(2, int(history_size)))
        self._base_high: Optional[float] = None
        self._base_low: Optional[float] = None
        self._session_date = None

    # ── Base range ──────────────────────────────────────────────────────────────
    @property
    def base_captured(self) -> bool:
        return self._base_high is not None and self._base_low is not None

    @property
    def base_range(self) -> Optional[float]:
        if not self.base_captured:
            return None
        return self._base_high - self._base_low

    @property
    def base_high(self) -> Optional[float]:
        return self._base_high

    @property
    def base_low(self) -> Optional[float]:
        return self._base_low

    def fib_price(self, ratio: float) -> Optional[float]:
        if not self.base_captured:
            return None
        return fib_price(self._base_low, self._base_high, ratio)

    def fib_levels(self) -> Dict[float, Optional[float]]:
        return {ratio: self.fib_price(ratio) for ratio in FIB_LEVELS}

    def reset(self) -> None:
        """Clear the base range + history (new trading day / manual reset)."""
        self._base_high = None
        self._base_low = None
        self._session_date = None
        self._history.clear()
        logger.info("FibonacciSwingEngine reset — awaiting a fresh base candle.")

    def set_base(self, high: float, low: float, session_date=None) -> bool:
        """Install the base range from an external source (historical backfill).

        Used when the worker starts after the 09:15-09:16 opening candle has already
        closed: the true opening range is fetched over REST and installed here so the
        first *live* candle is evaluated against it instead of being (mis)captured as
        the base. ``on_candle`` only captures when ``base_captured`` is False, so once
        this runs the base is immutable for the rest of the session (it is only cleared
        by the next trading day's ``reset``). Returns False when the values are invalid
        and no base is installed.
        """
        h = _to_float(high)
        l = _to_float(low)
        if h is None or l is None or h <= l:
            return False
        self._base_high = h
        self._base_low = l
        if isinstance(session_date, datetime):
            self._session_date = session_date.date()
        elif isinstance(session_date, str):
            try:
                self._session_date = datetime.fromisoformat(session_date).date()
            except ValueError:
                self._session_date = datetime.now().date()
        elif session_date is not None:
            self._session_date = session_date
        else:
            self._session_date = datetime.now().date()
        logger.info(
            "Fibonacci base set (historical backfill): high=%.2f low=%.2f range=%.2f",
            h, l, h - l,
        )
        return True

    def _capture_base(self, candle: Dict[str, Any]) -> None:
        high = _to_float(candle.get("high"))
        low = _to_float(candle.get("low"))
        if high is None or low is None or high <= low:
            return
        self._base_high = high
        self._base_low = low
        self._session_date = _candle_date(candle) or datetime.now().date()
        logger.info(
            "Fibonacci base captured (first candle): high=%.2f low=%.2f range=%.2f",
            high, low, high - low,
        )

    def _maybe_reset_session(self, candle: Dict[str, Any]) -> None:
        d = _candle_date(candle)
        if d is None:
            return
        if self._session_date is not None and d != self._session_date:
            self.reset()

    # ── Market context (score / momentum / day-gamma) ───────────────────────────
    def _read_context(self) -> tuple:
        """Return ``(score, momentum, day_gamma_trend)`` from the context provider."""
        if self.context_provider is None:
            return 50.0, 0.0, None
        try:
            ctx = self.context_provider()
        except Exception:
            logger.exception("Fibonacci context provider failed; using neutrals")
            return 50.0, 0.0, None

        score = momentum = gamma = None
        if isinstance(ctx, dict):
            score = _to_float(ctx.get("score"))
            momentum = _to_float(ctx.get("momentum"))
            gamma = ctx.get("day_gamma_trend")
        elif isinstance(ctx, (tuple, list)) and len(ctx) >= 2:
            score = _to_float(ctx[0])
            momentum = _to_float(ctx[1])
            gamma = ctx[2] if len(ctx) >= 3 else None

        if isinstance(gamma, str):
            gamma = gamma.strip().upper() or None
        return (
            score if score is not None else 50.0,
            momentum if momentum is not None else 0.0,
            gamma,
        )

    # ── Entry / SL / targets ────────────────────────────────────────────────────
    def _build_trigger(
        self,
        signal: FibSignal,
        candle: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Assemble the Redis Pub/Sub payload (frontend-compatible, plus trade math)."""
        levels = {
            str(ratio): (round(price, 2) if price is not None else None)
            for ratio, price in self.fib_levels().items()
        }
        return {
            "action": "trade_trigger",
            "status": "BUY_NOW",
            "setup_type": signal.setup_name,
            "option_side": signal.option_side,
            "entry_price": round(signal.entry_price, 2) if signal.entry_price is not None else None,
            "stop_loss": round(signal.stop_loss, 2) if signal.stop_loss is not None else None,
            "target_1": round(signal.target_1, 2) if signal.target_1 is not None else None,
            "target_2": round(signal.target_2, 2) if signal.target_2 is not None else None,
            "fib_level": signal.fib_level,
            "fib_level_price": round(signal.fib_level_price, 2) if signal.fib_level_price is not None else None,
            "fib_levels": levels,
            "pattern": signal.pattern,
            "math_console_score": round(signal.math_console_score, 2) if signal.math_console_score is not None else None,
            "momentum": round(signal.momentum, 2) if signal.momentum is not None else None,
            "day_gamma_trend": signal.day_gamma_trend,
            "symbol": self.symbol,
            "confirm_candle": {
                key: candle.get(key)
                for key in ("open", "high", "low", "close", "ts_open", "ts_close")
            },
        }

    # ── Public API ──────────────────────────────────────────────────────────────
    def on_candle(self, candle: Dict[str, Any]) -> Optional[FibSignal]:
        """Handle a closed candle: capture base, evaluate, and dispatch when fired."""
        if not isinstance(candle, dict):
            return None
        if self.symbol and candle.get("symbol") != self.symbol:
            return None

        self._maybe_reset_session(candle)

        if not self.base_captured:
            self._capture_base(candle)
            # The base candle DEFINES the range — never trade on it.
            return None

        self._history.append(candle)
        signal = self.evaluate(candle)

        if signal is not None and signal.triggered:
            logger.info(
                "Fibonacci signal fired: setup=%s side=%s fib=%.3f reasons=%s",
                signal.setup_name, signal.option_side, signal.fib_level, signal.reasons,
            )
            if self.should_dispatch:
                dispatch_trade_trigger(signal.trigger)

        return signal

    def evaluate(self, candle: Dict[str, Any]) -> Optional[FibSignal]:
        """Evaluate the just-closed candle against all three strategies."""
        if not self.base_captured:
            return None

        prev_candle = self._history[-2] if len(self._history) >= 2 else None
        score, _momentum, gamma_trend = self._read_context()

        # ── Strategy A: Base Support Bounce (CE) ────────────────────────────────
        ce = self._evaluate_base_bounce(candle, prev_candle, score)
        if ce is not None:
            return ce

        # ── Strategy B: Exhaustion Reversal (PE) ────────────────────────────────
        pe = self._evaluate_exhaustion(candle, prev_candle)
        if pe is not None:
            return pe

        # ── Strategy C: Trend Retracement (gamma-gated, CE/PE) ──────────────────
        return self._evaluate_trend_retracement(candle, prev_candle, gamma_trend)

    def _evaluate_base_bounce(
        self,
        candle: Dict[str, Any],
        prev_candle: Optional[Dict[str, Any]],
        score: float,
    ) -> Optional[FibSignal]:
        level_price = self.fib_price(FIB_0_0)
        low = _to_float(candle.get("low"))
        if level_price is None or low is None:
            return None
        if not within_fib_zone(low, level_price, self.zone_buffer_pct):
            return None

        pattern = has_bullish_pattern(candle, prev_candle)
        if not pattern:
            return None

        score_ok = score > self.min_math_console_score
        if not score_ok:
            logger.info(
                "Fib CE rejected by data filter: score=%.2f <= %.2f",
                score, self.min_math_console_score,
            )
            return None

        entry = _to_float(candle.get("high"))
        stop = low
        target_1, target_2 = targets_for_ratio(
            self._base_low, self._base_high, FIB_0_0, "CE"
        )
        signal = FibSignal(
            triggered=True,
            setup_name=SETUP_DAY_LOW_REJECTION,
            option_side="CE",
            fib_level=FIB_0_0,
            fib_level_price=level_price,
            entry_price=entry,
            stop_loss=stop,
            target_1=target_1,
            target_2=target_2,
            pattern=pattern,
            math_console_score=score,
            momentum=None,
            reasons=[
                "zone:fib_0.00",
                f"pattern:{pattern}",
                f"score:{score:.2f}>{self.min_math_console_score}",
            ],
        )
        signal.trigger = self._build_trigger(signal, candle)
        return signal

    def _evaluate_exhaustion(
        self,
        candle: Dict[str, Any],
        prev_candle: Optional[Dict[str, Any]],
    ) -> Optional[FibSignal]:
        high = _to_float(candle.get("high"))
        if high is None:
            return None

        # Find which extension (1.618 / 2.618) the spike reached.
        hit_ratio: Optional[float] = None
        level_price: Optional[float] = None
        for ratio in (FIB_1_618, FIB_2_618):
            price = self.fib_price(ratio)
            if price is not None and within_fib_zone(high, price, self.zone_buffer_pct):
                hit_ratio = ratio
                level_price = price
                break
        if hit_ratio is None or level_price is None:
            return None

        pattern = has_bearish_pattern(candle, prev_candle)
        if not pattern:
            return None

        entry = _to_float(candle.get("low"))
        stop = high
        target_1, target_2 = targets_for_ratio(
            self._base_low, self._base_high, hit_ratio, "PE"
        )
        signal = FibSignal(
            triggered=True,
            setup_name=SETUP_FIB_EXTENSION_TRAP,
            option_side="PE",
            fib_level=hit_ratio,
            fib_level_price=level_price,
            entry_price=entry,
            stop_loss=stop,
            target_1=target_1,
            target_2=target_2,
            pattern=pattern,
            math_console_score=None,
            momentum=None,
            reasons=[
                f"zone:fib_{hit_ratio:g}",
                f"pattern:{pattern}",
            ],
        )
        signal.trigger = self._build_trigger(signal, candle)
        return signal

    # ── Strategy C: Trend Retracement (gamma-gated) ─────────────────────────────
    def _evaluate_trend_retracement(
        self,
        candle: Dict[str, Any],
        prev_candle: Optional[Dict[str, Any]],
        gamma_trend: Optional[str],
    ) -> Optional[FibSignal]:
        """Trend-retracement leg, gated by the live Day Gamma trend.

        POSITIVE gamma → bullish retracements only (CE on dips at 0.786/0.50/0.236).
        NEGATIVE gamma → bearish retracements only (PE on rallies at 0.236/0.50/0.786).
        """
        if not self.base_captured:
            return None
        trend = (str(gamma_trend) if gamma_trend else "").strip().upper()

        if trend == "POSITIVE":
            low = _to_float(candle.get("low"))
            if low is None:
                return None
            # Deepest retracement first (strongest bounce): 0.786 → 0.50 → 0.236.
            for ratio in reversed(RETRACEMENT_LEVELS):
                price = self.fib_price(ratio)
                if price is None or not within_fib_zone(low, price, self.zone_buffer_pct):
                    continue
                pattern = has_bullish_pattern(candle, prev_candle)
                if not pattern:
                    return None
                return self._retracement_signal(
                    candle, "CE", ratio, price, pattern, trend,
                    [f"gamma:{trend}", f"zone:fib_{ratio:g}", f"pattern:{pattern}"],
                )
            return None

        if trend == "NEGATIVE":
            high = _to_float(candle.get("high"))
            if high is None:
                return None
            # Shallowest rally first (strongest rejection): 0.236 → 0.50 → 0.786.
            for ratio in RETRACEMENT_LEVELS:
                price = self.fib_price(ratio)
                if price is None or not within_fib_zone(high, price, self.zone_buffer_pct):
                    continue
                pattern = has_bearish_pattern(candle, prev_candle)
                if not pattern:
                    return None
                return self._retracement_signal(
                    candle, "PE", ratio, price, pattern, trend,
                    [f"gamma:{trend}", f"zone:fib_{ratio:g}", f"pattern:{pattern}"],
                )
            return None

        return None

    def _retracement_signal(
        self,
        candle: Dict[str, Any],
        side: str,
        ratio: float,
        level_price: float,
        pattern: str,
        gamma_trend: str,
        reasons: List[str],
    ) -> FibSignal:
        """Build (and trigger-ize) a trend-retracement signal with full trade math."""
        high = _to_float(candle.get("high"))
        low = _to_float(candle.get("low"))
        entry = high if side == "CE" else low
        stop = low if side == "CE" else high
        target_1, target_2 = retracement_targets(
            self._base_low, self._base_high, ratio, side
        )
        setup = (
            BULLISH_RETRACE_SETUPS[ratio] if side == "CE" else BEARISH_RETRACE_SETUPS[ratio]
        )
        signal = FibSignal(
            triggered=True,
            setup_name=setup,
            option_side=side,
            fib_level=ratio,
            fib_level_price=level_price,
            entry_price=entry,
            stop_loss=stop,
            target_1=target_1,
            target_2=target_2,
            pattern=pattern,
            math_console_score=None,
            momentum=None,
            day_gamma_trend=gamma_trend,
            reasons=reasons,
        )
        signal.trigger = self._build_trigger(signal, candle)
        return signal

# ── Self-test (no Redis / no Django required) ───────────────────────────────────
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")

    def _show(engine: FibonacciSwingEngine, label: str, signal: Optional[FibSignal]) -> None:
        print(f"\n[{label}]")
        print("  base  :", engine.base_low, "->", engine.base_high)
        print(
            "  fibs  :",
            {f"{r:g}": round(p, 2) for r, p in engine.fib_levels().items() if p is not None},
        )
        if signal is None or not signal.triggered:
            print("  signal: none")
            return
        print(
            "  signal:", signal.setup_name, signal.option_side,
            f"entry={signal.entry_price} sl={signal.stop_loss}",
            f"t1={signal.target_1} t2={signal.target_2}",
        )
        print("  trigger keys:", sorted(signal.trigger.keys()))

    # Controlled context: score 55 (CE passes), momentum 35 (PE passes).
    engine = FibonacciSwingEngine(
        symbol="NSE_FO|TEST",
        context_provider=lambda: {"score": 55.0, "momentum": 35.0},
        dispatch=False,
    )

    # First candle 09:15-09:16 defines the base range.
    engine.on_candle({
        "symbol": "NSE_FO|TEST", "ts_open": "2026-10-09T09:15:00+05:30",
        "open": 22500.0, "high": 22580.0, "low": 22490.0, "close": 22550.0,
    })

    # A) Bullish pin bar at the 0.00 fib (base low ~22490).
    ce_signal = engine.on_candle({
        "symbol": "NSE_FO|TEST", "ts_open": "2026-10-09T10:15:00+05:30",
        "open": 22520.0, "high": 22525.0, "low": 22488.0, "close": 22522.0,
    })
    _show(engine, "A) Base Support Bounce (expect CE trigger)", ce_signal)

    # B) Bearish pin bar at the 1.618 fib (~22635.62).
    pe_signal = engine.on_candle({
        "symbol": "NSE_FO|TEST", "ts_open": "2026-10-09T11:00:00+05:30",
        "open": 22620.0, "high": 22640.0, "low": 22610.0, "close": 22613.0,
    })
    _show(engine, "B) Fib Extension Trap (expect PE trigger)", pe_signal)

    # C) Trend Retracement — POSITIVE gamma → bullish bounce at 0.50 (~22535).
    pos_engine = FibonacciSwingEngine(
        symbol="NSE_FO|TEST",
        context_provider=lambda: {"score": 50.0, "momentum": 0.0, "day_gamma_trend": "POSITIVE"},
        dispatch=False,
    )
    pos_engine.on_candle({
        "symbol": "NSE_FO|TEST", "ts_open": "2026-10-09T09:15:00+05:30",
        "open": 22500.0, "high": 22580.0, "low": 22490.0, "close": 22550.0,
    })
    retrace_ce = pos_engine.on_candle({
        "symbol": "NSE_FO|TEST", "ts_open": "2026-10-09T10:30:00+05:30",
        "open": 22545.0, "high": 22546.0, "low": 22534.0, "close": 22544.0,
    })
    _show(pos_engine, "C) Trend Retracement POSITIVE (expect Medium Bullish Bounce 0.50)", retrace_ce)

    # D) Trend Retracement — NEGATIVE gamma → bearish reject at 0.50 (~22535).
    neg_engine = FibonacciSwingEngine(
        symbol="NSE_FO|TEST",
        context_provider=lambda: {"score": 50.0, "momentum": 0.0, "day_gamma_trend": "NEGATIVE"},
        dispatch=False,
    )
    neg_engine.on_candle({
        "symbol": "NSE_FO|TEST", "ts_open": "2026-10-09T09:15:00+05:30",
        "open": 22500.0, "high": 22580.0, "low": 22490.0, "close": 22550.0,
    })
    retrace_pe = neg_engine.on_candle({
        "symbol": "NSE_FO|TEST", "ts_open": "2026-10-09T11:30:00+05:30",
        "open": 22533.0, "high": 22540.0, "low": 22531.0, "close": 22532.0,
    })
    _show(neg_engine, "D) Trend Retracement NEGATIVE (expect Medium Bearish Reject 0.50)", retrace_pe)

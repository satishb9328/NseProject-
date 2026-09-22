"""
chart_patterns.py — Macroscopic chart-pattern detection (Phase 2).
===================================================================

Isolated module for multi-bar chart formations (e.g. the Double Bottom / "W"
pattern) that require swing/pivot analysis rather than the 1-3 candle shapes
handled by candlestick_scanner.py.

Everything is vectorized with NumPy/SciPy; there are no per-row Python loops.
The public API mirrors the candlestick scanner: each detector returns a
``pandas.Series`` of booleans aligned to the input DataFrame index.

Pivot convention
----------------
A swing low/high is a bar that is strictly lower/higher than ``order`` bars on
BOTH sides (``order=5`` -> a 5-bar strict neighbourhood on each side).

Usage
-----
    from chart_patterns import detect_double_bottom

    df["double_bottom"] = detect_double_bottom(df)   # bool Series, len == df
"""

from __future__ import annotations

import numpy as np
import pandas as pd

__all__ = ["detect_double_bottom", "find_swing_lows", "find_swing_highs"]

# ────────────────────────────────────────────────────────────────────────────────
# DEFAULTS
# ────────────────────────────────────────────────────────────────────────────────

DEFAULT_ORDER = 5          # pivot neighbourhood: 5 bars strictly on each side
DEFAULT_TOLERANCE = 0.03   # Low1 vs Low2 must be within 3% of each other
DEFAULT_MIN_GAP = 10       # min trading days between the two lows
DEFAULT_MAX_GAP = 60       # max trading days between the two lows
BOUNCE_PCT = 0.02          # a "strong bounce" off Low2 = close >= Low2 * (1 + 2%)


# ────────────────────────────────────────────────────────────────────────────────
# SWING PIVOT DETECTION (vectorized)
# ────────────────────────────────────────────────────────────────────────────────

def _pivot_indices_numpy(values: np.ndarray, order: int, *, is_low: bool) -> np.ndarray:
    """Pure-NumPy fallback: strictly lower/higher than ``order`` bars on both sides."""
    n = values.shape[0]
    idx = np.arange(n)
    mask = np.ones(n, dtype=bool)
    for k in range(1, order + 1):
        left_ok = np.zeros(n, dtype=bool)
        right_ok = np.zeros(n, dtype=bool)
        if is_low:
            left_ok[k:] = values[k:] < values[:-k]
            right_ok[: n - k] = values[: n - k] < values[k:]
        else:
            left_ok[k:] = values[k:] > values[:-k]
            right_ok[: n - k] = values[: n - k] > values[k:]
        mask &= left_ok & right_ok
    return idx[mask]


def _pivot_indices(values: np.ndarray, order: int, *, is_low: bool) -> np.ndarray:
    """Return sorted integer indices of swing pivots for ``values``."""
    order = int(order)
    n = values.shape[0]
    if order < 1 or n < 2 * order + 1:
        return np.array([], dtype=np.int64)

    try:
        from scipy.signal import argrelextrema

        comparator = np.less if is_low else np.greater
        idx = argrelextrema(values, comparator, order=order)[0].astype(np.int64)
    except Exception:  # noqa: BLE001 — scipy missing / import error
        idx = _pivot_indices_numpy(values, order, is_low=is_low)

    # Keep only fully-interior pivots (a full ``order``-bar neighbourhood on both
    # sides), otherwise boundary bars can be flagged without enough left context.
    return idx[(idx >= order) & (idx <= n - 1 - order)]


def find_swing_lows(low, order: int = DEFAULT_ORDER) -> np.ndarray:
    """Return indices where ``low`` is strictly lower than ``order`` bars each side."""
    return _pivot_indices(np.asarray(low, dtype=float), order, is_low=True)


def find_swing_highs(high, order: int = DEFAULT_ORDER) -> np.ndarray:
    """Return indices where ``high`` is strictly higher than ``order`` bars each side."""
    return _pivot_indices(np.asarray(high, dtype=float), order, is_low=False)


# ────────────────────────────────────────────────────────────────────────────────
# DOUBLE BOTTOM (W-PATTERN)
# ────────────────────────────────────────────────────────────────────────────────

def detect_double_bottom(
    df: pd.DataFrame,
    tolerance_pct: float = DEFAULT_TOLERANCE,
    min_gap: int = DEFAULT_MIN_GAP,
    max_gap: int = DEFAULT_MAX_GAP,
    order: int = DEFAULT_ORDER,
) -> pd.Series:
    """Return a boolean Series marking Double Bottom (W-pattern) triggers.

    A valid double bottom needs two consecutive swing lows (Low1 then Low2) that
    are:

      * nearly equal  — |Low2 - Low1| / max(Low1, Low2) <= ``tolerance_pct``
      * spaced apart — ``min_gap < idx(Low2) - idx(Low1) < max_gap`` trading bars

    with a neckline equal to the highest swing high between them.

    Two low-noise triggers are emitted, both bounded to fire **strictly within
    ``max_gap`` bars after Low2**:

      * BREAKOUT — the FIRST close above the neckline after Low2 (flagged on that
        single day only; never re-fires for the same pattern).
      * BOUNCE   — close rallies ``BOUNCE_PCT`` above Low2 while the recent 3-bar
        low still sits in the Low2 zone (the right side of the "W" is forming).

    Returns a boolean Series aligned to ``df.index`` (all ``False`` if there are
    too few pivots to form a pattern).
    """
    for col in ("Low", "High", "Close"):
        if col not in df.columns:
            raise ValueError(
                f"chart_patterns.detect_double_bottom: missing column {col!r}"
            )

    n = len(df)
    low = df["Low"].astype(float)
    high = df["High"].astype(float)
    close = df["Close"].astype(float)

    order = int(order)
    result_arr = np.zeros(n, dtype=bool)
    if n < 2 * order + 1:
        return pd.Series(result_arr, index=df.index, name="double_bottom")

    low_idx = _pivot_indices(low.to_numpy(), order, is_low=True)
    high_idx = _pivot_indices(high.to_numpy(), order, is_low=False)

    # Not enough pivots to define a W.
    if len(low_idx) < 2:
        return pd.Series(result_arr, index=df.index, name="double_bottom")

    low_arr = low.to_numpy()
    high_arr = high.to_numpy()
    close_arr = close.to_numpy()
    recent_low = low.rolling(window=3, min_periods=1).min().to_numpy()

    # Consecutive swing-low pairs -> Low1 (earlier) and Low2 (later).
    l1_idx = low_idx[:-1]
    l2_idx = low_idx[1:]
    l1_px = low_arr[l1_idx]
    l2_px = low_arr[l2_idx]

    gap = l2_idx - l1_idx
    rel_diff = np.abs(l2_px - l1_px) / np.maximum(l1_px, l2_px)
    valid = (gap > min_gap) & (gap < max_gap) & (rel_diff <= tolerance_pct)

    # First-cross state is tracked in a tiny loop over the (few) candidate pairs;
    # pivot detection and pair filtering remain fully vectorized.
    for i in np.flatnonzero(valid):
        a = int(l1_idx[i])
        b = int(l2_idx[i])
        l2_price = float(l2_px[i])

        # Neckline = highest swing HIGH strictly between Low1 and Low2.
        between = (high_idx > a) & (high_idx < b)
        if not between.any():
            continue
        neck = float(high_arr[high_idx[between]].max())
        if neck <= l2_price:
            continue

        # Bounded window: strictly within ``max_gap`` bars after Low2.
        end = min(b + max_gap, n)
        if end <= b:
            continue

        window_close = close_arr[b:end]

        # BREAKOUT — first close above the neckline only (single day, no re-fire).
        above = window_close > neck
        if above.any():
            result_arr[b + int(np.argmax(above))] = True

        # BOUNCE — strong rally off Low2 while the 3-bar low is still in the zone.
        bounce = (window_close > l2_price * (1.0 + BOUNCE_PCT)) & (
            recent_low[b:end] <= l2_price * (1.0 + tolerance_pct)
        )
        result_arr[b:end] |= bounce

    return pd.Series(result_arr, index=df.index, name="double_bottom")


# ────────────────────────────────────────────────────────────────────────────────
# SELF-TEST (run:  python chart_patterns.py)
# ────────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    # Deterministic synthetic "W": decline -> Low1, rally -> neckline, decline
    # -> Low2 (~equal to Low1), then rally -> breakout above the neckline.
    n = 220
    x = np.array([0, 40, 60, 90, 120, n - 1])
    y = np.array([100.0, 80.0, 90.0, 82.0, 95.0, 95.0])
    close = pd.Series(np.interp(np.arange(n), x, y))

    df = pd.DataFrame(
        {
            "Open": close.shift(1).fillna(close.iloc[0]),
            "High": close + 1.0,
            "Low": close - 1.0,
            "Close": close,
        }
    )

    sig = detect_double_bottom(df)
    hit_days = np.flatnonzero(sig.to_numpy())

    print("rows:", len(df))
    print("swing lows:", find_swing_lows(df["Low"]).tolist())
    print("swing highs:", find_swing_highs(df["High"]).tolist())
    print("double_bottom True days:", hit_days.tolist())
    print("dtype:", sig.dtype, "| no NaN:", bool(sig.notna().all()))



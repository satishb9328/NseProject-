r"""
╔══════════════════════════════════════════════════════════════════════════════════╗
║   MULTI-TIMEFRAME (MTF) CANDLESTICK PATTERN SCANNER                              ║
║   Fully vectorized (NumPy / Pandas boolean masking) — STRICTLY NO for-loops.     ║
║                                                                                  ║
║   Each pattern is only valid when it prints at a LOGICAL SUPPORT AREA.           ║
║   The strongest setups occur when a DAILY pattern tests a WEEKLY support.        ║
║                                                                                  ║
║   PATTERNS:                                                                      ║
║     1. Valid Hammer            (bullish pin bar)   — 1 candle                    ║
║     2. Valid Bullish Engulfing                     — 2 candles                   ║
║     3. Valid Morning Star                          — 3 candles                   ║
║     4. Fresh Golden Cross (Daily 50/200 EMA)       — 15-day window               ║
║     5. Weekly Bullish Hammer                       — weekly pin bar              ║
║     6. Weekly Bullish Engulfing                    — weekly 2 candles            ║
║     7. Weekly Bearish Engulfing                    — weekly 2 candles            ║
║                                                                                  ║
║   Every detector returns a ``pandas.Series`` of booleans aligned to the input    ║
║   DataFrame index, so results can be attached to a Django QuerySet / DataFrame   ║
║   with a single assignment.                                                      ║
╚══════════════════════════════════════════════════════════════════════════════════╝

Expected input columns (already merged Daily + Weekly data):
    Open, High, Low, Close,
    Daily_EMA_50, Daily_EMA_200, Weekly_EMA_50, Daily_RSI,
    Weekly_Open, Weekly_High, Weekly_Low, Weekly_Close

Usage
-----
    from candlestick_scanner import scan_mtf_patterns

    signals = scan_mtf_patterns(df)                 # -> DataFrame of booleans
    df["is_hammer"]            = signals["hammer"]
    df["is_bullish_engulfing"] = signals["bullish_engulfing"]
    df["is_morning_star"]      = signals["morning_star"]
    df["fresh_golden_cross"]   = signals["fresh_golden_cross"]
    df["weekly_hammer"]        = signals["weekly_hammer"]
    df["weekly_bullish_engulfing"] = signals["weekly_bullish_engulfing"]
    df["weekly_bearish_engulfing"] = signals["weekly_bearish_engulfing"]
    df["any_pattern"]          = signals["any_pattern"]
"""

from __future__ import annotations

import numpy as np
import pandas as pd

__all__ = [
    "MTFCandleScanner",
    "detect_hammer",
    "detect_bullish_engulfing",
    "detect_morning_star",
    "detect_fresh_golden_cross",
    "detect_weekly_hammer",
    "detect_weekly_bullish_engulfing",
    "detect_weekly_bearish_engulfing",
    "scan_mtf_patterns",
]

# ────────────────────────────────────────────────────────────────────────────────
# COLUMN CONTRACT & DEFAULT THRESHOLDS
# ────────────────────────────────────────────────────────────────────────────────

REQUIRED_COLUMNS = (
    "Open",
    "High",
    "Low",
    "Close",
    "Daily_EMA_50",
    "Weekly_EMA_50",
    "Daily_RSI",
    "Daily_EMA_200",
    "Weekly_Open",
    "Weekly_High",
    "Weekly_Low",
    "Weekly_Close",
)

DEFAULT_EMA_MARGIN = 0.015  # 1.5% proximity band around an EMA level

HAMMER_RSI_MAX = 40.0
ENGULFING_RSI_MAX = 45.0
MORNING_STAR_RSI_MAX = 40.0

# Morning-star candle-strength multipliers (relative to a rolling mean body).
STRONG_BODY_MULT = 1.25  # C1 / C3 bodies must be >= 1.25x the average body
SMALL_BODY_MULT = 0.5    # C2 body must be <= 0.50x the C1 body (small / doji)
BODY_LOOKBACK = 20       # rolling window used to compute the "average body"

# Fresh Golden Cross (Daily 50 EMA / 200 EMA).
GOLDEN_CROSS_LOOKBACK = 15  # crossover must print within the last N trading days

# Weekly bearish-engulfing location thresholds.
BEARISH_ENGULFING_RSI_MIN = 70.0     # Daily_RSI > 70 → overbought proxy
BEARISH_ENGULFING_FAR_MARGIN = 0.03  # weekly close must be >= 3% above Weekly_EMA_50

# ────────────────────────────────────────────────────────────────────────────────
# HELPERS (pure vectorization)
# ────────────────────────────────────────────────────────────────────────────────

def _validate_columns(df: pd.DataFrame) -> None:
    """Raise a clear error if any required column is missing."""
    missing = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    if missing:
        raise ValueError(
            f"candlestick_scanner: missing required columns {missing}; "
            f"expected {list(REQUIRED_COLUMNS)}"
        )


def _near_level(price: pd.Series, level: pd.Series, margin: float) -> pd.Series:
    """Return True where ``price`` is within ``margin`` percent of ``level``.

    Vectorized and NaN-safe: rows where either series is NaN (or the level is
    zero) evaluate to False instead of raising or leaking NaN.
    """
    level = level.astype(float).replace(0.0, np.nan)
    distance = (price.astype(float) - level).abs() / level.abs()
    return distance <= float(margin)


def _far_above_level(price: pd.Series, level: pd.Series, margin: float) -> pd.Series:
    """Return True where ``price`` is at least ``margin`` percent ABOVE ``level``.

    Vectorized and NaN-safe: rows where the level is NaN (or zero) evaluate to
    False instead of leaking NaN.
    """
    level = level.astype(float).replace(0.0, np.nan)
    return price.astype(float) >= level * (1.0 + float(margin))


# ────────────────────────────────────────────────────────────────────────────────
# SCANNER
# ────────────────────────────────────────────────────────────────────────────────

class MTFCandleScanner:
    """Vectorized multi-timeframe candlestick pattern scanner.

    The scanner derives all candle anatomy (real body, wicks, ranges) and the
    three reversal patterns entirely through NumPy ufuncs and Pandas boolean
    masking — no Python ``for`` loops are used anywhere in the scanning logic.

    Parameters
    ----------
    df : pandas.DataFrame
        Merged Daily + Weekly frame. Must contain the ``REQUIRED_COLUMNS``.
    ema_margin : float, optional
        Proximity band (as a fraction) used to decide whether a Low is "near"
        an EMA. Default 0.015 (i.e. 1.5%).
    hammer_rsi_max : float, optional
        RSI ceiling that qualifies a hammer location when no EMA is nearby.
    engulfing_rsi_max : float, optional
        RSI ceiling that qualifies a bullish engulfing location.
    morning_star_rsi_max : float, optional
        RSI ceiling that qualifies a morning-star base when it is not testing
        the Weekly EMA.
    strong_body_mult : float, optional
        Multiplier over the rolling mean body that marks a candle as "strong".
    small_body_mult : float, optional
        Max ratio of C2 body vs C1 body that marks the middle candle as small.
    body_lookback : int, optional
        Rolling window (bars) used to compute the "average body".
    """

    def __init__(
        self,
        df: pd.DataFrame,
        *,
        ema_margin: float = DEFAULT_EMA_MARGIN,
        hammer_rsi_max: float = HAMMER_RSI_MAX,
        engulfing_rsi_max: float = ENGULFING_RSI_MAX,
        morning_star_rsi_max: float = MORNING_STAR_RSI_MAX,
        strong_body_mult: float = STRONG_BODY_MULT,
        small_body_mult: float = SMALL_BODY_MULT,
        body_lookback: int = BODY_LOOKBACK,
        golden_cross_lookback: int = GOLDEN_CROSS_LOOKBACK,
        bearish_rsi_min: float = BEARISH_ENGULFING_RSI_MIN,
        bearish_far_margin: float = BEARISH_ENGULFING_FAR_MARGIN,
    ) -> None:
        _validate_columns(df)

        self.df = df
        self.ema_margin = float(ema_margin)
        self.hammer_rsi_max = float(hammer_rsi_max)
        self.engulfing_rsi_max = float(engulfing_rsi_max)
        self.morning_star_rsi_max = float(morning_star_rsi_max)
        self.strong_body_mult = float(strong_body_mult)
        self.small_body_mult = float(small_body_mult)
        self.body_lookback = int(body_lookback)
        self.golden_cross_lookback = int(golden_cross_lookback)
        self.bearish_rsi_min = float(bearish_rsi_min)
        self.bearish_far_margin = float(bearish_far_margin)

        # ── OHLC & context series (single vectorized extraction) ───────────────
        self.open_ = df["Open"].astype(float)
        self.high = df["High"].astype(float)
        self.low = df["Low"].astype(float)
        self.close = df["Close"].astype(float)
        self.daily_ema = df["Daily_EMA_50"].astype(float)
        self.weekly_ema = df["Weekly_EMA_50"].astype(float)
        self.rsi = df["Daily_RSI"].astype(float)
        self.daily_ema_200 = df["Daily_EMA_200"].astype(float)
        self.weekly_open = df["Weekly_Open"].astype(float)
        self.weekly_high = df["Weekly_High"].astype(float)
        self.weekly_low = df["Weekly_Low"].astype(float)
        self.weekly_close = df["Weekly_Close"].astype(float)

        # ── Candle anatomy (vectorized) ────────────────────────────────────────
        self.body = (self.close - self.open_).abs()               # real body
        self.lower_wick = np.minimum(self.open_, self.close) - self.low
        self.upper_wick = self.high - np.maximum(self.open_, self.close)
        self.range = self.high - self.low

        # ── Weekly candle anatomy (vectorized) ─────────────────────────────────
        self.weekly_body = (self.weekly_close - self.weekly_open).abs()
        self.weekly_lower_wick = (
            np.minimum(self.weekly_open, self.weekly_close) - self.weekly_low
        )
        self.weekly_upper_wick = (
            self.weekly_high - np.maximum(self.weekly_open, self.weekly_close)
        )
        self.weekly_range = self.weekly_high - self.weekly_low

    # ── 1. VALID HAMMER (Bullish Pin Bar) ──────────────────────────────────────
    def valid_hammer(self) -> pd.Series:
        """Bullish pin bar printing at a Daily/Weekly support or oversold RSI.

        Shape  : lower wick >= 2x real body AND upper wick <= 0.5x real body.
        Location: Daily Low near Daily_EMA_50 OR Weekly_EMA_50 (1.5% band)
                  OR Daily_RSI < 40.
        """
        shape = (
            (self.body > 0)
            & (self.lower_wick >= 2.0 * self.body)
            & (self.upper_wick <= 0.5 * self.body)
        )

        near_daily = _near_level(self.low, self.daily_ema, self.ema_margin)
        near_weekly = _near_level(self.low, self.weekly_ema, self.ema_margin)
        location = near_daily | near_weekly | (self.rsi < self.hammer_rsi_max)

        return (shape & location).fillna(False)

    # ── 2. VALID BULLISH ENGULFING ─────────────────────────────────────────────
    def valid_bullish_engulfing(self) -> pd.Series:
        """Two-candle bullish reversal at a support area.

        Shape  : previous candle Red, current candle Green, and the current
                 real body completely engulfs the previous real body.
        Location: Daily Low near Weekly_EMA_50 OR Daily_EMA_50 (1.5% band)
                  OR Daily_RSI < 45.
        """
        prev_open = self.open_.shift(1)
        prev_close = self.close.shift(1)

        prev_red = prev_close < prev_open
        curr_green = self.close > self.open_
        # Engulf: current open at/below previous close AND current close at/above
        # previous open (the classic complete body engulf).
        engulfs = (self.open_ <= prev_close) & (self.close >= prev_open)
        shape = prev_red & curr_green & engulfs

        near_daily = _near_level(self.low, self.daily_ema, self.ema_margin)
        near_weekly = _near_level(self.low, self.weekly_ema, self.ema_margin)
        location = near_daily | near_weekly | (self.rsi < self.engulfing_rsi_max)

        return (shape & location).fillna(False)

    # ── 3. VALID MORNING STAR (3-Candle Pattern) ───────────────────────────────
    def valid_morning_star(self) -> pd.Series:
        """Three-candle bottom reversal confirmed inside a short-term downtrend.

        Shape:
          C1 (t-2)  strong Red body.
          C2 (t-1)  small body / doji that gaps or trades lower than C1.
          C3 (t)    strong Green body closing deep (above the midpoint) into
                    C1's real body.
        Location:
          A short-term downtrend (two declining closes into C1) where the base
          (C2's Low) is testing Weekly_EMA_50 OR Daily_RSI < 40.
        """
        o = self.open_
        c = self.close
        body = self.body

        o1, c1 = o.shift(2), c.shift(2)
        o2, c2 = o.shift(1), c.shift(1)
        l2 = self.low.shift(1)
        body1 = body.shift(2)
        body2 = body.shift(1)

        avg_body = body.rolling(window=self.body_lookback, min_periods=1).mean()

        # Candle 1 — strong red
        c1_red = c1 < o1
        c1_strong = body1 >= self.strong_body_mult * avg_body

        # Candle 2 — small body/doji, gapping or trading lower
        c2_small = body2 <= self.small_body_mult * body1
        c2_gap_or_lower = (o2 < c1) | (c2 < c1)

        # Candle 3 — strong green pushing deep into C1's body
        c3_green = c > o
        c3_strong = body >= self.strong_body_mult * avg_body
        c1_midpoint = (o1 + c1) / 2.0
        c3_deep = c > c1_midpoint

        shape = (
            c1_red
            & c1_strong
            & c2_small
            & c2_gap_or_lower
            & c3_green
            & c3_strong
            & c3_deep
        )

        # Location — short-term downtrend + base testing weekly support / oversold
        downtrend = (c.shift(4) > c.shift(3)) & (c.shift(3) > c.shift(2))
        base_near_weekly = _near_level(l2, self.weekly_ema, self.ema_margin)
        location = downtrend & (
            base_near_weekly | (self.rsi < self.morning_star_rsi_max)
        )

        return (shape & location).fillna(False)

    # ── 4. FRESH GOLDEN CROSS (Daily 50/200 EMA) ──────────────────────────────
    def fresh_golden_cross(self, lookback_days: int | None = None) -> pd.Series:
        """Bullish trend confirmation: a *fresh* Daily Golden Cross.

        A Golden Cross is the 50 EMA crossing above the 200 EMA. We only flag
        it while it is still "fresh" — i.e. the actual crossover bar printed
        within the last ``lookback_days`` trading days AND the 50 EMA is still
        above the 200 EMA today.
        """
        lookback = int(
            self.golden_cross_lookback if lookback_days is None else lookback_days
        )
        if lookback < 1:
            raise ValueError("candlestick_scanner: lookback_days must be >= 1")
        
        ema50 = self.daily_ema
        ema200 = self.daily_ema_200

        # Exact crossover bar: 50 EMA moves from <= to > the 200 EMA.
        crossover = ((ema50 > ema200) & (ema50.shift(1) <= ema200.shift(1))).astype(
            np.int8
        )

        # Flag the crossover bar + the following (lookback - 1) bars as "fresh".
        fresh = (
            crossover.rolling(window=lookback, min_periods=1)
            .max()
            .fillna(0)
            .astype(bool)
        )

        # Current alignment must still be bullish (50 EMA > 200 EMA).
        return (fresh & (ema50 > ema200)).fillna(False)

    # ── 5. WEEKLY BULLISH HAMMER ───────────────────────────────────────────────
    def weekly_hammer(self) -> pd.Series:
        """Weekly bullish pin bar printing near Weekly_EMA_50 support."""
        shape = (
            (self.weekly_body > 0)
            & (self.weekly_lower_wick >= 2.0 * self.weekly_body)
            & (self.weekly_upper_wick <= 0.5 * self.weekly_body)
        )
        near_weekly = _near_level(self.weekly_low, self.weekly_ema, self.ema_margin)
        return (shape & near_weekly).fillna(False)

    # ── 6. WEEKLY BULLISH ENGULFING ────────────────────────────────────────────
    def weekly_bullish_engulfing(self) -> pd.Series:
        """Weekly Red→Green engulfing candle at Weekly_EMA_50 support."""
        prev_open = self.weekly_open.shift(1)
        prev_close = self.weekly_close.shift(1)

        prev_red = prev_close < prev_open
        curr_green = self.weekly_close > self.weekly_open
        engulfs = (self.weekly_open <= prev_close) & (self.weekly_close >= prev_open)
        shape = prev_red & curr_green & engulfs

        near_weekly = _near_level(self.weekly_low, self.weekly_ema, self.ema_margin)
        return (shape & near_weekly).fillna(False)

    # ── 7. WEEKLY BEARISH ENGULFING ────────────────────────────────────────────
    def weekly_bearish_engulfing(self) -> pd.Series:
        """Weekly Green→Red engulfing candle far above Weekly_EMA_50 / overbought."""
        prev_open = self.weekly_open.shift(1)
        prev_close = self.weekly_close.shift(1)

        prev_green = prev_close > prev_open
        curr_red = self.weekly_close < self.weekly_open
        engulfs = (self.weekly_open >= prev_close) & (self.weekly_close <= prev_open)
        shape = prev_green & curr_red & engulfs

        far_above = _far_above_level(
            self.weekly_close, self.weekly_ema, self.bearish_far_margin
        )
        overbought = self.rsi > self.bearish_rsi_min
        location = far_above | overbought

        return (shape & location).fillna(False)

    # ── COMBINED OUTPUT ────────────────────────────────────────────────────────
    def scan(self) -> pd.DataFrame:
        """Return one boolean column per pattern (plus an ``any_pattern`` OR)."""
        hammer = self.valid_hammer()
        bullish_engulfing = self.valid_bullish_engulfing()
        morning_star = self.valid_morning_star()
        fresh_golden_cross = self.fresh_golden_cross()
        weekly_hammer = self.weekly_hammer()
        weekly_bullish_engulfing = self.weekly_bullish_engulfing()
        weekly_bearish_engulfing = self.weekly_bearish_engulfing()

        return pd.DataFrame(
            {
                "hammer": hammer,
                "bullish_engulfing": bullish_engulfing,
                "morning_star": morning_star,
                "fresh_golden_cross": fresh_golden_cross,
                "weekly_hammer": weekly_hammer,
                "weekly_bullish_engulfing": weekly_bullish_engulfing,
                "weekly_bearish_engulfing": weekly_bearish_engulfing,
                "any_pattern": (
                    hammer
                    | bullish_engulfing
                    | morning_star
                    | fresh_golden_cross
                    | weekly_hammer
                    | weekly_bullish_engulfing
                    | weekly_bearish_engulfing
                ),
            }
        )

# ────────────────────────────────────────────────────────────────────────────────
# MODULE-LEVEL CONVENIENCE FUNCTIONS
# ────────────────────────────────────────────────────────────────────────────────

def detect_hammer(df: pd.DataFrame, **kwargs) -> pd.Series:
    """Return a boolean Series of Valid Hammer signals for ``df``."""
    return MTFCandleScanner(df, **kwargs).valid_hammer()


def detect_bullish_engulfing(df: pd.DataFrame, **kwargs) -> pd.Series:
    """Return a boolean Series of Valid Bullish Engulfing signals for ``df``."""
    return MTFCandleScanner(df, **kwargs).valid_bullish_engulfing()


def detect_morning_star(df: pd.DataFrame, **kwargs) -> pd.Series:
    """Return a boolean Series of Valid Morning Star signals for ``df``."""
    return MTFCandleScanner(df, **kwargs).valid_morning_star()


def detect_fresh_golden_cross(
    df: pd.DataFrame, lookback_days: int = 15, **kwargs
) -> pd.Series:
    """Return a boolean Series of Fresh Golden Cross signals for ``df``."""
    return MTFCandleScanner(df, **kwargs).fresh_golden_cross(
        lookback_days=lookback_days
    )

def detect_weekly_hammer(df: pd.DataFrame, **kwargs) -> pd.Series:
    """Return a boolean Series of Weekly Bullish Hammer signals for ``df``."""
    return MTFCandleScanner(df, **kwargs).weekly_hammer()

def detect_weekly_bullish_engulfing(df: pd.DataFrame, **kwargs) -> pd.Series:
    """Return a boolean Series of Weekly Bullish Engulfing signals for ``df``."""
    return MTFCandleScanner(df, **kwargs).weekly_bullish_engulfing()

def detect_weekly_bearish_engulfing(df: pd.DataFrame, **kwargs) -> pd.Series:
    """Return a boolean Series of Weekly Bearish Engulfing signals for ``df``."""
    return MTFCandleScanner(df, **kwargs).weekly_bearish_engulfing()

def scan_mtf_patterns(df: pd.DataFrame, **kwargs) -> pd.DataFrame:
    """Return a DataFrame of boolean pattern columns aligned to ``df.index``."""
    return MTFCandleScanner(df, **kwargs).scan()

# ────────────────────────────────────────────────────────────────────────────────
# SELF-TEST (run:  python candlestick_scanner.py)
# ────────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    rng = np.random.default_rng(42)
    n = 260

    idx = pd.date_range(end="2026-09-16", periods=n, freq="B")

    # Synthetic OHLC random walk (vectorized, no loops).
    rets = rng.normal(0.0004, 0.015, size=n)
    close = pd.Series(100.0 * np.exp(np.cumsum(rets)), index=idx, name="Close")
    open_ = (close.shift(1) * (1 + rng.normal(0, 0.002, size=n))).fillna(
        close.iloc[0]
    )
    high = np.maximum(open_, close) * (1 + np.abs(rng.normal(0, 0.004, size=n)))
    low = np.minimum(open_, close) * (1 - np.abs(rng.normal(0, 0.004, size=n)))

    # Vectorized indicators (matching the merged-frame contract).
    daily_ema = close.ewm(span=50, adjust=False).mean()
    daily_ema_200 = close.ewm(span=200, adjust=False).mean()
    weekly_close = close.resample("W-FRI").last()
    weekly_ema = (
        weekly_close.ewm(span=50, adjust=False).mean()
        .reindex(idx, method="ffill")
        .ffill()
    )

    # Weekly OHLC bars (up-sampled back onto the daily index via forward-fill).
    weekly_open = open_.resample("W-FRI").first().reindex(idx, method="ffill").ffill()
    weekly_high = high.resample("W-FRI").max().reindex(idx, method="ffill").ffill()
    weekly_low = low.resample("W-FRI").min().reindex(idx, method="ffill").ffill()

    delta = close.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1 / 14, min_periods=14, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / 14, min_periods=14, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0.0, np.nan)
    rsi = (100 - 100 / (1 + rs)).fillna(50.0)

    df = pd.DataFrame(
        {
            "Open": open_,
            "High": high,
            "Low": low,
            "Close": close,
            "Daily_EMA_50": daily_ema,
            "Weekly_EMA_50": weekly_ema,
            "Daily_RSI": rsi,
            "Daily_EMA_200": daily_ema_200,
            "Weekly_Open": weekly_open,
            "Weekly_High": weekly_high,
            "Weekly_Low": weekly_low,
            "Weekly_Close": weekly_close,
        }
    )

    signals = scan_mtf_patterns(df)
    print("Pattern columns:", list(signals.columns))
    print("Dtypes:", signals.dtypes.to_dict())
    print("Boolean (no NaN):", bool(signals.notna().all().all()))
    print("\nPattern counts:")
    print(signals.sum().to_string())
    print("\nTotal rows scanned:", len(signals))


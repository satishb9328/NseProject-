"""Redis-driven Gamma Blast analytics worker — V2 (revamped Day GEX signal).

V2 CHANGES (vs. v1 / original `gamma_blast_engine.py`):
  A. Adaptive ±0.15 neutral band for the Day GEX ratio. The band now scales
     with DTE (shorter DTE → wider band, because gamma is noisier near
     expiry), ATM IV (higher IV → wider band), and symbol (BANKNIFTY gets a
     1.4x multiplier vs NIFTY). Controlled via GAMMA_DAY_NEUTRAL_BASE /
     GAMMA_DAY_DTE_REF / GAMMA_DAY_IV_REF / GAMMA_DAY_BANK_MULT env vars.
  B. ATM-weighted gross GEX for the Day signal. Far-OTM strikes with large
     COI but tiny gamma no longer inflate the denominator and suppress the
     ratio toward a false "range-bound". Weights follow exp(-|K-S|/decay)
     with decay = GAMMA_ATM_DECAY_POINTS (default 500).
  C. Composite 5-factor score for the Day signal. Instead of GEX-ratio-only,
     the score now combines:
        (1) GEX ratio (refined with adaptive band)   weight ±2.0
        (2) Day PCR (put-writing conviction)          weight ±1.0
        (3) Spot vs Day gamma-flip level              weight ±1.0
        (4) Day net charm (delta bleed)               weight ±0.5
        (5) ATM IV skew (fear gauge)                  weight ±0.5
  D. 5-tier Day signal output:
        score <= -3.0  🔴 Strong Bearish / BUY PE
        -3.0 < score <= -1.5  🟠 Mild Bearish / Hedge longs
        -1.5 < score < +1.5   🟡 Range-bound / Choppy
        +1.5 <= score < +3.0  🟢 Mild Bullish / Hold longs
        score >= +3.0         🟢 Strong Bullish / Buy Dips
  E. Persistence filter (3 updates by default). The v1 signal flipped on a
     single COI spike. v2 keeps a rolling window of the last N raw scores
     and only emits a confirmed directional signal when all of them share
     the same sign and stay within 50% of their mean. Controlled via
     GAMMA_PERSISTENCE_WINDOW env var.
  F. Time-of-day ramp. Early-session COI (9:15-10:30 IST) is thin and noisy.
     A session-progress factor (0.0 at open → 1.0 at 15:30) scales the
     composite score so a 9:20 print cannot trigger a strong signal until
     enough of the session has elapsed. Controlled via
     GAMMA_SESSION_RAMP_FRACTION (default 0.15 = first 15% of session is
     dampened).
  G. Weekly / Day agreement layer. The new `combined_weekly_day_signal()`
     overlays the day signal on top of the weekly regime to flag high-
     conviction setups (buy-the-dip, sell-the-rip, trend continuation).
  H. `reasons` dict alongside every Day signal so the dashboard can show
     *why* the engine took a stance (each factor's contribution + numeric
     input vs threshold). Also exposed in `last_profile`.

EVERYTHING ELSE FROM v1 IS PRESERVED:
  - Black-Scholes greeks (gamma, charm, vanna) with put_charm correction.
  - Linear interpolation for Gamma Flip levels.
  - True DTE against NSE session close (15:30 IST), tz-aware via zoneinfo.
  - Contract-scaled exposures (lot factor divided out at payload entry).
  - Compact `*_display` strings.
  - Weekly gamma regime signal (GEX sign + PCR threshold) UNCHANGED.
  - GEX zero-crossing + PCR-drop alerts UNCHANGED.
  - Redis pub/sub listener with thread-safe state, health-check, clean
    shutdown via Event().wait().
  - All v1 bug fixes (weekly gross_gex/net_gex_ratio, net_charm/vanna sign,
    put_charm formula, IV surface repair, non-string dict keys, wall guard,
    thread safety, logging, redis health-check, Event().wait() main loop).

AUTO-TRADE CODE REMAINS REMOVED (per the original request). The engine
emits analytics + signals only — it does not place orders.
"""

from __future__ import annotations

import json
import logging
import math
import os
import threading
import time
from collections import deque
from collections.abc import Iterable
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Deque, Dict, List, Optional, Tuple

try:
    import redis
except ImportError:
    redis = None

# Shared (expiry_date, strike_price) keying, used by the live feed and the
# dashboard too so producer and consumers can never disagree about which chain
# a strike belongs to.
from Nse_Project.expiry_utils import (
    belongs_to_expiry,
    expiry_group_key,
    normalize_expiry,
    resolve_row_expiry,
)


# === Config ================================================================

CONFIG = {
    "symbol": os.getenv("GAMMA_SYMBOL", "NIFTY"),
    "lot_size": int(os.getenv("GAMMA_LOT_SIZE", "65")),
    "gamma_flip_band": 500,
    "risk_free_rate": float(os.getenv("RISK_FREE_RATE", "0.065")),
    # Exposure scale of the INBOUND Redis payload (see resolve_oi_divisor).
    "oi_input_scale": os.getenv("GAMMA_OI_INPUT_SCALE", "shares").strip().lower(),
    # Compact units for the *_display strings.
    "exposure_units": os.getenv("GAMMA_EXPOSURE_UNITS", "indian").strip().lower(),
    # --- V2: Day-signal tuning ------------------------------------------------
    # Base half-width of the neutral band before DTE/IV/symbol multipliers.
    "day_neutral_base": float(os.getenv("GAMMA_DAY_NEUTRAL_BASE", "0.15")),
    # Reference DTE (in days) at which the multiplier is 1.0. Shorter DTE
    # widens the band (sqrt scaling), longer DTE narrows it.
    "day_dte_ref_days": float(os.getenv("GAMMA_DAY_DTE_REF", "7.0")),
    # Reference ATM IV (in %) at which the multiplier is 1.0.
    "day_iv_ref": float(os.getenv("GAMMA_DAY_IV_REF", "15.0")),
    # Symbol multiplier for high-volatility indices (BANKNIFTY etc).
    "day_bank_mult": float(os.getenv("GAMMA_DAY_BANK_MULT", "1.4")),
    # Damping distance (in points) for ATM-weighted gross GEX. A strike
    # `decay` points away from spot contributes 1/e ≈ 37% of its raw weight.
    "atm_decay_points": float(os.getenv("GAMMA_ATM_DECAY_POINTS", "500")),
    # Persistence window (number of consecutive updates that must agree on
    # sign and magnitude before a directional signal is confirmed).
    "persistence_window": int(os.getenv("GAMMA_PERSISTENCE_WINDOW", "3")),
    # First N% of the NSE session during which the composite score is dampened
    # toward neutral (early-session COI is thin and noisy).
    "session_ramp_fraction": float(os.getenv("GAMMA_SESSION_RAMP_FRACTION", "0.15")),
    # Day PCR thresholds used by the composite score.
    "day_pcr_bull": float(os.getenv("GAMMA_DAY_PCR_BULL", "1.10")),
    "day_pcr_bear": float(os.getenv("GAMMA_DAY_PCR_BEAR", "0.70")),
    # Charm magnitude (per-contract, per-day) above which a directional
    # delta-bleed is credited to the score.
    "day_charm_threshold": float(os.getenv("GAMMA_DAY_CHARM_THRESHOLD", "1000")),
    # ATM IV skew thresholds (put_iv - call_iv at ATM) for the fear gauge.
    "day_skew_bear": float(os.getenv("GAMMA_DAY_SKEW_BEAR", "3.0")),
    "day_skew_bull": float(os.getenv("GAMMA_DAY_SKEW_BULL", "-1.0")),
    # Spot-vs-flip distance (in % of spot) below which we treat them as
    # "at flip" (no directional credit).
    "day_flip_deadzone_pct": float(os.getenv("GAMMA_DAY_FLIP_DEADZONE_PCT", "0.30")),
    # Composite-score thresholds for the 5-tier classifier.
    "score_strong_bear": float(os.getenv("GAMMA_SCORE_STRONG_BEAR", "-3.0")),
    "score_mild_bear": float(os.getenv("GAMMA_SCORE_MILD_BEAR", "-1.5")),
    "score_mild_bull": float(os.getenv("GAMMA_SCORE_MILD_BULL", "1.5")),
    "score_strong_bull": float(os.getenv("GAMMA_SCORE_STRONG_BULL", "3.0")),
}

REDIS_URL = os.getenv("REDIS_URL", "redis://127.0.0.1:6379/0")
REDIS_DATA_KEY = os.getenv("GAMMA_REDIS_DATA_KEY", "live_options_data")
REDIS_CHANNEL = os.getenv("GAMMA_REDIS_CHANNEL", "gamma:live_options_updated")

DAYS_PER_YEAR = 365.0
SECONDS_PER_YEAR = DAYS_PER_YEAR * 24.0 * 60.0 * 60.0
VOL_POINTS_PER_UNIT = 100.0
MIN_DTE_YEARS = 1.0 / (DAYS_PER_YEAR * 24.0)
SQRT_2PI = math.sqrt(2.0 * math.pi)
SQRT_2 = math.sqrt(2.0)

# --- Timezone handling ------------------------------------------------------
_DEFAULT_TZ_NAME = os.getenv("GAMMA_TIMEZONE", "Asia/Kolkata").strip() or "Asia/Kolkata"
try:
    from zoneinfo import ZoneInfo  # Python 3.9+
    LOCAL_TZ = ZoneInfo(_DEFAULT_TZ_NAME)
except (ImportError, ValueError, KeyError):
    LOCAL_TZ = timezone(timedelta(hours=5, minutes=30), name="IST")

NSE_MARKET_OPEN_HOUR = 9
NSE_MARKET_OPEN_MINUTE = 15
NSE_MARKET_CLOSE_HOUR = 15
NSE_MARKET_CLOSE_MINUTE = 30

# --- Logging ---------------------------------------------------------------
logger = logging.getLogger("gamma_blast")
if not logger.handlers:
    _handler = logging.StreamHandler()
    _handler.setFormatter(logging.Formatter("[%(levelname)s] %(asctime)s %(message)s"))
    logger.addHandler(_handler)
    logger.setLevel(logging.INFO)


class GammaBotError(RuntimeError):
    """Raised when the Redis-driven worker cannot be started."""


def _number(value: Any, default: float = 0.0) -> float:
    """Coerce arbitrary JSON values to finite floats; fall back on error."""
    try:
        number = default if value in (None, "") else float(value)
        return number if math.isfinite(number) else default
    except (TypeError, ValueError):
        return default


CONTRACT_SCALE_HINTS = frozenset({"contract", "contracts", "lot", "lots"})

INDIAN_UNITS = ((1e7, "Cr"), (1e5, "L"), (1e3, "K"))
INTERNATIONAL_UNITS = ((1e12, "T"), (1e9, "B"), (1e6, "M"), (1e3, "K"))


def resolve_oi_divisor(payload: Optional[Dict[str, Any]] = None) -> float:
    """Return the factor that converts published OI/COI back into CONTRACTS."""
    payload = payload if isinstance(payload, dict) else {}
    declared_scale = str(payload.get("scale") or "").strip().lower()
    if declared_scale in CONTRACT_SCALE_HINTS:
        return 1.0
    if str(CONFIG.get("oi_input_scale") or "").strip().lower() in CONTRACT_SCALE_HINTS:
        return 1.0
    lot_size = _number(payload.get("lot_size") or payload.get("lotSize"))
    if lot_size <= 0:
        lot_size = _number(CONFIG.get("lot_size"))
    return lot_size if lot_size > 0 else 1.0


def format_exposure(value: Any, units: Optional[str] = None, precision: int = 2) -> str:
    """Render a raw exposure as a short, dashboard-friendly string."""
    number = _number(value, default=float("nan"))
    if not math.isfinite(number):
        return "--"
    scale = str(units or CONFIG.get("exposure_units") or "indian").strip().lower()
    table = INTERNATIONAL_UNITS if scale.startswith("int") else INDIAN_UNITS
    if number == 0:
        suffix = " Cr" if not scale.startswith("int") else " K"
        return f"0{suffix}"
    magnitude = abs(number)
    for threshold, suffix in table:
        if magnitude >= threshold:
            return f"{number / threshold:,.{precision}f} {suffix}"
    return f"{number:,.0f}" if magnitude >= 1 else f"{number:,.{precision}f}"


def _norm_cdf(x: float) -> float:
    """Standard normal CDF N(x) via math.erf - no scipy dependency."""
    return 0.5 * (1.0 + math.erf(x / SQRT_2))


def bs_greeks(
    spot: float, strike: float, dte: float, rate: float, volatility: float
) -> tuple[float, float, float, float]:
    """Return (gamma, call_charm, put_charm, vanna) for a European option."""
    if not all(math.isfinite(v) for v in (spot, strike, dte, rate, volatility)):
        return 0.0, 0.0, 0.0, 0.0
    if spot <= 0 or strike <= 0 or dte <= 0 or volatility <= 0:
        return 0.0, 0.0, 0.0, 0.0
    try:
        sqrt_dte = math.sqrt(dte)
        vol_sqrt_dte = volatility * sqrt_dte
        variance = volatility * volatility
        d1 = (math.log(spot / strike) + (rate + 0.5 * variance) * dte) / vol_sqrt_dte
        d2 = d1 - vol_sqrt_dte
        normal_pdf = math.exp(-0.5 * d1 * d1) / SQRT_2PI

        gamma = normal_pdf / (spot * vol_sqrt_dte)

        charm_core = (
            normal_pdf
            * (2.0 * rate * dte - d2 * vol_sqrt_dte)
            / (2.0 * dte * vol_sqrt_dte)
        )
        charm_core /= DAYS_PER_YEAR

        call_charm = -charm_core

        n_neg_d2 = _norm_cdf(-d2)
        put_charm_correction = (rate * math.exp(-rate * dte) * n_neg_d2) / DAYS_PER_YEAR
        put_charm = charm_core - put_charm_correction

        vanna = (-normal_pdf * d2 / volatility) / VOL_POINTS_PER_UNIT

        values = (gamma, call_charm, put_charm, vanna)
        return values if all(math.isfinite(v) for v in values) else (0.0, 0.0, 0.0, 0.0)
    except (ArithmeticError, ValueError):
        return 0.0, 0.0, 0.0, 0.0


# === V1 weekly signal (UNCHANGED) ==========================================

def gamma_regime_signal(net_gex: float, pcr: float) -> str:
    if net_gex < 0 and pcr < 0.7:
        return "🔴 Negative Gamma: Market Weak / BUY PE"
    if net_gex > 0 and pcr >= 0.7:
        return "🟢 Positive Gamma: Stable Market / Buy Dips"
    return "🟡 Range-bound: Choppy / Transition Zone"


# === V2 Day-signal helpers =================================================

def _adaptive_neutral_band(dte_years: float, atm_iv_pct: float, symbol: str = "NIFTY") -> float:
    """Compute the Day-signal neutral half-width based on DTE, IV, and symbol.

    The v1 engine hard-coded 0.15. v2 widens the band when gamma is noisier
    (short DTE) or when the underlying is more volatile (high IV, BANKNIFTY)
    and narrows it otherwise. All multipliers are clamped so a degenerate
    input can't blow the band out to >50% of gross.

    Scaling laws:
      - DTE:  sqrt(dte_ref / dte_days), clamped to [0.5, 2.5]
              7 DTE  -> 1.0x ; 1 DTE -> 2.65x ; 30 DTE -> 0.48x
      - IV:   atm_iv / iv_ref, clamped to [0.7, 1.6]
              15% IV -> 1.0x ; 25% -> 1.33x ; 10% -> 0.80x
      - Sym:  1.4x for BANKNIFTY-family, 1.0x otherwise
    """
    base = float(CONFIG.get("day_neutral_base", 0.15))
    dte_ref = float(CONFIG.get("day_dte_ref_days", 7.0))
    iv_ref = float(CONFIG.get("day_iv_ref", 15.0))
    bank_mult = float(CONFIG.get("day_bank_mult", 1.4))

    dte_days = max(1.0, dte_years * DAYS_PER_YEAR)
    dte_factor = math.sqrt(dte_ref / dte_days)
    dte_factor = max(0.5, min(2.5, dte_factor))

    iv_factor = atm_iv_pct / iv_ref if iv_ref > 0 else 1.0
    iv_factor = max(0.7, min(1.6, iv_factor))

    sym_factor = bank_mult if str(symbol).upper().startswith("BANK") else 1.0

    return max(0.05, min(0.40, base * dte_factor * iv_factor * sym_factor))


def _session_progress(now: Optional[datetime] = None) -> float:
    """Return 0.0 at NSE open (09:15 IST), 1.0 at close (15:30 IST).

    Before open / after close → 1.0 (no dampening). Used to mute early-
    session noise: the first `session_ramp_fraction` of the session scales
    the composite score from 0 up to 1.
    """
    now = now or datetime.now(LOCAL_TZ)
    try:
        o = now.replace(hour=NSE_MARKET_OPEN_HOUR, minute=NSE_MARKET_OPEN_MINUTE,
                        second=0, microsecond=0)
        c = now.replace(hour=NSE_MARKET_CLOSE_HOUR, minute=NSE_MARKET_CLOSE_MINUTE,
                        second=0, microsecond=0)
    except (ValueError, AttributeError):
        return 1.0
    if now <= o or now >= c:
        return 1.0
    return (now - o).total_seconds() / (c - o).total_seconds()


def _session_damp_factor(progress: float) -> float:
    """Map session progress (0..1) to a 0..1 damp multiplier on the score.

    For the first `session_ramp_fraction` of the session, the damp factor
    ramps linearly from 0 → 1. After that, it is 1.0 (no dampening).
    """
    ramp = float(CONFIG.get("session_ramp_fraction", 0.15))
    if ramp <= 0:
        return 1.0
    if progress <= 0:
        return 0.0
    if progress >= ramp:
        return 1.0
    return progress / ramp


def day_gamma_regime_signal_v2(
    net_gex: float,
    gross_gex: float,
    pcr: float,
    net_charm: float,
    skew: float,
    spot: float,
    gamma_flip: Optional[float],
    dte: float,
    atm_iv_pct: float,
    symbol: str = "NIFTY",
    now: Optional[datetime] = None,
) -> Tuple[str, Dict[str, Any]]:
    """Composite 5-factor Day signal. Returns (signal_text, reasons_dict).

    Factors and weights:
      (1) GEX ratio vs adaptive band          ±2.0
      (2) Day PCR (put-writing conviction)    ±1.0
      (3) Spot vs Day gamma-flip level        ±1.0
      (4) Day net charm (delta bleed)         ±0.5
      (5) ATM IV skew (fear gauge)            ±0.5

    The raw composite score is then dampened by the session-progress factor
    so an early-session print cannot produce a strong signal. Final score
    is mapped to one of 5 tiers via the GAMMA_SCORE_* thresholds.
    """
    reasons: Dict[str, Any] = {
        "raw_score": 0.0,
        "dampened_score": 0.0,
        "factors": {},
        "neutral_band": 0.0,
        "session_progress": 0.0,
        "session_damp": 0.0,
    }
    score = 0.0

    # ---- Factor 1: GEX ratio (adaptive band) ----
    if gross_gex <= 0 or not math.isfinite(gross_gex):
        reasons["factors"]["gex_ratio"] = "no day flow yet (gross=0)"
        # No directional credit, but also no penalty. Score stays 0 so the
        # classifier falls through to "Range-bound".
        return _classify_day_score(0.0, reasons, dampened=False)
    ratio = net_gex / gross_gex
    band = _adaptive_neutral_band(dte, atm_iv_pct, symbol)
    reasons["neutral_band"] = round(band, 4)
    if ratio <= -band:
        score -= 2.0
        reasons["factors"]["gex_ratio"] = (
            f"{ratio:+.3f} vs ±{band:.3f} → bearish (−2.0)"
        )
    elif ratio >= band:
        score += 2.0
        reasons["factors"]["gex_ratio"] = (
            f"{ratio:+.3f} vs ±{band:.3f} → bullish (+2.0)"
        )
    else:
        reasons["factors"]["gex_ratio"] = (
            f"{ratio:+.3f} vs ±{band:.3f} → neutral (0)"
        )

    # ---- Factor 2: Day PCR ----
    pcr_bull = float(CONFIG.get("day_pcr_bull", 1.10))
    pcr_bear = float(CONFIG.get("day_pcr_bear", 0.70))
    if pcr >= pcr_bull:
        score += 1.0
        reasons["factors"]["pcr"] = f"{pcr:.2f} >= {pcr_bull} → strong put writing (+1.0)"
    elif pcr < pcr_bear:
        score -= 1.0
        reasons["factors"]["pcr"] = f"{pcr:.2f} < {pcr_bear} → put unwinding (−1.0)"
    else:
        reasons["factors"]["pcr"] = f"{pcr:.2f} in [{pcr_bear}, {pcr_bull}) → neutral (0)"

    # ---- Factor 3: Spot vs Day gamma-flip ----
    deadzone = float(CONFIG.get("day_flip_deadzone_pct", 0.30))
    if gamma_flip is not None and gamma_flip > 0 and spot > 0:
        dist_pct = (spot - gamma_flip) / spot * 100.0
        if dist_pct < -deadzone:
            score -= 1.0
            reasons["factors"]["flip"] = (
                f"spot {dist_pct:+.2f}% below flip {gamma_flip:.0f} → bearish (−1.0)"
            )
        elif dist_pct > deadzone:
            score += 1.0
            reasons["factors"]["flip"] = (
                f"spot {dist_pct:+.2f}% above flip {gamma_flip:.0f} → bullish (+1.0)"
            )
        else:
            reasons["factors"]["flip"] = (
                f"spot {dist_pct:+.2f}% (within ±{deadzone}% of flip) → neutral (0)"
            )
    else:
        reasons["factors"]["flip"] = "no flip level available → neutral (0)"

    # ---- Factor 4: Day net charm (delta bleed) ----
    charm_thr = float(CONFIG.get("day_charm_threshold", 1000))
    if net_charm <= -charm_thr:
        score -= 0.5
        reasons["factors"]["charm"] = (
            f"{net_charm:+,.0f} <= −{charm_thr:,.0f} → bearish delta bleed (−0.5)"
        )
    elif net_charm >= charm_thr:
        score += 0.5
        reasons["factors"]["charm"] = (
            f"{net_charm:+,.0f} >= +{charm_thr:,.0f} → bullish delta bleed (+0.5)"
        )
    else:
        reasons["factors"]["charm"] = (
            f"{net_charm:+,.0f} within ±{charm_thr:,.0f} → neutral (0)"
        )

    # ---- Factor 5: ATM IV skew ----
    skew_bear = float(CONFIG.get("day_skew_bear", 3.0))
    skew_bull = float(CONFIG.get("day_skew_bull", -1.0))
    if skew > skew_bear:
        score -= 0.5
        reasons["factors"]["skew"] = (
            f"{skew:.2f} > {skew_bear} → fear elevated (−0.5)"
        )
    elif skew < skew_bull:
        score += 0.5
        reasons["factors"]["skew"] = (
            f"{skew:.2f} < {skew_bull} → call skew (+0.5)"
        )
    else:
        reasons["factors"]["skew"] = f"{skew:.2f} normal → neutral (0)"

    reasons["raw_score"] = round(score, 3)

    # ---- Session dampening ----
    progress = _session_progress(now)
    damp = _session_damp_factor(progress)
    reasons["session_progress"] = round(progress, 3)
    reasons["session_damp"] = round(damp, 3)
    dampened = score * damp
    reasons["dampened_score"] = round(dampened, 3)

    return _classify_day_score(dampened, reasons, dampened=True)


def _classify_day_score(
    score: float, reasons: Dict[str, Any], dampened: bool
) -> Tuple[str, Dict[str, Any]]:
    """Map a (dampened) composite score to one of 5 tiers."""
    s_bear_strong = float(CONFIG.get("score_strong_bear", -3.0))
    s_bear_mild = float(CONFIG.get("score_mild_bear", -1.5))
    s_bull_mild = float(CONFIG.get("score_mild_bull", 1.5))
    s_bull_strong = float(CONFIG.get("score_strong_bull", 3.0))

    prefix = "" if dampened else "(undamped) "
    if score <= s_bear_strong:
        signal = f"{prefix}🔴 Strong Bearish Day Gamma / BUY PE"
    elif score <= s_bear_mild:
        signal = f"{prefix}🟠 Mild Bearish Day Gamma / Hedge longs"
    elif score < s_bull_mild:
        signal = f"{prefix}🟡 Range-bound: Choppy / Transition Zone"
    elif score < s_bull_strong:
        signal = f"{prefix}🟢 Mild Bullish Day Gamma / Hold longs"
    else:
        signal = f"{prefix}🟢 Strong Bullish Day Gamma / Buy Dips"
    return signal, reasons


def combined_weekly_day_signal(weekly_signal: str, day_signal: str) -> Tuple[str, Dict[str, str]]:
    """Overlay the Day signal on top of the Weekly regime.

    Returns a high-conviction composite read + a reasons dict so the
    dashboard can show the parent signals side by side.
    """
    weekly_bull = "Positive" in weekly_signal or "Bullish" in weekly_signal
    weekly_bear = "Negative" in weekly_signal or "Bearish" in weekly_signal
    day_bull = "Bullish" in day_signal
    day_bear = "Bearish" in day_signal
    day_strong = "Strong" in day_signal
    day_mild = "Mild" in day_signal

    reasons = {"weekly": weekly_signal, "day": day_signal}

    if weekly_bull and day_bear:
        msg = "💎 Buy-the-dip setup (macro up, intraday flush)" if day_strong \
            else "🟢 Pullback in uptrend (watch for reversal)"
        return msg, reasons
    if weekly_bear and day_bull:
        msg = "⚠️ Sell-the-rip setup (macro down, intraday bounce)" if day_strong \
            else "🟠 Dead-cat bounce in downtrend (fade strength)"
        return msg, reasons
    if weekly_bull and day_bull:
        msg = "🟢 Trend continuation UP (add longs on dips)" if day_strong \
            else "🟢 Trend up, momentum confirming"
        return msg, reasons
    if weekly_bear and day_bear:
        msg = "🔴 Trend continuation DOWN (add hedges on rips)" if day_strong \
            else "🔴 Trend down, momentum confirming"
        return msg, reasons
    if day_mild and not (weekly_bull or weekly_bear):
        return "🟡 Mild intraday bias, weekly neutral — wait for breakout", reasons
    return "🟡 Mixed / wait for confirmation", reasons


# === Dataclasses ===========================================================

@dataclass
class OptionStrike:
    strike: float
    call_oi: float = 0.0
    call_iv: float = 0.0
    call_ltp: float = 0.0
    call_vol: float = 0.0
    put_oi: float = 0.0
    put_iv: float = 0.0
    put_ltp: float = 0.0
    put_vol: float = 0.0
    call_coi: float = 0.0
    put_coi: float = 0.0
    has_day_coi: bool = False
    net_gex: float = 0.0
    day_net_gex: float = 0.0
    call_gex: float = 0.0
    put_gex: float = 0.0
    day_call_gex: float = 0.0
    day_put_gex: float = 0.0
    call_charm: float = 0.0
    put_charm: float = 0.0
    call_vanna: float = 0.0
    put_vanna: float = 0.0
    net_charm: float = 0.0
    day_net_charm: float = 0.0
    net_vanna: float = 0.0
    day_net_vanna: float = 0.0


@dataclass
class GammaMetrics:
    pcr: float = 0.0
    gamma_flip_level: Optional[float] = None
    net_gex: float = 0.0
    call_wall_strike: Optional[float] = None
    put_wall_strike: Optional[float] = None
    call_wall: Optional[float] = None
    put_wall: Optional[float] = None
    net_charm: float = 0.0
    net_vanna: float = 0.0
    signal: str = ""
    gross_gex: float = 0.0
    net_gex_ratio: float = 0.0
    strike_count: int = 0
    call_flow: float = 0.0
    put_flow: float = 0.0
    net_gex_display: str = ""
    gross_gex_display: str = ""
    net_charm_display: str = ""
    net_vanna_display: str = ""
    call_flow_display: str = ""
    put_flow_display: str = ""
    exposure_unit: str = "contracts"
    # --- V2 Day-signal extras ---
    # ATM-weighted gross used by the v2 day signal (raw `gross_gex` above is
    # kept for backwards compat / dashboards that already render it).
    atm_weighted_gross: float = 0.0
    # Composite v2 score and the per-factor breakdown.
    day_signal_score: float = 0.0
    day_signal_reasons: Dict[str, Any] = field(default_factory=dict)
    # Final composite (weekly+day) read.
    combined_signal: str = ""
    combined_reasons: Dict[str, str] = field(default_factory=dict)


@dataclass
class GammaProfile:
    spot: float
    expiry: str
    dte: float
    strikes: List[OptionStrike] = field(default_factory=list)
    net_gex: float = 0.0
    gamma_flip_level: Optional[float] = None
    call_wall: Optional[float] = None
    put_wall: Optional[float] = None
    pcr: float = 0.0
    skew: float = 0.0
    weekly_gamma: GammaMetrics = field(default_factory=GammaMetrics)
    day_gamma: GammaMetrics = field(default_factory=GammaMetrics)
    contract_divisor: float = 1.0
    oi_unit: str = "contracts"
    net_gex_display: str = ""
    # --- V2: ATM IV (used by adaptive band) ---
    atm_iv: float = 0.0


# === GammaProcessor ========================================================

class GammaProcessor:
    def __init__(self, symbol: str) -> None:
        self.symbol = symbol
        self.risk_free_rate = CONFIG["risk_free_rate"]

    def process_payload(self, payload: Dict[str, Any]) -> Optional[GammaProfile]:
        spot = _number(payload.get("spot_price"))
        expiry = str(payload.get("expiry_date") or "")
        rows = payload.get("data")
        if spot <= 0 or not expiry or not isinstance(rows, list):
            return None

        oi_divisor = resolve_oi_divisor(payload)
        strikes = [self._to_strike(row, oi_divisor) for row in rows if isinstance(row, dict)]
        strikes = [strike for strike in strikes if strike is not None]
        if not strikes:
            return None

        dte = self._fractional_dte(expiry)
        self._repair_iv_surface(strikes)
        rate = self.risk_free_rate
        gex_scale = spot
        for strike in strikes:
            try:
                call_gamma, strike.call_charm, _, strike.call_vanna = bs_greeks(
                    spot, strike.strike, dte, rate, strike.call_iv / 100
                )
                put_gamma, _, strike.put_charm, strike.put_vanna = bs_greeks(
                    spot, strike.strike, dte, rate, strike.put_iv / 100
                )

                strike.call_gex = -strike.call_oi * call_gamma * gex_scale
                strike.put_gex = strike.put_oi * put_gamma * gex_scale
                strike.net_gex = strike.call_gex + strike.put_gex
                strike.net_charm = -strike.call_oi * strike.call_charm + strike.put_oi * strike.put_charm
                strike.net_vanna = -strike.call_oi * strike.call_vanna + strike.put_oi * strike.put_vanna

                if strike.has_day_coi:
                    strike.day_call_gex = -strike.call_coi * call_gamma * gex_scale
                    strike.day_put_gex = strike.put_coi * put_gamma * gex_scale
                    strike.day_net_charm = -strike.call_coi * strike.call_charm + strike.put_coi * strike.put_charm
                    strike.day_net_vanna = -strike.call_coi * strike.call_vanna + strike.put_coi * strike.put_vanna
                else:
                    strike.day_call_gex = strike.day_put_gex = 0.0
                    strike.day_net_charm = strike.day_net_vanna = 0.0
                strike.day_net_gex = strike.day_call_gex + strike.day_put_gex
            except (ArithmeticError, TypeError, ValueError):
                logger.exception("Failed to compute greeks for strike %s", strike.strike)

        profile = GammaProfile(
            spot=spot,
            expiry=expiry,
            dte=dte,
            strikes=strikes,
            contract_divisor=oi_divisor,
            oi_unit="contracts",
        )
        self._build_metrics(profile)
        return profile

    def _fractional_dte(self, expiry: str) -> float:
        try:
            expiry_close = datetime.strptime(expiry, "%Y-%m-%d").replace(
                hour=NSE_MARKET_CLOSE_HOUR,
                minute=NSE_MARKET_CLOSE_MINUTE,
                second=0,
                microsecond=0,
                tzinfo=LOCAL_TZ,
            )
        except ValueError:
            return MIN_DTE_YEARS
        remaining = (expiry_close - datetime.now(LOCAL_TZ)).total_seconds() / SECONDS_PER_YEAR
        return max(remaining, MIN_DTE_YEARS)

    @staticmethod
    def _to_strike(row: Dict[str, Any], oi_divisor: Optional[float] = None) -> Optional[OptionStrike]:
        strike = _number(row.get("strike_price", row.get("strike")))
        if strike <= 0:
            return None
        divisor = _number(oi_divisor) if oi_divisor is not None else resolve_oi_divisor()
        if divisor <= 0:
            divisor = 1.0
        call_coi_raw = row.get("call_coi")
        put_coi_raw = row.get("put_coi")
        has_day_coi = call_coi_raw is not None and put_coi_raw is not None
        return OptionStrike(
            strike=strike,
            call_oi=_number(row.get("call_oi")) / divisor, call_iv=_number(row.get("call_iv")),
            call_ltp=_number(row.get("call_ltp")), call_vol=_number(row.get("call_volume")),
            put_oi=_number(row.get("put_oi")) / divisor, put_iv=_number(row.get("put_iv")),
            put_ltp=_number(row.get("put_ltp")), put_vol=_number(row.get("put_volume")),
            call_coi=_number(call_coi_raw) / divisor, put_coi=_number(put_coi_raw) / divisor,
            has_day_coi=has_day_coi,
        )

    @staticmethod
    def _repair_iv_surface(strikes: List[OptionStrike]) -> None:
        """Replace missing/invalid IVs with the chain mean (rejects ≤0)."""
        valid_call_ivs = [s.call_iv for s in strikes if s.call_iv > 0]
        valid_put_ivs = [s.put_iv for s in strikes if s.put_iv > 0]
        default_call_iv = sum(valid_call_ivs) / len(valid_call_ivs) if valid_call_ivs else 15.0
        default_put_iv = sum(valid_put_ivs) / len(valid_put_ivs) if valid_put_ivs else 15.0
        for strike in strikes:
            strike.call_iv = strike.call_iv if strike.call_iv > 0 else default_call_iv
            strike.put_iv = strike.put_iv if strike.put_iv > 0 else default_put_iv

    @staticmethod
    def _atm_weighted_gross(
        strikes: List[OptionStrike],
        spot: float,
        call_attr: str,
        put_attr: str,
    ) -> float:
        """V2: ATM-weighted absolute gross GEX.

        Each strike's |call_gex| + |put_gex| contribution is multiplied by
        exp(-|K-S|/decay). Decay defaults to 500 pts (NIFTY); tune via
        GAMMA_ATM_DECAY_POINTS. Far-OTM noise that used to inflate the
        denominator and force the ratio toward 0 (false "range-bound") now
        dies off exponentially.
        """
        decay = float(CONFIG.get("atm_decay_points", 500))
        if decay <= 0 or spot <= 0 or not strikes:
            return 0.0
        total = 0.0
        for s in strikes:
            w = math.exp(-abs(s.strike - spot) / decay)
            total += w * (abs(getattr(s, call_attr)) + abs(getattr(s, put_attr)))
        return total

    @staticmethod
    def _build_metrics(profile: GammaProfile) -> None:
        profile.weekly_gamma = GammaProcessor._metrics_for(
            profile.strikes, "call_oi", "put_oi", "net_gex",
            "net_charm", "net_vanna", profile.spot,
        )
        day_strikes = [strike for strike in profile.strikes if strike.has_day_coi]
        profile.day_gamma = GammaProcessor._metrics_for(
            day_strikes, "call_coi", "put_coi", "day_net_gex",
            "day_net_charm", "day_net_vanna",
            profile.spot, absolute_pcr=True,
        )

        profile.weekly_gamma.gross_gex = sum(
            abs(s.call_gex) + abs(s.put_gex) for s in profile.strikes
        )
        profile.weekly_gamma.net_gex_ratio = (
            round(profile.weekly_gamma.net_gex / profile.weekly_gamma.gross_gex, 4)
            if profile.weekly_gamma.gross_gex > 0 else 0.0
        )

        # V2: ATM-weighted gross for the day signal. The raw `gross_gex`
        # below is still computed for backwards compat / dashboards.
        profile.day_gamma.gross_gex = sum(
            abs(s.day_call_gex) + abs(s.day_put_gex) for s in day_strikes
        )
        profile.day_gamma.atm_weighted_gross = GammaProcessor._atm_weighted_gross(
            day_strikes, profile.spot, "day_call_gex", "day_put_gex"
        )
        profile.day_gamma.net_gex_ratio = (
            round(profile.day_gamma.net_gex / profile.day_gamma.gross_gex, 4)
            if profile.day_gamma.gross_gex > 0 else 0.0
        )

        profile.weekly_gamma.gamma_flip_level = GammaProcessor._find_gamma_flip(
            profile.strikes, "call_gex", "put_gex", profile.spot
        )
        profile.day_gamma.gamma_flip_level = GammaProcessor._find_gamma_flip(
            day_strikes, "day_call_gex", "day_put_gex", profile.spot
        )

        profile.weekly_gamma.signal = gamma_regime_signal(
            profile.weekly_gamma.net_gex, profile.weekly_gamma.pcr
        )

        # V2: composite day signal. Uses ATM-weighted gross so far-OTM
        # noise doesn't suppress the ratio. Falls back to raw gross when
        # ATM-weighted is 0 (single-strike chain edge case).
        atm_iv = GammaProcessor._atm_iv(profile)
        profile.atm_iv = atm_iv
        gross_for_signal = (
            profile.day_gamma.atm_weighted_gross
            if profile.day_gamma.atm_weighted_gross > 0
            else profile.day_gamma.gross_gex
        )
        day_signal, day_reasons = day_gamma_regime_signal_v2(
            net_gex=profile.day_gamma.net_gex,
            gross_gex=gross_for_signal,
            pcr=profile.day_gamma.pcr,
            net_charm=profile.day_gamma.net_charm,
            skew=profile.skew,
            spot=profile.spot,
            gamma_flip=profile.day_gamma.gamma_flip_level,
            dte=profile.dte,
            atm_iv_pct=atm_iv,
            symbol=CONFIG.get("symbol", "NIFTY"),
        )
        profile.day_gamma.signal = day_signal
        profile.day_gamma.day_signal_score = float(day_reasons.get("dampened_score", 0.0))
        profile.day_gamma.day_signal_reasons = day_reasons

        # V2: weekly/day agreement layer.
        combined, combined_reasons = combined_weekly_day_signal(
            profile.weekly_gamma.signal, profile.day_gamma.signal
        )
        profile.day_gamma.combined_signal = combined
        profile.day_gamma.combined_reasons = combined_reasons

        profile.net_gex = profile.weekly_gamma.net_gex
        profile.pcr = profile.weekly_gamma.pcr
        profile.gamma_flip_level = profile.weekly_gamma.gamma_flip_level

        if any(s.call_oi > 0 for s in profile.strikes):
            profile.call_wall = max(profile.strikes, key=lambda s: s.call_oi).strike
        if any(s.put_oi > 0 for s in profile.strikes):
            profile.put_wall = max(profile.strikes, key=lambda s: s.put_oi).strike

        atm = min(profile.strikes, key=lambda s: abs(s.strike - profile.spot))
        profile.skew = atm.put_iv - atm.call_iv

        GammaProcessor._apply_display_fields(profile.weekly_gamma)
        GammaProcessor._apply_display_fields(profile.day_gamma)
        profile.net_gex_display = format_exposure(profile.net_gex)

    @staticmethod
    def _atm_iv(profile: GammaProfile) -> float:
        """Mean of ATM call + put IV (in %). Used by the adaptive band."""
        if not profile.strikes or profile.spot <= 0:
            return float(CONFIG.get("day_iv_ref", 15.0))
        atm = min(profile.strikes, key=lambda s: abs(s.strike - profile.spot))
        return (atm.call_iv + atm.put_iv) / 2.0

    @staticmethod
    def _apply_display_fields(metrics: GammaMetrics) -> GammaMetrics:
        metrics.net_gex_display = format_exposure(metrics.net_gex)
        metrics.gross_gex_display = format_exposure(metrics.gross_gex)
        metrics.net_charm_display = format_exposure(metrics.net_charm)
        metrics.net_vanna_display = format_exposure(metrics.net_vanna)
        metrics.call_flow_display = format_exposure(metrics.call_flow)
        metrics.put_flow_display = format_exposure(metrics.put_flow)
        return metrics

    @staticmethod
    def _metrics_for(
        strikes: List[OptionStrike],
        call_field: str, put_field: str,
        gex_field: str, charm_field: str, vanna_field: str,
        spot: float, absolute_pcr: bool = False,
    ) -> GammaMetrics:
        call_total = sum(getattr(s, call_field) for s in strikes)
        put_total = sum(getattr(s, put_field) for s in strikes)
        if absolute_pcr:
            pcr_call = sum(abs(getattr(s, call_field)) for s in strikes)
            pcr_put = sum(abs(getattr(s, put_field)) for s in strikes)
        else:
            pcr_call = call_total
            pcr_put = put_total
        call_wall_strike = (
            max(strikes, key=lambda s: getattr(s, call_field)).strike if strikes else None
        )
        put_wall_strike = (
            max(strikes, key=lambda s: getattr(s, put_field)).strike if strikes else None
        )
        metrics = GammaMetrics(
            pcr=round(pcr_put / pcr_call, 3) if pcr_call else 0.0,
            net_gex=sum(getattr(s, gex_field) for s in strikes),
            gamma_flip_level=None,
            call_wall_strike=call_wall_strike,
            put_wall_strike=put_wall_strike,
            call_wall=call_wall_strike,
            put_wall=put_wall_strike,
            net_charm=sum(getattr(s, charm_field) for s in strikes),
            net_vanna=sum(getattr(s, vanna_field) for s in strikes),
            strike_count=len(strikes),
            call_flow=call_total,
            put_flow=put_total,
        )
        return GammaProcessor._apply_display_fields(metrics)

    @staticmethod
    def _find_gamma_flip(
        strikes: List[OptionStrike],
        call_gex_attr: str, put_gex_attr: str, spot: float,
    ) -> Optional[float]:
        """Return the dominant zero-crossing of cumulative net GEX near spot."""
        band = float(CONFIG.get("gamma_flip_band", 500))
        ordered = sorted(strikes, key=lambda s: s.strike)
        if not ordered:
            return None

        lower_edge = spot - band
        upper_edge = spot + band

        cumulative = 0.0
        prev_cumulative: Optional[float] = None
        prev_strike: Optional[float] = None
        best_level: Optional[float] = None
        best_swing = -1.0

        for strike in ordered:
            strike_price = strike.strike
            if strike_price > upper_edge:
                break

            strike_gex = getattr(strike, call_gex_attr) + getattr(strike, put_gex_attr)

            if strike_price < lower_edge:
                cumulative += strike_gex
                continue

            if prev_strike is None:
                cumulative += strike_gex
                prev_cumulative = cumulative
                prev_strike = strike_price
                continue

            prev_cumulative = cumulative
            cumulative += strike_gex

            prev_sign = (prev_cumulative > 0) - (prev_cumulative < 0)
            curr_sign = (cumulative > 0) - (cumulative < 0)
            if prev_sign != 0 and prev_sign != curr_sign:
                gex_diff = cumulative - prev_cumulative
                if gex_diff != 0:
                    ratio = abs(prev_cumulative) / abs(gex_diff)
                    level = prev_strike + (strike_price - prev_strike) * ratio
                    swing = abs(gex_diff)
                    if swing > best_swing or (
                        swing == best_swing
                        and (best_level is None or abs(level - spot) < abs(best_level - spot))
                    ):
                        best_swing = swing
                        best_level = level

            prev_strike = strike_price

        return best_level


# === GammaAlertEngine (V2 with persistence filter) ========================

class GammaAlertEngine:
    """Detects GEX zero crossings, PCR drops, AND applies persistence filter
    to the V2 composite Day signal so a single COI spike can't whipsaw the
    regime. Only emits the *confirmed* signal (and a flip alert when the
    confirmed score changes sign) to the alerts deque.
    """

    def __init__(self, persistence_window: Optional[int] = None) -> None:
        self._previous_gamma_expiry: Optional[str] = None
        self._previous_weekly_gex: Optional[float] = None
        self._previous_day_gex: Optional[float] = None
        self._previous_day_pcr: Optional[float] = None
        # V2: rolling window of raw (undamped) composite scores for the
        # persistence filter. We track the RAW score here, not the dampened
        # one — session dampening is a presentation concern, not a signal-
        # quality concern.
        window = int(persistence_window if persistence_window is not None
                     else CONFIG.get("persistence_window", 3))
        window = max(1, window)
        self._day_score_history: Deque[float] = deque(maxlen=window)
        self._previous_confirmed_score: Optional[float] = None

    def evaluate_gamma_alerts(self, profile: GammaProfile) -> List[str]:
        weekly = profile.weekly_gamma
        day = profile.day_gamma
        if profile.expiry != self._previous_gamma_expiry:
            self._previous_gamma_expiry = profile.expiry
            self._previous_weekly_gex = weekly.net_gex
            self._previous_day_gex = day.net_gex
            self._previous_day_pcr = day.pcr
            self._day_score_history.clear()
            self._previous_confirmed_score = None
            return []

        alerts: List[str] = []

        # --- Weekly GEX zero-crossings (unchanged from v1) ---
        if self._crossed_below_zero(self._previous_weekly_gex, weekly.net_gex):
            alerts.append("⚠️ Weekly Alert: Weekly GEX flipped Negative (Macro selling pressure building)")
        elif self._crossed_above_zero(self._previous_weekly_gex, weekly.net_gex):
            alerts.append("✅ Weekly Alert: Weekly GEX flipped Positive (Macro stability improving)")

        # --- Day GEX zero-crossings (unchanged from v1) ---
        if self._crossed_below_zero(self._previous_day_gex, day.net_gex):
            alerts.append("⚡ Intraday Alert: Day GEX flipped Negative (Selling pressure building)")
        elif self._crossed_above_zero(self._previous_day_gex, day.net_gex):
            alerts.append("⚡ Intraday Alert: Day GEX flipped Positive (Buying support improving)")

        # --- PCR drop alert (unchanged from v1) ---
        previous_pcr = self._previous_day_pcr
        if previous_pcr is not None and previous_pcr > 0 and day.pcr < previous_pcr:
            drop = previous_pcr - day.pcr
            if drop >= max(0.2, previous_pcr * 0.25):
                alerts.append("⚠️ Intraday Alert: Sharp drop in Put writing detected")

        # --- V2: persistence-filtered Day-score flip alerts ---
        raw_score = float(day.day_signal_reasons.get("raw_score", 0.0))
        confirmed = self._confirmed_score(raw_score)
        if confirmed is not None and self._previous_confirmed_score is not None:
            prev_sign = (self._previous_confirmed_score > 0) - (self._previous_confirmed_score < 0)
            curr_sign = (confirmed > 0) - (confirmed < 0)
            if prev_sign != 0 and curr_sign != 0 and prev_sign != curr_sign:
                direction = "Bullish" if curr_sign > 0 else "Bearish"
                alerts.append(
                    f"🎯 Day-signal regime flip confirmed: now {direction} "
                    f"(avg score {confirmed:+.2f} over {len(self._day_score_history)} updates)"
                )
        if confirmed is not None:
            self._previous_confirmed_score = confirmed

        self._previous_weekly_gex = weekly.net_gex
        self._previous_day_gex = day.net_gex
        self._previous_day_pcr = day.pcr
        return alerts

    def _confirmed_score(self, raw_score: float) -> Optional[float]:
        """Return the running mean of the last N scores IF they all share a
        sign AND stay within 50% of the mean. Otherwise return None (signal
        is "pending confirmation" and no flip alert fires)."""
        self._day_score_history.append(raw_score)
        if len(self._day_score_history) < self._day_score_history.maxlen:
            return None
        signs = {(s > 0) - (s < 0) for s in self._day_score_history}
        if len(signs) != 1:
            return None
        mean = sum(self._day_score_history) / len(self._day_score_history)
        # Magnitude coherence: every reading within ±50% of the mean.
        if abs(mean) < 1e-6:
            return 0.0
        for s in self._day_score_history:
            if abs(s - mean) > abs(mean) * 0.5:
                return None
        return mean

    @staticmethod
    def _crossed_below_zero(previous: Optional[float], current: float) -> bool:
        return previous is not None and previous >= 0 > current

    @staticmethod
    def _crossed_above_zero(previous: Optional[float], current: float) -> bool:
        return previous is not None and previous <= 0 < current


# === Payload normalization ================================================

_SYMBOL_SOURCE_FIELDS = ("tradingsymbol", "trading_symbol", "symbol", "underlying", "underlying_name")


def _is_foreign_underlying(item: Dict[str, Any], target: str) -> bool:
    """True only when an entry's symbol PROVABLY belongs to a different underlying."""
    for field_name in _SYMBOL_SOURCE_FIELDS:
        raw = item.get(field_name)
        if not isinstance(raw, str):
            continue
        text = raw.strip().upper().replace(" ", "")
        if not text:
            continue
        if not text.startswith(target):
            return True
        rest = text[len(target):]
        if rest and not rest[0].isdigit():
            return True
        return False
    return False


def normalize_live_payload(payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Normalize a live snapshot into {spot_price, expiry_date, data:[rows]}."""
    if isinstance(payload.get("data"), list):
        normalized = dict(payload)
        normalized["spot_price"] = _find_spot_price(payload) or _estimate_spot_from_rows(normalized["data"])
        return normalized

    instrument_data = payload.get("instrument_data")
    if not isinstance(instrument_data, dict):
        return None

    active_expiry = normalize_expiry(payload.get("expiry_date"))
    target_symbol = str(CONFIG.get("symbol") or "NIFTY").strip().upper().replace(" ", "")
    rows: Dict[tuple, Dict[str, Any]] = {}
    foreign_expiries: Dict[str, int] = {}
    foreign_underlyings = 0
    for item in instrument_data.values():
        if not isinstance(item, dict):
            continue
        strike = _number(item.get("strike"))
        side = str(item.get("option_type", "")).upper()
        if strike <= 0 or side not in {"CE", "PE"}:
            continue
        if _is_foreign_underlying(item, target_symbol):
            foreign_underlyings += 1
            continue
        if active_expiry and not belongs_to_expiry(item, active_expiry):
            foreign = resolve_row_expiry(item) or "unknown"
            foreign_expiries[foreign] = foreign_expiries.get(foreign, 0) + 1
            continue

        row_expiry = expiry_group_key(item, active_expiry)
        row = rows.setdefault(
            (row_expiry, strike),
            {"strike_price": strike, "expiry": row_expiry or None},
        )
        prefix = "call" if side == "CE" else "put"
        row[f"{prefix}_oi"] = _number(item.get("oi"))
        raw_coi = item.get("coi")
        row[f"{prefix}_coi"] = None if raw_coi is None else _number(raw_coi)
        row[f"{prefix}_iv"] = _number(item.get("iv"))
        row[f"{prefix}_ltp"] = _number(item.get("ltp"))
        row[f"{prefix}_volume"] = _number(item.get("volume"))

    option_rows = [row for _, row in sorted(rows.items(), key=lambda item: (item[0][0], item[0][1]))]
    return {
        "spot_price": _find_spot_price(payload) or _estimate_spot_from_rows(option_rows),
        "expiry_date": payload.get("expiry_date"),
        "data": option_rows,
        "lot_size": payload.get("lot_size") or payload.get("lotSize"),
        "scale": payload.get("scale"),
        "foreign_expiry_rows": sum(foreign_expiries.values()),
        "foreign_expiries": sorted(foreign_expiries),
        "foreign_underlying_rows": foreign_underlyings,
    }


def _find_spot_price(payload: Any) -> float:
    """Recursively locate the spot price in a nested payload."""
    spot_keys = {"spot_price", "underlying_spot", "underlying_ltp", "index_ltp", "nifty_ltp"}
    if isinstance(payload, dict):
        for key, value in payload.items():
            if isinstance(key, str) and key.lower() in spot_keys:
                spot = _number(value)
                if spot > 0:
                    return spot
        for value in payload.values():
            spot = _find_spot_price(value)
            if spot > 0:
                return spot
    elif isinstance(payload, list):
        for value in payload:
            spot = _find_spot_price(value)
            if spot > 0:
                return spot
    return 0.0


def _estimate_spot_from_rows(rows: Iterable[Dict[str, Any]]) -> float:
    parity_estimates: List[float] = []
    active_strikes: List[float] = []
    for row in rows:
        strike = _number(row.get("strike_price", row.get("strike")))
        call_ltp = _number(row.get("call_ltp"))
        put_ltp = _number(row.get("put_ltp"))
        if strike <= 0:
            continue
        if call_ltp > 0 and put_ltp > 0:
            parity_estimates.append(strike + call_ltp - put_ltp)
        if call_ltp > 0 or put_ltp > 0 or _number(row.get("call_oi")) > 0 or _number(row.get("put_oi")) > 0:
            active_strikes.append(strike)

    if not active_strikes:
        return 0.0

    active_strikes.sort()
    atm = active_strikes[len(active_strikes) // 2]
    band = float(CONFIG.get("gamma_flip_band", 500))
    valid_parity = [estimate for estimate in parity_estimates if abs(estimate - atm) <= band]
    estimates = sorted(valid_parity or parity_estimates or [atm])
    middle = len(estimates) // 2
    return estimates[middle] if len(estimates) % 2 else (estimates[middle - 1] + estimates[middle]) / 2


# === RedisGammaBot =========================================================

class RedisGammaBot:
    """Owns the pub/sub thread and hands each Redis update to the analytics engine."""

    def __init__(
        self,
        redis_url: str = REDIS_URL,
        data_key: str = REDIS_DATA_KEY,
        channel: str = REDIS_CHANNEL,
    ) -> None:
        self.redis_url = redis_url
        self.data_key = data_key
        self.channel = channel
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()
        self._redis_client: Any = None
        self.processor = GammaProcessor(CONFIG["symbol"])
        self.alert_engine = GammaAlertEngine()

        self.last_profile: Optional[Dict[str, Any]] = None
        self.gamma_alerts: Deque[Dict[str, Any]] = deque(maxlen=50)
        self.last_error: Optional[str] = None
        self.processed_updates = 0

    def start(self) -> bool:
        with self._lock:
            if self.is_running:
                return False
            if redis is None:
                raise GammaBotError(
                    "redis-py is not installed; install the 'redis' package in the active environment."
                )
            self._redis_client = redis.Redis.from_url(
                self.redis_url, decode_responses=True, health_check_interval=30
            )
            try:
                self._redis_client.ping()
            except Exception as exc:
                raise GammaBotError(f"Redis is unavailable at {self.redis_url}: {exc}") from exc
            self._stop_event.clear()
            self._prime_from_redis()
            self._thread = threading.Thread(
                target=self._listen, name="gamma-redis-listener", daemon=True
            )
            self._thread.start()
            return True

    def stop(self) -> bool:
        with self._lock:
            if not self.is_running:
                return False
            self._stop_event.set()
            return True

    @property
    def is_running(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    def status(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "running": self.is_running,
                "redis_url": self.redis_url,
                "data_key": self.data_key,
                "channel": self.channel,
                "processed_updates": self.processed_updates,
                "gamma_alerts": list(self.gamma_alerts),
                "last_profile": self.last_profile,
                "last_error": self.last_error,
            }

    def _prime_from_redis(self) -> None:
        if self._redis_client is None:
            return
        try:
            raw = self._redis_client.get(self.data_key)
            if raw:
                self._process_message(raw)
        except Exception as exc:
            self.last_error = str(exc)
            logger.exception("Prime from Redis failed")

    def _listen(self) -> None:
        try:
            with self._redis_client.pubsub(ignore_subscribe_messages=True) as pubsub:
                pubsub.subscribe(self.channel)
                while not self._stop_event.is_set():
                    try:
                        message = pubsub.get_message(timeout=1.0)
                        if message and message.get("type") == "message":
                            self._process_message(message.get("data"))
                    except Exception as exc:
                        self.last_error = str(exc)
                        logger.exception("Listener iteration failed")
                        time.sleep(0.05)
        except Exception as exc:
            self.last_error = str(exc)
            logger.exception("Listener thread crashed")

    def _process_message(self, message_data: Any) -> None:
        with self._lock:
            try:
                raw_payload = (
                    json.loads(message_data)
                    if isinstance(message_data, str) and message_data.strip().startswith("{")
                    else self._redis_client.get(self.data_key)
                )
                if isinstance(raw_payload, str):
                    raw_payload = json.loads(raw_payload)
                if not isinstance(raw_payload, dict):
                    raise ValueError("Redis update did not contain a JSON object")
                payload = normalize_live_payload(raw_payload)
                if not payload:
                    raise ValueError("Redis payload is missing option-chain data")
                profile = self.processor.process_payload(payload)
                if not profile:
                    raise ValueError("Redis payload is missing valid spot, expiry, or strike data")

                for message in self.alert_engine.evaluate_gamma_alerts(profile):
                    self.gamma_alerts.append({
                        "timestamp": datetime.now(LOCAL_TZ).isoformat(timespec="seconds"),
                        "message": message,
                    })

                self.last_profile = {
                    "timestamp": datetime.now(LOCAL_TZ).isoformat(timespec="seconds"),
                    "spot": profile.spot,
                    "expiry": profile.expiry,
                    "dte": profile.dte,
                    "atm_iv": profile.atm_iv,
                    "gamma_flip_level": profile.gamma_flip_level,
                    "net_gex": profile.net_gex,
                    "net_gex_display": profile.net_gex_display,
                    "pcr": profile.pcr,
                    "skew": profile.skew,
                    "call_wall": profile.call_wall,
                    "put_wall": profile.put_wall,
                    "lot_size": profile.contract_divisor,
                    "oi_unit": profile.oi_unit,
                    "exposure_units": CONFIG["exposure_units"],
                    "weekly_gamma": asdict(profile.weekly_gamma),
                    "day_gamma": asdict(profile.day_gamma),
                    # V2: surface the combined (weekly+day) signal at the top
                    # level so dashboards don't have to drill in.
                    "combined_signal": profile.day_gamma.combined_signal,
                    "combined_reasons": profile.day_gamma.combined_reasons,
                    "day_signal_score": profile.day_gamma.day_signal_score,
                    "day_signal_reasons": profile.day_gamma.day_signal_reasons,
                    "foreign_expiry_rows": payload.get("foreign_expiry_rows", 0),
                    "foreign_expiries": payload.get("foreign_expiries", []),
                    "foreign_underlying_rows": payload.get("foreign_underlying_rows", 0),
                }
                self.processed_updates += 1
                self.last_error = None
            except Exception as exc:
                self.last_error = str(exc)
                logger.exception("Failed to process Redis message")


_default_bot: Optional[RedisGammaBot] = None
_default_bot_lock = threading.Lock()


def get_gamma_bot() -> RedisGammaBot:
    global _default_bot
    with _default_bot_lock:
        if _default_bot is None:
            _default_bot = RedisGammaBot()
        return _default_bot


def main() -> None:
    logger.info("Starting Gamma Blast Engine (V2 — revamped Day GEX signal)...")

    bot = RedisGammaBot()
    try:
        bot.start()
    except GammaBotError as e:
        logger.error("Error: %s", e)
        return

    logger.info("Connected to Redis at %s", bot.redis_url)
    logger.info("Listening on channel '%s' (data key: %s)", bot.channel, bot.data_key)
    logger.info(
        "Timezone: '%s' -> now=%s; NSE open %02d:%02d / close %02d:%02d.",
        _DEFAULT_TZ_NAME,
        datetime.now(LOCAL_TZ).isoformat(timespec="seconds"),
        NSE_MARKET_OPEN_HOUR, NSE_MARKET_OPEN_MINUTE,
        NSE_MARKET_CLOSE_HOUR, NSE_MARKET_CLOSE_MINUTE,
    )
    logger.info(
        "OI/COI input scale: %s (lot size %d) -> exposures reported per contract in %s units.",
        CONFIG["oi_input_scale"], CONFIG["lot_size"], CONFIG["exposure_units"],
    )
    logger.info(
        "V2 day-signal config: neutral_base=%.2f, dte_ref=%.1fd, iv_ref=%.1f%%, "
        "bank_mult=%.2f, atm_decay=%dpts, persistence=%d, session_ramp=%.0f%%",
        CONFIG["day_neutral_base"], CONFIG["day_dte_ref_days"], CONFIG["day_iv_ref"],
        CONFIG["day_bank_mult"], CONFIG["atm_decay_points"],
        CONFIG["persistence_window"], CONFIG["session_ramp_fraction"] * 100,
    )
    logger.info("Auto-trade code REMOVED. Engine emits gamma analytics + alerts only.")
    logger.info("Waiting for live option-chain updates. Press Ctrl+C to exit.")

    try:
        while not bot._stop_event.wait(1.0):
            if not bot.is_running:
                logger.warning("Listener thread stopped unexpectedly - restarting...")
                try:
                    bot.start()
                except GammaBotError as e:
                    logger.error("Error: %s", e)
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt received - shutting down gracefully...")
    except Exception as e:
        logger.exception("Main loop error: %s", e)
    finally:
        bot.stop()
        logger.info("Gamma Blast Engine stopped.")


if __name__ == "__main__":
    main()

"""Redis-driven Gamma Blast analytics worker.

CLEAN VERSION - what is IN:
  1. Black-Scholes greeks (gamma, charm, vanna) with the textbook put_charm
     correction term (-r * e^(-rT) * N(-d2)) included.
  2. Linear interpolation for Gamma Flip levels.
  3. True DTE against NSE session close (15:30 IST), tz-aware via zoneinfo.
  4. Contract-scaled exposures: lot factor divided out at payload entry so
     GEX/charm/vanna are per-contract, not 65x inflated.
  5. Compact ``*_display`` strings ("-1.07 Cr") alongside every raw exposure.
  6. Gamma regime signals (weekly + day) and GEX zero-crossing alerts.
  7. Redis pub/sub listener with thread-safe state, health-check, and clean
     shutdown via Event().wait().

CLEAN VERSION - what is OUT (auto-trade code, removed per request):
  - EntrySignal dataclass
  - SignalEngine.evaluate_entry / manage_trade / active_trade tracking
  - 2-minute OHLC candle grouping for ATR
  - _compute_true_atr / _volume_surge
  - Trade-related CONFIG (capital, risk_per_trade, max_positions, ATR
    multipliers, volume_surge_mult, history_size, candle_minutes)
  - All broker/order executor classes
  - previous_spot, last_signal trade events, _prime trade rehydration

BUG FIXES APPLIED (vs. the original):
  - weekly_gamma.gross_gex / net_gex_ratio now computed (was always 0.0).
  - net_charm / net_vanna sign flipped to match the dealer perspective used
    by net_gex (calls short -> negative, puts long -> positive).
  - put_charm formula now includes the -r*e^(-rT)*N(-d2) correction that was
    silently dropped in the original (only exact when r=0).
  - _repair_iv_surface rejects negative IVs, not just zeros.
  - _find_spot_price handles non-string dict keys (was AttributeError on
    integer keys, swallowed by the broad except in _process_message).
  - Wall computation guards against all-zero OI (no misleading wall when
    no real position exists at any strike).
  - _apply_display_fields no longer called twice for weekly_gamma.
  - Thread safety: _process_message and status() hold self._lock so the
    dashboard reader can't see torn state.
  - logging module used instead of swallowed exceptions in _process_message.
  - Redis client built with health_check_interval=30 for auto-reconnect.
  - main() loop uses Event().wait(1) so Ctrl+C is instantaneous.
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
from typing import Any, Deque, Dict, List, Optional

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
    # Exposure scale of the INBOUND Redis payload.
    # upstox_live_feed._scale_oi_to_internal() publishes every OI/COI figure as
    # upstox_oi * LOT_SIZE (65 for NIFTY), i.e. in SHARES, not contracts.
    # Consumed as-is, every GEX/charm/vanna sum below came out exactly LOT_SIZE
    # (65x) too large. "shares" divides that lot factor back out so all
    # exposures are per CONTRACT; set GAMMA_OI_INPUT_SCALE=contracts if a
    # producer ever publishes raw lot counts.
    "oi_input_scale": os.getenv("GAMMA_OI_INPUT_SCALE", "shares").strip().lower(),
    # Compact units for the *_display strings: "indian" -> K/L/Cr,
    # "international" -> K/M/B/T.
    "exposure_units": os.getenv("GAMMA_EXPOSURE_UNITS", "indian").strip().lower(),
    # Day-GEX neutral band (fraction of gross_gex); within +/- this fraction
    # the regime is reported as range-bound.
    "day_gex_neutral_fraction": float(os.getenv("GAMMA_DAY_NEUTRAL_FRACTION", "0.15")),
}

REDIS_URL = os.getenv("REDIS_URL", "redis://127.0.0.1:6379/0")
REDIS_DATA_KEY = os.getenv("GAMMA_REDIS_DATA_KEY", "live_options_data")
REDIS_CHANNEL = os.getenv("GAMMA_REDIS_CHANNEL", "gamma:live_options_updated")

DAYS_PER_YEAR = 365.0
SECONDS_PER_YEAR = DAYS_PER_YEAR * 24.0 * 60.0 * 60.0
VOL_POINTS_PER_UNIT = 100.0
MIN_DTE_YEARS = 1.0 / (DAYS_PER_YEAR * 24.0)
# Precomputed sqrt(2*pi) for the Black-Scholes normal density (per strike).
SQRT_2PI = math.sqrt(2.0 * math.pi)
# Precomputed sqrt(2) for the erf-based normal CDF (no scipy dependency).
SQRT_2 = math.sqrt(2.0)

# --- Timezone handling ------------------------------------------------------
# The strategy prices DTE against the NSE session close (15:30 IST) and the
# host running the worker may be in UTC (typical on cloud servers). Naive
# datetime.now() calls would silently shift every DTE by 5h30m and every
# alert timestamp by the same. So every datetime.now() in this module is
# datetime.now(LOCAL_TZ) and _fractional_dte attaches LOCAL_TZ to expiry_close
# so the subtraction is between two aware datetimes (Python refuses to
# subtract aware-naive pairs).
#
# Override for non-NSE markets via env: GAMMA_TIMEZONE="America/New_York"
# (uses zoneinfo when available, falls back to a fixed +05:30 IST offset).
_DEFAULT_TZ_NAME = os.getenv("GAMMA_TIMEZONE", "Asia/Kolkata").strip() or "Asia/Kolkata"
try:
    from zoneinfo import ZoneInfo  # Python 3.9+
    LOCAL_TZ = ZoneInfo(_DEFAULT_TZ_NAME)
except (ImportError, ValueError, KeyError):
    # zoneinfo missing or the supplied name is invalid -> fixed +05:30 IST.
    LOCAL_TZ = timezone(timedelta(hours=5, minutes=30), name="IST")
# NSE market close in LOCAL_TZ (15:30 IST). Used by _fractional_dte.
NSE_MARKET_CLOSE_HOUR = 15
NSE_MARKET_CLOSE_MINUTE = 30

# --- Logging ---------------------------------------------------------------
# Use the stdlib logging module instead of print() so the worker can be silent
# in production and chatty in dev just by setting the level. The logger is
# module-scoped so callers can attach additional handlers (e.g. syslog).
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


# Producer-side ``scale`` hints that already mean "one unit == one contract".
CONTRACT_SCALE_HINTS = frozenset({"contract", "contracts", "lot", "lots"})

# Compact suffix tables used by format_exposure(). Ordered high -> low.
INDIAN_UNITS = ((1e7, "Cr"), (1e5, "L"), (1e3, "K"))
INTERNATIONAL_UNITS = ((1e12, "T"), (1e9, "B"), (1e6, "M"), (1e3, "K"))


def resolve_oi_divisor(payload: Optional[Dict[str, Any]] = None) -> float:
    """Return the factor that converts published OI/COI back into CONTRACTS.

    The live feed stores oi_shares = oi_contracts * LOT_SIZE (see
    upstox_live_feed._scale_oi_to_internal), so every exposure built from those
    figures was inflated by exactly the lot size. Dividing the OI/COI by this
    factor *before* the greeks are applied removes that inflation once, at the
    single point where the payload enters the engine, which keeps GEX, charm,
    vanna, the COI flows and the walls all in the same (per-contract) unit.

    The payload wins when it declares its own convention (lot_size/scale), so a
    producer that switches to a different lot size - or to raw lot counts - is
    honoured without touching the engine. 1.0 means "leave the numbers alone".
    """
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
    """Render a raw exposure as a short, dashboard-friendly string.

    -1072130050043 becomes "-107,213.01 Cr" in Indian units (or "-1.07 T" in
    international ones) instead of a 14-digit wall of numbers. Non-finite
    input is reported as "--" so the UI never prints "nan".
    """
    number = _number(value, default=float("nan"))
    if not math.isfinite(number):
        return "--"
    scale = str(units or CONFIG.get("exposure_units") or "indian").strip().lower()
    table = INTERNATIONAL_UNITS if scale.startswith("int") else INDIAN_UNITS
    if number == 0:
        # Surface a unit even for zero so the dashboard cell stays consistent
        # with neighbouring "X Cr" cells. Original returned bare "0".
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
    """Return (gamma, call_charm, put_charm, vanna) for a European option.

    All charm values are per-day (calendar time, T decreasing); vanna is per
    unit of decimal volatility. The put_charm now includes the textbook
    correction term -r*e^(-rT)*N(-d2) which the original silently dropped; with
    r=0.065 and short DTE the missing term was small but non-zero and grew for
    longer-dated options.
    """
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

        # charm_core = d(call_delta)/dT (per year). Wikipedia formula:
        # -N'(d1) * (2rT - d2*sigma*sqrt(T)) / (2 * T * sigma * sqrt(T))
        charm_core = (
            normal_pdf
            * (2.0 * rate * dte - d2 * vol_sqrt_dte)
            / (2.0 * dte * vol_sqrt_dte)
        )
        charm_core /= DAYS_PER_YEAR  # per year -> per calendar day

        # call_charm is the rate of CHANGE of call delta as time passes (T
        # decreases). Wikipedia's sign convention has the negation baked in.
        call_charm = -charm_core

        # FIX: put_charm = charm_call + r*e^(-rT)*N(-d2) (per day). The original
        # used put_charm = +charm_core, which equals call_charm's negation and
        # drops the r*e^(-rT)*N(-d2) correction. For r != 0 and T > 0 that term
        # is non-zero, so the original was an approximation. Now exact.
        n_neg_d2 = _norm_cdf(-d2)
        put_charm_correction = (rate * math.exp(-rate * dte) * n_neg_d2) / DAYS_PER_YEAR
        put_charm = charm_core - put_charm_correction

        # vanna = d(call_delta)/d(sigma) per unit of decimal vol, scaled to
        # "per percentage point of IV" by VOL_POINTS_PER_UNIT.
        vanna = (-normal_pdf * d2 / volatility) / VOL_POINTS_PER_UNIT

        values = (gamma, call_charm, put_charm, vanna)
        return values if all(math.isfinite(v) for v in values) else (0.0, 0.0, 0.0, 0.0)
    except (ArithmeticError, ValueError):
        return 0.0, 0.0, 0.0, 0.0


def gamma_regime_signal(net_gex: float, pcr: float) -> str:
    if net_gex < 0 and pcr < 0.7:
        return "🔴 Negative Gamma: Market Weak / BUY PE"
    if net_gex > 0 and pcr >= 0.7:
        return "🟢 Positive Gamma: Stable Market / Buy Dips"
    return "🟡 Range-bound: Choppy / Transition Zone"


def day_gamma_regime_signal(net_gex: float, gross_gex: float) -> str:
    if not math.isfinite(net_gex) or not math.isfinite(gross_gex) or gross_gex <= 0:
        return "🟡 Range-bound: Choppy / Transition Zone"
    ratio = net_gex / gross_gex
    neutral = float(CONFIG.get("day_gex_neutral_fraction", 0.15))
    if ratio <= -neutral:
        return "🔴 Negative Gamma: Market Weak / BUY PE"
    if ratio >= neutral:
        return "🟢 Positive Gamma: Stable Market / Buy Dips"
    return "🟡 Range-bound: Choppy / Transition Zone"


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
    # Pre-formatted, compact copies of the exposures above (e.g. "-1.07 Cr").
    # Raw floats stay untouched for maths/alerts; the dashboard renders these
    # strings so a multi-crore exposure doesn't show up as a 14-digit number.
    net_gex_display: str = ""
    gross_gex_display: str = ""
    net_charm_display: str = ""
    net_vanna_display: str = ""
    call_flow_display: str = ""
    put_flow_display: str = ""
    exposure_unit: str = "contracts"


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
    # Lot factor that was divided out of the inbound OI/COI (1.0 = payload was
    # already per contract) plus the resulting unit, both for traceability.
    contract_divisor: float = 1.0
    oi_unit: str = "contracts"
    net_gex_display: str = ""


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

        # The feed publishes OI/COI in SHARES (contracts * lot size). Undo
        # that exactly once, here, so every downstream figure (GEX, charm,
        # vanna, COI flows, walls) is per CONTRACT instead of 65x too large.
        oi_divisor = resolve_oi_divisor(payload)
        strikes = [self._to_strike(row, oi_divisor) for row in rows if isinstance(row, dict)]
        strikes = [strike for strike in strikes if strike is not None]
        if not strikes:
            return None

        dte = self._fractional_dte(expiry)
        self._repair_iv_surface(strikes)
        # Hoist loop-invariant values: the spot-scaled GEX factor and the
        # risk-free rate never change across strikes, so compute them once.
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
                # FIX: net_charm / net_vanna now use the SAME dealer sign
                # convention as net_gex (calls short -> negative, puts long
                # -> positive). Previously they used the opposite (customer)
                # convention, which meant any future charm/vanna flip alert
                # would trigger on the wrong sign.
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
        # Build the expiry-close datetime as tz-AWARE in LOCAL_TZ so the
        # subtraction against datetime.now(LOCAL_TZ) is valid (Python raises
        # TypeError when subtracting aware-naive pairs). On a UTC host, using
        # naive datetimes here used to shift DTE by ~5h30m and silently corrupt
        # every greek derived downstream.
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
        """Build one strike, converting the published OI/COI back into contracts.

        ``oi_divisor`` is the lot factor to remove (see resolve_oi_divisor);
        when omitted the configured lot size is used, so a direct caller gets
        the same per-contract scale as process_payload. A ``coi`` of ``None``
        still means "unknown" and must survive the conversion untouched - only
        a real number is rescaled.
        """
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
        """Replace missing/invalid IVs with the chain mean.

        FIX vs original: the original used ``strike.call_iv or default`` which
        only catches falsy 0.0; a stray negative IV would slip through and break
        math.log/sqrt downstream. Now we explicitly check ``> 0`` so any
        non-positive IV is repaired.
        """
        valid_call_ivs = [s.call_iv for s in strikes if s.call_iv > 0]
        valid_put_ivs = [s.put_iv for s in strikes if s.put_iv > 0]
        default_call_iv = sum(valid_call_ivs) / len(valid_call_ivs) if valid_call_ivs else 15.0
        default_put_iv = sum(valid_put_ivs) / len(valid_put_ivs) if valid_put_ivs else 15.0
        for strike in strikes:
            strike.call_iv = strike.call_iv if strike.call_iv > 0 else default_call_iv
            strike.put_iv = strike.put_iv if strike.put_iv > 0 else default_put_iv

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
        # FIX: weekly gross_gex and net_gex_ratio were never computed in the
        # original (only day_gamma.gross_gex was). Without these the weekly
        # gross_gex_display always rendered "0" and any code that wanted the
        # weekly regime ratio (like the day-side signal) had nothing to read.
        profile.weekly_gamma.gross_gex = sum(
            abs(s.call_gex) + abs(s.put_gex) for s in profile.strikes
        )
        profile.weekly_gamma.net_gex_ratio = (
            round(profile.weekly_gamma.net_gex / profile.weekly_gamma.gross_gex, 4)
            if profile.weekly_gamma.gross_gex > 0 else 0.0
        )
        profile.day_gamma.gross_gex = sum(
            abs(s.day_call_gex) + abs(s.day_put_gex) for s in day_strikes
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
        profile.day_gamma.signal = day_gamma_regime_signal(
            profile.day_gamma.net_gex, profile.day_gamma.gross_gex
        )

        profile.net_gex = profile.weekly_gamma.net_gex
        profile.pcr = profile.weekly_gamma.pcr
        profile.gamma_flip_level = profile.weekly_gamma.gamma_flip_level

        # FIX: walls guard against all-zero OI. The original ``max(...)``
        # returned the first strike even when every OI was 0, publishing a
        # misleading "wall" at a strike with no real position. Now None.
        if any(s.call_oi > 0 for s in profile.strikes):
            profile.call_wall = max(profile.strikes, key=lambda s: s.call_oi).strike
        if any(s.put_oi > 0 for s in profile.strikes):
            profile.put_wall = max(profile.strikes, key=lambda s: s.put_oi).strike

        atm = min(profile.strikes, key=lambda s: abs(s.strike - profile.spot))
        profile.skew = atm.put_iv - atm.call_iv

        # Refresh compact display strings LAST so gross_gex (set just above)
        # is included. _metrics_for already called _apply_display_fields once
        # with gross_gex=0; re-calling here overwrites with the real values.
        GammaProcessor._apply_display_fields(profile.weekly_gamma)
        GammaProcessor._apply_display_fields(profile.day_gamma)
        profile.net_gex_display = format_exposure(profile.net_gex)

    @staticmethod
    def _apply_display_fields(metrics: GammaMetrics) -> GammaMetrics:
        """Fill the compact ``*_display`` strings from the raw exposure floats."""
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
            # Gross absolute flow per leg: sum |coi| per strike so signed
            # build/unwind flows can't cancel into a near-zero denominator
            # (which produced absurd day PCRs like 95.566). call_total /
            # put_total above stay signed for call_flow/put_flow below.
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
        """Return the dominant zero-crossing of cumulative net GEX near spot.

        The cumulative sum is seeded from the absolute bottom of the supplied
        chain (not reset at the band edge) so every crossing tested is a
        chain-wide crossing. Candidate crossings are restricted to
        gamma_flip_band around spot. When signed data (day COI) produces
        multiple crossings, the one with the largest cumulative swing wins;
        ties break toward spot. None means no flip exists.
        """
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
            # The chain is sorted ascending, so once we pass the band nothing
            # above it can ever contribute to a crossing near spot - stop.
            if strike_price > upper_edge:
                break

            strike_gex = getattr(strike, call_gex_attr) + getattr(strike, put_gex_attr)

            # Seed the running total from every strike below the band so the
            # first in-band cumulative is chain-absolute rather than band-relative.
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


# === GammaAlertEngine ======================================================
# Renamed from SignalEngine - the class no longer generates trade signals
# (auto-trade code removed per request); it only emits gamma-flip alerts.
# The previous name was kept misleading. The class still tracks previous
# GEX/PCR state to detect zero crossings between updates.

class GammaAlertEngine:
    """Detects GEX zero crossings and PCR drops between consecutive updates."""

    def __init__(self) -> None:
        self._previous_gamma_expiry: Optional[str] = None
        self._previous_weekly_gex: Optional[float] = None
        self._previous_day_gex: Optional[float] = None
        self._previous_day_pcr: Optional[float] = None

    def evaluate_gamma_alerts(self, profile: GammaProfile) -> List[str]:
        weekly = profile.weekly_gamma
        day = profile.day_gamma
        if profile.expiry != self._previous_gamma_expiry:
            self._previous_gamma_expiry = profile.expiry
            self._previous_weekly_gex = weekly.net_gex
            self._previous_day_gex = day.net_gex
            self._previous_day_pcr = day.pcr
            return []

        alerts: List[str] = []
        if self._crossed_below_zero(self._previous_weekly_gex, weekly.net_gex):
            alerts.append("⚠️ Weekly Alert: Weekly GEX flipped Negative (Macro selling pressure building)")
        elif self._crossed_above_zero(self._previous_weekly_gex, weekly.net_gex):
            alerts.append("✅ Weekly Alert: Weekly GEX flipped Positive (Macro stability improving)")

        if self._crossed_below_zero(self._previous_day_gex, day.net_gex):
            alerts.append("⚡ Intraday Alert: Day GEX flipped Negative (Selling pressure building)")
        elif self._crossed_above_zero(self._previous_day_gex, day.net_gex):
            alerts.append("⚡ Intraday Alert: Day GEX flipped Positive (Buying support improving)")

        previous_pcr = self._previous_day_pcr
        if previous_pcr is not None and previous_pcr > 0 and day.pcr < previous_pcr:
            drop = previous_pcr - day.pcr
            if drop >= max(0.2, previous_pcr * 0.25):
                alerts.append("⚠️ Intraday Alert: Sharp drop in Put writing detected")

        self._previous_weekly_gex = weekly.net_gex
        self._previous_day_gex = day.net_gex
        self._previous_day_pcr = day.pcr
        return alerts

    @staticmethod
    def _crossed_below_zero(previous: Optional[float], current: float) -> bool:
        return previous is not None and previous >= 0 > current

    @staticmethod
    def _crossed_above_zero(previous: Optional[float], current: float) -> bool:
        return previous is not None and previous <= 0 < current


# === Payload normalization ================================================

_SYMBOL_SOURCE_FIELDS = ("tradingsymbol", "trading_symbol", "symbol", "underlying", "underlying_name")


def _is_foreign_underlying(item: Dict[str, Any], target: str) -> bool:
    """True only when an entry's symbol PROVABLY belongs to a different underlying.

    The engine sums GEX/charm/vanna across every row it is given and derives
    the gamma flip, PCR and the walls from those sums, so a single leaked
    MIDCPNIFTY/BANKNIFTY or stock-option strike silently distorts the whole
    profile - it lands at a strike level (15225, 52000, 3000) that has nothing
    to do with NIFTY.

    A genuine NIFTY option symbol is the underlying followed IMMEDIATELY by
    the expiry code, which always starts with a digit. Sibling indices either
    fail the prefix (BANKNIFTY..., MIDCPNIFTY..., RELIANCE...) or put a LETTER
    right after it (NIFTYNXT50..., NIFTYFPI...). An entry with no symbol at
    all is NOT foreign: the feed is the only writer and it publishes one
    underlying, so "unknown" must never drop data.
    """
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
    """Normalize a live snapshot into {spot_price, expiry_date, data:[rows]}.

    Rows are keyed by the COMPOSITE (expiry_date, strike_price). Keying them
    by the bare strike let a payload that carried two expiries collapse into
    one chain: the CE leg of expiry A and the PE leg of expiry B were written
    into the SAME row, and every aggregate built from it (COI flow, day GEX,
    PCR, gamma flip) then mixed two unrelated chains while process_payload
    priced the whole lot at a single DTE.

    Instruments that provably belong to an expiry other than
    payload["expiry_date"] are dropped, because that DTE is what the greeks
    are computed with. Instruments whose expiry cannot be determined are kept
    and folded into the active bucket.
    """
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

    # Sorted by (expiry, strike) so the strike series handed to
    # _find_gamma_flip and the spot estimator is always monotonic per expiry
    # rather than dict-insertion ordered.
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
    """Recursively locate the spot price in a nested payload.

    FIX vs original: ``key.lower()`` would raise AttributeError when ``key``
    was an int (common in instrument_data where keys are instrument tokens).
    The exception was then swallowed by _process_message's broad except,
    silently dropping the entire payload. Now non-string keys are skipped
    explicitly via isinstance check.
    """
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
        # Lock guards every mutation of self.closes/self.last_profile/etc.
        # so a dashboard reader calling status() concurrently can never see
        # torn state (the original had _lock defined but only used it in
        # start()/stop(); _process_message mutated shared fields without it).
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
            # health_check_interval=30 lets the socket detect dead
            # connections and reconnect automatically. The original built the
            # client without it, so a stale socket would silently hang on
            # pubsub.get_message() forever after a network blip.
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
        # Snapshot under the lock so a dashboard reader never sees a
        # half-updated last_profile or a deque mid-rotation.
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
                    # A single bad message or transient Redis error must not
                    # kill the listener thread; catch per iteration and keep polling.
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
        # Hold the lock for the whole update so status() can never read torn
        # state mid-update. The original mutated last_profile / processed_updates
        # / gamma_alerts without any synchronization.
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
                    "foreign_expiry_rows": payload.get("foreign_expiry_rows", 0),
                    "foreign_expiries": payload.get("foreign_expiries", []),
                    "foreign_underlying_rows": payload.get("foreign_underlying_rows", 0),
                }
                self.processed_updates += 1
                self.last_error = None
            except Exception as exc:
                self.last_error = str(exc)
                # FIX: original silently swallowed every error into last_error
                # with no stack trace. Now log the full exception so a corrupt
                # chain or a missing field is debuggable.
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
    logger.info("Starting Gamma Blast Engine...")

    bot = RedisGammaBot()
    try:
        bot.start()
    except GammaBotError as e:
        logger.error("Error: %s", e)
        return

    logger.info("Connected to Redis at %s", bot.redis_url)
    logger.info("Listening on channel '%s' (data key: %s)", bot.channel, bot.data_key)
    logger.info(
        "Timezone: '%s' -> now=%s; NSE close %02d:%02d; override via GAMMA_TIMEZONE.",
        _DEFAULT_TZ_NAME,
        datetime.now(LOCAL_TZ).isoformat(timespec="seconds"),
        NSE_MARKET_CLOSE_HOUR,
        NSE_MARKET_CLOSE_MINUTE,
    )
    logger.info(
        "OI/COI input scale: %s (lot size %d) -> exposures reported per contract in %s units.",
        CONFIG["oi_input_scale"], CONFIG["lot_size"], CONFIG["exposure_units"],
    )
    logger.info("Auto-trade code REMOVED. Engine emits gamma analytics + alerts only.")
    logger.info("Waiting for live option-chain updates. Press Ctrl+C to exit.")

    try:
        # Event().wait() returns immediately when set, so Ctrl+C is instant
        # (the original time.sleep(1) could take up to 1s to notice SIGINT).
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

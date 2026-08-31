"""Redis-driven Gamma Blast strategy worker.

Final Version Includes:
1. Linear interpolation for precise Gamma Flip levels.
2. Granular volume Surge (measuring volume near the flip, not the whole chain).
3. True ATR Calculation (accounting for Highs, Lows, and gaps).
4. Dynamic Risk-Free Rate via environment variables.
5. 2-Minute Timeframe Grouping: Groups live Redis ticks into 2m OHLC candles 
   so ATR and Stop Loss are sized correctly for intraday volatility.
6. Contract-scaled exposures: the feed publishes OI/COI in shares (contracts * lot
   size), so the lot factor is divided back out before the Gamma/Charm/Vanna formulas
   run - NET GEX/CHARM/VANNA are no longer 65x too large.
7. Compact ``*_display`` strings ("-1.07 Cr") published next to every raw exposure so
   the dashboard never has to print a 14-digit number.
"""

from __future__ import annotations

import json
import math
import os
import threading
import time
from collections import deque
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta
from typing import Any, Deque, Dict, Iterable, List, Optional

try:
    import redis
except ImportError:
    redis = None

# Shared (expiry_date, strike_price) keying, used by the live feed and the dashboard too
# so producer and consumers can never disagree about which chain a strike belongs to.
from Nse_Project.expiry_utils import (
    belongs_to_expiry,
    expiry_group_key,
    normalize_expiry,
    resolve_row_expiry,
)


CONFIG = {
    "symbol": os.getenv("GAMMA_SYMBOL", "NIFTY"),
    "lot_size": int(os.getenv("GAMMA_LOT_SIZE", "65")),
    "volume_surge_mult": 1.8,
    "atr_sl_mult": 1.2,
    "atr_target_mult": 2.5,
    "risk_per_trade": 0.02,
    "capital": 100000,
    "max_positions": 1,
    "history_size": 120,
    "gamma_flip_band": 500,
    "risk_free_rate": float(os.getenv("RISK_FREE_RATE", "0.065")), 
    "candle_minutes": 2,  # Timeframe for OHLC grouping
    # --- Exposure scale of the INBOUND Redis payload --------------------------------
    # upstox_live_feed._scale_oi_to_internal() publishes every OI/COI figure as
    # ``upstox_oi * LOT_SIZE`` (65 for NIFTY), i.e. in SHARES, not contracts. Consumed
    # as-is, every GEX/charm/vanna sum below came out exactly LOT_SIZE (65x) too large
    # (the 1,07,21,30,50,043-style numbers on the dashboard). "shares" divides that lot
    # factor back out so all exposures are per CONTRACT; set
    # GAMMA_OI_INPUT_SCALE=contracts if a producer ever publishes raw lot counts.
    "oi_input_scale": os.getenv("GAMMA_OI_INPUT_SCALE", "shares").strip().lower(),
    # Compact units for the *_display strings: "indian" -> K/L/Cr, "international" -> K/M/B/T.
    "exposure_units": os.getenv("GAMMA_EXPOSURE_UNITS", "indian").strip().lower(),
}

REDIS_URL = os.getenv("GAMMA_REDIS_URL", "redis://127.0.0.1:6379/0")
REDIS_DATA_KEY = os.getenv("GAMMA_REDIS_DATA_KEY", "live_options_data")
REDIS_CHANNEL = os.getenv("GAMMA_REDIS_CHANNEL", "gamma:live_options_updated")

DAYS_PER_YEAR = 365.0
SECONDS_PER_YEAR = DAYS_PER_YEAR * 24.0 * 60.0 * 60.0
VOL_POINTS_PER_UNIT = 100.0
MIN_DTE_YEARS = 1.0 / (DAYS_PER_YEAR * 24.0)


class GammaBotError(RuntimeError):
    """Raised when the Redis-driven worker cannot be started."""


def _number(value: Any, default: float = 0.0) -> float:
    try:
        number = default if value in (None, "") else float(value)
        return number if math.isfinite(number) else default
    except (TypeError, ValueError):
        return default


# Producer-side ``scale`` hints that already mean "one unit == one contract", so there is
# no lot factor left to undo.
CONTRACT_SCALE_HINTS = frozenset({"contract", "contracts", "lot", "lots"})

# Compact suffix tables used by format_exposure(). Ordered high -> low.
INDIAN_UNITS = ((1e7, "Cr"), (1e5, "L"), (1e3, "K"))
INTERNATIONAL_UNITS = ((1e12, "T"), (1e9, "B"), (1e6, "M"), (1e3, "K"))


def resolve_oi_divisor(payload: Optional[Dict[str, Any]] = None) -> float:
    """Return the factor that converts published OI/COI back into CONTRACTS.

    The live feed stores ``oi_shares = oi_contracts * LOT_SIZE`` (see
    ``upstox_live_feed._scale_oi_to_internal``), so every exposure the engine built from
    those figures was inflated by exactly the lot size. Dividing the OI/COI by this
    factor *before* the greeks are applied removes that inflation once, at the single
    point where the payload enters the engine, which keeps GEX, charm, vanna, the COI
    flows and the walls all in the same (per-contract) unit.

    The payload wins when it declares its own convention (``lot_size``/``scale``), so a
    producer that switches to a different lot size - or to raw lot counts - is honoured
    without touching the engine. ``1.0`` means "leave the numbers alone".
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

    ``-1072130050043`` becomes ``-107,213.01 Cr`` in Indian units (or ``-1.07 T`` in
    international ones) instead of a 14-digit wall of numbers. Non-finite input is
    reported as ``--`` so the UI never prints ``nan``.
    """
    number = _number(value, default=float("nan"))
    if not math.isfinite(number):
        return "--"
    if number == 0:
        return "0"
    scale = str(units or CONFIG.get("exposure_units") or "indian").strip().lower()
    table = INTERNATIONAL_UNITS if scale.startswith("int") else INDIAN_UNITS
    magnitude = abs(number)
    for threshold, suffix in table:
        if magnitude >= threshold:
            return f"{number / threshold:,.{precision}f} {suffix}"
    return f"{number:,.0f}" if magnitude >= 1 else f"{number:,.{precision}f}"


def bs_greeks(spot: float, strike: float, dte: float, rate: float, volatility: float) -> tuple[float, float, float, float]:
    if not all(math.isfinite(value) for value in (spot, strike, dte, rate, volatility)):
        return 0.0, 0.0, 0.0, 0.0
    if spot <= 0 or strike <= 0 or dte <= 0 or volatility <= 0:
        return 0.0, 0.0, 0.0, 0.0
    try:
        sqrt_dte = math.sqrt(dte)
        d1 = (math.log(spot / strike) + (rate + 0.5 * volatility**2) * dte) / (volatility * sqrt_dte)
        d2 = d1 - volatility * sqrt_dte
        normal_pdf = math.exp(-0.5 * d1**2) / math.sqrt(2 * math.pi)
        gamma = normal_pdf / (spot * volatility * sqrt_dte)
        
        charm_core = normal_pdf * (2 * rate * dte - d2 * volatility * sqrt_dte) / (2 * dte * volatility * sqrt_dte)
        charm_core /= DAYS_PER_YEAR
        call_charm = -charm_core
        put_charm = charm_core
        
        vanna = (-normal_pdf * d2 / volatility) / VOL_POINTS_PER_UNIT
        values = (gamma, call_charm, put_charm, vanna)
        return values if all(math.isfinite(value) for value in values) else (0.0, 0.0, 0.0, 0.0)
    except (ArithmeticError, ValueError):
        return 0.0, 0.0, 0.0, 0.0


def gamma_regime_signal(net_gex: float, pcr: float) -> str:
    if net_gex < 0 and pcr < 0.7:
        return "🔴 Negative Gamma: Market Weak / BUY PE"
    if net_gex > 0 and pcr >= 0.7:
        return "🟢 Positive Gamma: Stable Market / Buy Dips"
    return "🟡 Range-bound: Choppy / Transition Zone"

DAY_GEX_NEUTRAL_FRACTION = float(os.getenv("GAMMA_DAY_NEUTRAL_FRACTION", "0.15"))

def day_gamma_regime_signal(net_gex: float, gross_gex: float) -> str:
    if not math.isfinite(net_gex) or not math.isfinite(gross_gex) or gross_gex <= 0:
        return "🟡 Range-bound: Choppy / Transition Zone"
    ratio = net_gex / gross_gex
    if ratio <= -DAY_GEX_NEUTRAL_FRACTION:
        return "🔴 Negative Gamma: Market Weak / BUY PE"
    if ratio >= DAY_GEX_NEUTRAL_FRACTION:
        return "🟢 Positive Gamma: Stable Market / Buy Dips"
    return "🟡 Range-bound: Choppy / Transition Zone"


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
    # --- Presentation ---------------------------------------------------------------
    # Pre-formatted, compact copies of the exposures above (e.g. "-1.07 Cr"). The raw
    # floats stay untouched for maths/alerts; the dashboard renders these strings so a
    # multi-crore exposure no longer shows up as a 14-digit number.
    net_gex_display: str = ""
    gross_gex_display: str = ""
    net_charm_display: str = ""
    net_vanna_display: str = ""
    call_flow_display: str = ""
    put_flow_display: str = ""
    # Unit the raw numbers are expressed in, so a consumer can never guess wrong.
    exposure_unit: str = "contracts"


@dataclass
class GammaProfile:
    spot: float
    expiry: str
    dte: float
    strikes: List[OptionStrike] = field(default_factory=list)
    net_gex: float = 0.0
    gamma_flip_level: Optional[float] = None
    call_wall: float = 0.0
    put_wall: float = 0.0
    pcr: float = 0.0
    skew: float = 0.0
    weekly_gamma: GammaMetrics = field(default_factory=GammaMetrics)
    day_gamma: GammaMetrics = field(default_factory=GammaMetrics)
    # Lot factor that was divided out of the inbound OI/COI (1.0 = payload was already
    # per contract) plus the resulting unit, both recorded for traceability.
    contract_divisor: float = 1.0
    oi_unit: str = "contracts"
    net_gex_display: str = ""


@dataclass
class EntrySignal:
    timestamp: str
    symbol: str
    spot: float
    direction: str
    entry_type: str
    entry_price: float
    initial_sl: float
    target: float
    rr: float
    score: int
    position_size: int


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

        # The feed publishes OI/COI in SHARES (contracts * lot size). Undo that exactly
        # once, here, so every downstream figure (GEX, charm, vanna, COI flows, walls) is
        # per CONTRACT instead of 65x too large.
        oi_divisor = resolve_oi_divisor(payload)
        strikes = [self._to_strike(row, oi_divisor) for row in rows if isinstance(row, dict)]
        strikes = [strike for strike in strikes if strike is not None]
        if not strikes:
            return None

        dte = self._fractional_dte(expiry)
        self._repair_iv_surface(strikes)
        for strike in strikes:
            try:
                call_gamma, strike.call_charm, _, strike.call_vanna = bs_greeks(spot, strike.strike, dte, self.risk_free_rate, strike.call_iv / 100)
                put_gamma, _, strike.put_charm, strike.put_vanna = bs_greeks(spot, strike.strike, dte, self.risk_free_rate, strike.put_iv / 100)
                
                gex_scale = spot
                strike.call_gex = -strike.call_oi * call_gamma * gex_scale
                strike.put_gex = strike.put_oi * put_gamma * gex_scale
                strike.net_gex = strike.call_gex + strike.put_gex
                strike.net_charm = strike.call_oi * strike.call_charm - strike.put_oi * strike.put_charm
                strike.net_vanna = strike.call_oi * strike.call_vanna - strike.put_oi * strike.put_vanna
                
                if strike.has_day_coi:
                    strike.day_call_gex = -strike.call_coi * call_gamma * gex_scale
                    strike.day_put_gex = strike.put_coi * put_gamma * gex_scale
                    strike.day_net_charm = strike.call_coi * strike.call_charm - strike.put_coi * strike.put_charm
                    strike.day_net_vanna = strike.call_coi * strike.call_vanna - strike.put_coi * strike.put_vanna
                else:
                    strike.day_call_gex = strike.day_put_gex = 0.0
                    strike.day_net_charm = strike.day_net_vanna = 0.0
                strike.day_net_gex = strike.day_call_gex + strike.day_put_gex
            except (ArithmeticError, TypeError, ValueError):
                pass

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
            expiry_close = datetime.strptime(expiry, "%Y-%m-%d").replace(hour=15, minute=30)
        except ValueError:
            return MIN_DTE_YEARS
        remaining = (expiry_close - datetime.now()).total_seconds() / SECONDS_PER_YEAR
        return max(remaining, MIN_DTE_YEARS)

    @staticmethod
    def _to_strike(row: Dict[str, Any], oi_divisor: Optional[float] = None) -> Optional[OptionStrike]:
        """Build one strike, converting the published OI/COI back into contracts.

        ``oi_divisor`` is the lot factor to remove (see ``resolve_oi_divisor``); when it
        is omitted the configured lot size is used, so a direct caller gets the same
        per-contract scale as ``process_payload``. A ``coi`` of ``None`` still means
        "unknown" and must survive the conversion untouched - only a real number is
        rescaled.
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
        valid_call_ivs = [strike.call_iv for strike in strikes if strike.call_iv > 0]
        valid_put_ivs = [strike.put_iv for strike in strikes if strike.put_iv > 0]
        default_call_iv = sum(valid_call_ivs) / len(valid_call_ivs) if valid_call_ivs else 15.0
        default_put_iv = sum(valid_put_ivs) / len(valid_put_ivs) if valid_put_ivs else 15.0
        for strike in strikes:
            strike.call_iv = strike.call_iv or default_call_iv
            strike.put_iv = strike.put_iv or default_put_iv

    @staticmethod
    def _build_metrics(profile: GammaProfile) -> None:
        profile.weekly_gamma = GammaProcessor._metrics_for(profile.strikes, "call_oi", "put_oi", "net_gex", "net_charm", "net_vanna", profile.spot)
        day_strikes = [strike for strike in profile.strikes if strike.has_day_coi]
        profile.day_gamma = GammaProcessor._metrics_for(
            day_strikes, "call_coi", "put_coi", "day_net_gex", "day_net_charm", "day_net_vanna",
            profile.spot, absolute_pcr=True,
        )
        profile.day_gamma.gross_gex = sum(
            abs(strike.day_call_gex) + abs(strike.day_put_gex) for strike in day_strikes
        )
        profile.day_gamma.net_gex_ratio = (
            round(profile.day_gamma.net_gex / profile.day_gamma.gross_gex, 4)
            if profile.day_gamma.gross_gex > 0 else 0.0
        )
        profile.weekly_gamma.gamma_flip_level = GammaProcessor._find_gamma_flip(profile.strikes, "call_gex", "put_gex", profile.spot)
        profile.day_gamma.gamma_flip_level = GammaProcessor._find_gamma_flip(day_strikes, "day_call_gex", "day_put_gex", profile.spot)
        
        profile.weekly_gamma.signal = gamma_regime_signal(profile.weekly_gamma.net_gex, profile.weekly_gamma.pcr)
        profile.day_gamma.signal = day_gamma_regime_signal(profile.day_gamma.net_gex, profile.day_gamma.gross_gex)
        
        profile.net_gex = profile.weekly_gamma.net_gex
        profile.pcr = profile.weekly_gamma.pcr
        profile.gamma_flip_level = profile.weekly_gamma.gamma_flip_level
        profile.call_wall = max(profile.strikes, key=lambda strike: strike.call_oi).strike
        profile.put_wall = max(profile.strikes, key=lambda strike: strike.put_oi).strike
        atm = min(profile.strikes, key=lambda strike: abs(strike.strike - profile.spot))
        profile.skew = atm.put_iv - atm.call_iv

        # Refresh the compact strings LAST so gross_gex (set above, after _metrics_for)
        # is included and every display value matches the final raw float.
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
    def _metrics_for(strikes: List[OptionStrike], call_field: str, put_field: str, gex_field: str, charm_field: str, vanna_field: str, spot: float, absolute_pcr: bool = False) -> GammaMetrics:
        call_total = sum(getattr(strike, call_field) for strike in strikes)
        put_total = sum(getattr(strike, put_field) for strike in strikes)
        pcr_call = abs(call_total) if absolute_pcr else call_total
        pcr_put = abs(put_total) if absolute_pcr else put_total
        call_wall_strike = max(strikes, key=lambda strike: getattr(strike, call_field)).strike if strikes else None
        put_wall_strike = max(strikes, key=lambda strike: getattr(strike, put_field)).strike if strikes else None
        metrics = GammaMetrics(
            pcr=round(pcr_put / pcr_call, 3) if pcr_call else 0.0,
            net_gex=sum(getattr(strike, gex_field) for strike in strikes),
            gamma_flip_level=None,
            call_wall_strike=call_wall_strike,
            put_wall_strike=put_wall_strike,
            call_wall=call_wall_strike,
            put_wall=put_wall_strike,
            net_charm=sum(getattr(strike, charm_field) for strike in strikes),
            net_vanna=sum(getattr(strike, vanna_field) for strike in strikes),
            strike_count=len(strikes),
            call_flow=call_total,
            put_flow=put_total,
        )
        return GammaProcessor._apply_display_fields(metrics)

    @staticmethod
    def _find_gamma_flip(strikes: List[OptionStrike], call_gex_attr: str, put_gex_attr: str, spot: float) -> Optional[float]:
        """Return the dominant zero-crossing of cumulative net GEX near spot.

        The cumulative sum is seeded from the absolute bottom of the supplied chain
        (not reset at the band edge) so every crossing tested is a chain-wide crossing.
        Candidate crossings are restricted to ``gamma_flip_band`` around spot. When
        signed data (day COI) produces multiple crossings, the one with the largest
        cumulative swing wins; ties break toward spot. ``None`` means no flip exists.
        """
        band = float(CONFIG.get("gamma_flip_band", 500))
        ordered = sorted(strikes, key=lambda strike: strike.strike)
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
            strike_gex = getattr(strike, call_gex_attr) + getattr(strike, put_gex_attr)

            # Seed the running total from every strike below the band so the first
            # in-band cumulative is chain-absolute rather than band-relative.
            if strike.strike < lower_edge:
                cumulative += strike_gex
                continue
            if strike.strike > upper_edge:
                continue

            if prev_strike is None:
                cumulative += strike_gex
                prev_cumulative = cumulative
                prev_strike = strike.strike
                continue

            prev_cumulative = cumulative
            cumulative += strike_gex

            prev_sign = (prev_cumulative > 0) - (prev_cumulative < 0)
            curr_sign = (cumulative > 0) - (cumulative < 0)
            if prev_sign != 0 and prev_sign != curr_sign:
                gex_diff = cumulative - prev_cumulative
                if gex_diff != 0:
                    ratio = abs(prev_cumulative) / abs(gex_diff)
                    level = prev_strike + (strike.strike - prev_strike) * ratio
                    swing = abs(gex_diff)
                    if swing > best_swing or (
                        swing == best_swing
                        and (best_level is None or abs(level - spot) < abs(best_level - spot))
                    ):
                        best_swing = swing
                        best_level = level

            prev_strike = strike.strike

        return best_level

class SignalEngine:
    def __init__(self) -> None:
        self.active_trade: Optional[Dict[str, Any]] = None
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

        alerts = []
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

    @staticmethod
    def _compute_true_atr(highs: Iterable[float], lows: Iterable[float], closes: Iterable[float]) -> float:
        h, l, c = list(highs), list(lows), list(closes)
        if len(c) < 15:
            return 0.0
        
        true_ranges = []
        for i in range(-14, 0):
            tr1 = h[i] - l[i]
            tr2 = abs(h[i] - c[i-1])
            tr3 = abs(l[i] - c[i-1])
            true_ranges.append(max(tr1, tr2, tr3))
            
        return sum(true_ranges) / 14

    @staticmethod
    def _volume_surge(volumes: Iterable[float]) -> float:
        values = list(volumes)
        if len(values) < 10:
            return 0.0
        baseline = sum(values[-10:-1]) / 9
        return values[-1] / max(baseline, 1.0)

    def evaluate_entry(
        self, 
        profile: GammaProfile, 
        previous_spot: float, 
        highs: Iterable[float], 
        lows: Iterable[float], 
        closes: Iterable[float], 
        flip_volumes: Iterable[float]
    ) -> Optional[EntrySignal]:
        
        atr = self._compute_true_atr(highs, lows, closes)
        if atr <= 0:
            return None
            
        flip_level = profile.gamma_flip_level
        if flip_level is None:
            return None

        crossed_up = previous_spot < flip_level <= profile.spot
        crossed_down = previous_spot > flip_level >= profile.spot
        if not (crossed_up or crossed_down):
            return None

        volume_surge = self._volume_surge(flip_volumes)
        flip_distance = abs(profile.spot - flip_level) / profile.spot * 100
        score = min(30, int(max(0, volume_surge - 1.5) * 20)) if volume_surge >= CONFIG["volume_surge_mult"] else 0
        score += min(20, int(flip_distance * 4)) + min(20, int(abs(profile.skew) * 2))
        score += 15 if profile.pcr < 0.6 or profile.pcr > 1.4 else 8 if profile.pcr < 0.8 or profile.pcr > 1.2 else 0
        
        if score < 50:
            return None

        direction = "CE" if crossed_up else "PE"
        sl = profile.spot - atr * CONFIG["atr_sl_mult"] if direction == "CE" else profile.spot + atr * CONFIG["atr_sl_mult"]
        target = profile.spot + atr * CONFIG["atr_target_mult"] if direction == "CE" else profile.spot - atr * CONFIG["atr_target_mult"]
        risk_amount = CONFIG["capital"] * CONFIG["risk_per_trade"]
        size = max(1, int(risk_amount / (abs(profile.spot - sl) * CONFIG["lot_size"])))
        
        signal = EntrySignal(
            datetime.now().isoformat(timespec="seconds"), 
            CONFIG["symbol"], profile.spot, direction, "FLIP_REVERSAL", 
            profile.spot, round(sl, 1), round(target, 1), 
            round(CONFIG["atr_target_mult"] / CONFIG["atr_sl_mult"], 2), 
            score, size
        )
        self.active_trade = {"entry": profile.spot, "direction": direction, "sl": signal.initial_sl, "target": signal.target}
        return signal

    def manage_trade(self, spot: float, atr: float) -> Optional[str]:
        if not self.active_trade or atr <= 0:
            return None
        trade = self.active_trade
        if trade["direction"] == "CE" and spot >= trade["entry"] + atr:
            trade["sl"] = max(trade["sl"], round(spot - atr * 0.8, 1))
        elif trade["direction"] == "PE" and spot <= trade["entry"] - atr:
            trade["sl"] = min(trade["sl"], round(spot + atr * 0.8, 1))
            
        hit_stop = spot <= trade["sl"] if trade["direction"] == "CE" else spot >= trade["sl"]
        hit_target = spot >= trade["target"] if trade["direction"] == "CE" else spot <= trade["target"]
        
        if hit_stop or hit_target:
            reason = "SL_HIT" if hit_stop else "TARGET_HIT"
            self.active_trade = None
            return reason
        return None


_SYMBOL_SOURCE_FIELDS = ("tradingsymbol", "trading_symbol", "symbol", "underlying", "underlying_name")


def _is_foreign_underlying(item: Dict[str, Any], target: str) -> bool:
    """True only when an entry's symbol PROVABLY belongs to a different underlying.

    The engine sums GEX/charm/vanna across every row it is given and derives the gamma
    flip, PCR and the walls from those sums, so a single leaked MIDCPNIFTY/BANKNIFTY or
    stock-option strike silently distorts the whole profile - it lands at a strike level
    (15225, 52000, 3000) that has nothing to do with NIFTY.

    A genuine NIFTY option symbol is the underlying followed IMMEDIATELY by the expiry
    code, which always starts with a digit. Sibling indices either fail the prefix
    (BANKNIFTY..., MIDCPNIFTY..., RELIANCE...) or put a LETTER right after it
    (NIFTYNXT50..., NIFTYFPI...). An entry with no symbol at all is NOT foreign: the feed
    is the only writer and it publishes one underlying, so "unknown" must never drop data.
    """
    for field in _SYMBOL_SOURCE_FIELDS:
        raw = item.get(field)
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
    """Normalize a live snapshot into ``{spot_price, expiry_date, data:[rows]}``.

    Rows are keyed by the COMPOSITE ``(expiry_date, strike_price)``. Keying them by the
    bare strike let a payload that carried two expiries collapse into one chain: the CE
    leg of expiry A and the PE leg of expiry B were written into the SAME row, and every
    aggregate built from it (COI flow, day GEX, PCR, gamma flip) then mixed two unrelated
    chains while ``process_payload`` priced the whole lot at a single DTE.

    Instruments that provably belong to an expiry other than ``payload["expiry_date"]``
    are dropped, because that DTE is what the greeks are computed with. Instruments whose
    expiry cannot be determined are kept and folded into the active bucket.
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
        # Underlying isolation: an equity or a sibling index that leaked into the payload
        # would otherwise be summed into the NIFTY gamma profile at its own strike level.
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

    # Sorted by (expiry, strike) so the strike series handed to _find_gamma_flip and the
    # spot estimator is always monotonic per expiry rather than dict-insertion ordered.
    option_rows = [row for _, row in sorted(rows.items(), key=lambda item: (item[0][0], item[0][1]))]
    return {
        "spot_price": _find_spot_price(payload) or _estimate_spot_from_rows(option_rows),
        "expiry_date": payload.get("expiry_date"),
        "data": option_rows,
        # OI/COI scale metadata, forwarded so process_payload can undo the producer's lot
        # multiplication with the lot size THAT producer used instead of a local guess.
        # Absent keys simply fall back to CONFIG["lot_size"] (see resolve_oi_divisor).
        "lot_size": payload.get("lot_size") or payload.get("lotSize"),
        "scale": payload.get("scale"),
        # Non-zero here means the producer published more than one expiry under a single
        # Redis key - the exact condition that used to corrupt the aggregates silently.
        "foreign_expiry_rows": sum(foreign_expiries.values()),
        "foreign_expiries": sorted(foreign_expiries),
        # Non-zero means an equity/sibling-index instrument reached the engine.
        "foreign_underlying_rows": foreign_underlyings,
    }


def _find_spot_price(payload: Any) -> float:
    spot_keys = {"spot_price", "underlying_spot", "underlying_ltp", "index_ltp", "nifty_ltp"}
    if isinstance(payload, dict):
        for key, value in payload.items():
            if key.lower() in spot_keys:
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
    parity_estimates = []
    active_strikes = []
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


class RedisGammaBot:
    """Owns the pub/sub thread and hands each Redis update directly to the strategy."""

    def __init__(self, redis_url: str = REDIS_URL, data_key: str = REDIS_DATA_KEY, channel: str = REDIS_CHANNEL) -> None:
        self.redis_url = redis_url
        self.data_key = data_key
        self.channel = channel
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()
        self._redis_client: Any = None
        self.processor = GammaProcessor(CONFIG["symbol"])
        self.signal_engine = SignalEngine()
        
        # Deques for 2-minute OHLC candles
        self.closes: Deque[float] = deque(maxlen=CONFIG["history_size"])
        self.highs: Deque[float] = deque(maxlen=CONFIG["history_size"])
        self.lows: Deque[float] = deque(maxlen=CONFIG["history_size"])
        self.flip_volumes: Deque[float] = deque(maxlen=CONFIG["history_size"])
        
        # 2-Minute Candle State Tracking
        self.candle_minutes = CONFIG["candle_minutes"]
        self.current_candle_start: Optional[datetime] = None
        self.current_candle_high: float = 0.0
        self.current_candle_low: float = float('inf')
        self.current_candle_close: float = 0.0
        self.current_candle_flip_vol: float = 0.0
        
        self.previous_spot = 0.0
        self.last_signal: Optional[Dict[str, Any]] = {
            "timestamp": datetime.now().isoformat(timespec="seconds"),
            "symbol": CONFIG["symbol"],
            "message": "🟢 Gamma Engine Active: Monitoring for volatility breakouts...",
            "heartbeat": True,
        }
        self.last_profile: Optional[Dict[str, Any]] = None
        self.gamma_alerts: Deque[Dict[str, Any]] = deque(maxlen=50)
        self.last_error: Optional[str] = None
        self.processed_updates = 0

    def start(self) -> bool:
        with self._lock:
            if self.is_running:
                return False
            if redis is None:
                raise GammaBotError("redis-py is not installed; install the 'redis' package in the active environment.")
            self._redis_client = redis.Redis.from_url(self.redis_url, decode_responses=True)
            try:
                self._redis_client.ping()
            except Exception as exc:
                raise GammaBotError(f"Redis is unavailable at {self.redis_url}: {exc}") from exc
            self._stop_event.clear()
            self._prime_from_redis()
            self._thread = threading.Thread(target=self._listen, name="gamma-redis-listener", daemon=True)
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
        return {
            "running": self.is_running,
            "redis_url": self.redis_url,
            "data_key": self.data_key,
            "channel": self.channel,
            "processed_updates": self.processed_updates,
            "last_signal": self.last_signal,
            "gamma_alerts": list(self.gamma_alerts),
            "last_profile": self.last_profile,
            "last_error": self.last_error,
            "active_trade": self.signal_engine.active_trade,
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

    def _listen(self) -> None:
        try:
            with self._redis_client.pubsub(ignore_subscribe_messages=True) as pubsub:
                pubsub.subscribe(self.channel)
                while not self._stop_event.is_set():
                    message = pubsub.get_message(timeout=1.0)
                    if message and message.get("type") == "message":
                        self._process_message(message.get("data"))
        except Exception as exc:
            self.last_error = str(exc)

    def _update_2m_candle(self, spot: float, flip_vol: float) -> None:
        """Groups live ticks into 2-minute OHLC candles to calculate accurate ATR."""
        now = datetime.now()
        # Truncate to the nearest 2-minute boundary (e.g., 10:00, 10:02, 10:04)
        current_bucket = now.replace(second=0, microsecond=0)
        current_bucket = current_bucket - timedelta(minutes=current_bucket.minute % self.candle_minutes)

        # If the 2-minute window has passed, save the completed candle and start a new one
        if self.current_candle_start is not None and current_bucket > self.current_candle_start:
            self.highs.append(self.current_candle_high)
            self.lows.append(self.current_candle_low)
            self.closes.append(self.current_candle_close)
            self.flip_volumes.append(self.current_candle_flip_vol)
            
            # Reset for the new candle
            self.current_candle_high = spot
            self.current_candle_low = spot
            self.current_candle_close = spot
            self.current_candle_flip_vol = flip_vol
        else:
            # Update the current forming candle
            self.current_candle_high = max(self.current_candle_high, spot)
            self.current_candle_low = min(self.current_candle_low, spot)
            self.current_candle_close = spot
            self.current_candle_flip_vol += flip_vol  # Accumulate volume within the 2m window
            
        self.current_candle_start = current_bucket

    def _process_message(self, message_data: Any) -> None:
        try:
            raw_payload = json.loads(message_data) if isinstance(message_data, str) and message_data.strip().startswith("{") else self._redis_client.get(self.data_key)
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

            # Calculate volume strictly near the Gamma Flip level (±100 pts)
            flip_band_vol = 100.0
            granular_flip_vol = 0.0
            if profile.gamma_flip_level is not None:
                granular_flip_vol = sum(
                    strike.call_vol + strike.put_vol
                    for strike in profile.strikes
                    if abs(strike.strike - profile.gamma_flip_level) <= flip_band_vol
                )
            # Group ticks into 2-minute OHLC candles for ATR
            self._update_2m_candle(profile.spot, granular_flip_vol)

            for message in self.signal_engine.evaluate_gamma_alerts(profile):
                self.gamma_alerts.append({
                    "timestamp": datetime.now().isoformat(timespec="seconds"),
                    "message": message,
                })
                
            # Only evaluate entries if we have enough 2-min candles for a valid ATR (14 candles = 28 mins)
            if self.previous_spot > 0 and len(self.closes) >= 15:
                signal = self.signal_engine.evaluate_entry(
                    profile, 
                    self.previous_spot, 
                    self.highs, 
                    self.lows, 
                    self.closes, 
                    self.flip_volumes 
                )
                if signal:
                    self.last_signal = asdict(signal)
                    
            atr = self.signal_engine._compute_true_atr(self.highs, self.lows, self.closes) if len(self.closes) >= 15 else 0.0
            self.signal_engine.manage_trade(profile.spot, atr)
            
            self.previous_spot = profile.spot
            self.last_profile = {
                "spot": profile.spot,
                "expiry": profile.expiry,
                "gamma_flip_level": profile.gamma_flip_level,
                "net_gex": profile.net_gex,
                # Compact, dashboard-ready copy of the same number ("-1.07 Cr").
                "net_gex_display": profile.net_gex_display,
                "pcr": profile.pcr,
                "skew": profile.skew,
                # Scale metadata: OI/COI arrived in shares and were divided by this lot
                # factor, so every exposure below is per contract.
                "lot_size": profile.contract_divisor,
                "oi_unit": profile.oi_unit,
                "exposure_units": CONFIG["exposure_units"],
                "weekly_gamma": asdict(profile.weekly_gamma),
                "day_gamma": asdict(profile.day_gamma),
            }
            self.processed_updates += 1
            self.last_error = None
        except Exception as exc:
            self.last_error = str(exc)


_default_bot: Optional[RedisGammaBot] = None
_default_bot_lock = threading.Lock()

def get_gamma_bot() -> RedisGammaBot:
    global _default_bot
    with _default_bot_lock:
        if _default_bot is None:
            _default_bot = RedisGammaBot()
        return _default_bot

def main() -> None:
    print("[START] Starting Gamma Blast Engine...")

    bot = RedisGammaBot()
    try:
        bot.start()
    except GammaBotError as e:
        print(f"[ERROR] Error: {e}")
        return

    print(f"[OK] Connected to Redis at {bot.redis_url}")
    print(f"[INFO] Listening on channel '{bot.channel}' (data key: {bot.data_key})")
    print(f"[INFO] Timeframe set to {CONFIG['candle_minutes']} minutes.")
    print(
        f"[INFO] OI/COI input scale: {CONFIG['oi_input_scale']} "
        f"(lot size {CONFIG['lot_size']}) -> exposures reported per contract in "
        f"{CONFIG['exposure_units']} units."
    )
    print("[INFO] Waiting for live option-chain updates. Press Ctrl+C to exit.")

    try:
        while True:
            if not bot.is_running:
                print("[WARN] Listener thread stopped unexpectedly - restarting...")
                try:
                    bot.start()
                except GammaBotError as e:
                    print(f"[ERROR] Error: {e}")
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n[STOP] KeyboardInterrupt received - shutting down gracefully...")
    except Exception as e:
        print(f"[ERROR] Error: {e}")
    finally:
        bot.stop()
        print("[STOP] Gamma Blast Engine stopped.")

if __name__ == "__main__":
    main()

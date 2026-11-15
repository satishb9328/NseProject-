"""
Redis-backed persistence for the live market-metrics state.

This module is deliberately dependency-free (stdlib only) so it can be imported by
both the standalone feed worker (``upstox_live_feed.py``) and the Django views
(``Live_dashboard/views.py``) without pulling in Django or redis.

Contract
--------
* One dynamic key per trading day: ``market_state:YYYY-MM-DD`` (IST date). Because
  the key is date-scoped, the state automatically resets on the next trading day.
* The value is a small JSON object carrying the four cumulative / peak-hold values
  the dashboard shows (``cumulative_cvd``, ``peak_bias``, ``peak_momentum``,
  ``peak_score``) plus the four advanced-quant diagnostics (``delta_wt_impact``,
  ``gamma_boost_val``, ``iv_skew_est``, ``acceleration``).
* Every write sets a 24h TTL so abandoned keys expire on their own and never pile
  up in Redis.
"""

import json
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

IST = timezone(timedelta(hours=5, minutes=30))

MARKET_STATE_KEY_PREFIX = "market_state"
MARKET_STATE_TTL_SECONDS = 24 * 60 * 60  # 24h

# Redis key + rolling cap for the last-N completed 2m candles (OHLC + CVD) that the
# dashboard rehydrates on page load so a browser refresh (F5) skips the ~30-minute
# ATR/divergence warm-up. Written by upstox_live_feed.py on each 2m candle close,
# read by views.py on the /live-signals/ page render.
NIFTY_2M_SEED_CANDLES_KEY = "nifty_2m_seed_candles"
NIFTY_2M_SEED_CANDLES_MAX = 20

# ``peak_score`` defaults to 50 (the neutral centre of the 0..100 score scale),
# while the other three defaults are 0.
DEFAULT_MARKET_STATE = {
    "cumulative_cvd": 0.0,
    "peak_bias": 0.0,
    "peak_momentum": 0.0,
    "peak_score": 50.0,
    # Authoritative current sentiment — the single source of truth the dashboard renders.
    "bias": 0.0,
    "momentum": 0.0,
    "score": 50.0,
    # Advanced-quant diagnostics (Step 2): raw engine math surfaced to the MATH
    # console so it refreshes at the 1 Hz option-chain cadence, not the 2m CVD one.
    "delta_wt_impact": 0.0,
    "gamma_boost_val": 0.0,
    "iv_skew_est": 0.0,
    "acceleration": 0.0,
    # Real Black-Scholes Net GEX + its bounded conviction ratio (net_gex / gross_gex).
    "net_gex": 0.0,
    "net_gex_ratio": 0.0,
    # Advanced Greek Blend inputs (Vanna / Charm / Gamma Flip level) that drive the
    # score's gamma boost. gamma_flip_level is 0.0 when no flip exists in the chain.
    "net_vanna": 0.0,
    "net_charm": 0.0,
    "gamma_flip_level": 0.0,
    # Real-time ATM time decay (₹/hr), reverse-solved from the live LTP via py_vollib.
    "theta_decay_per_hour": 0.0,
    # Whole-chain macro metrics for the left "TOTAL OPTION CHAIN (MACRO)" panel.
    # Current values plus their session High-Water Mark peaks (PCR centred on 1.0).
    "net_oi_bias": 0.0,
    "total_vol_skew": 0.0,
    "total_pcr": 1.0,
    "peak_net_oi_bias": 0.0,
    "peak_total_vol_skew": 0.0,
    "peak_total_pcr": 1.0,
    # AI Trade Setups: live 1-minute ATR of the ATM option candles (per side plus
    # the active-side value), used by the frontend to render the dynamic entry zone.
    "current_atr": 0.0,
    "current_atr_ce": 0.0,
    "current_atr_pe": 0.0,
    # Real volume-weighted VWAP (₹), fed by the NIFTY index future's average-traded-
    # price (Upstox `atp` = Σ(price·qty) / Σ(qty)) in the feed worker.
    "vwap": 0.0,
    # Live LTP of the NIFTY index future (FUTIDX). Pairs with `vwap` so the frontend
    # VWAP card compares future price vs future VWAP (apples-to-apples) instead of the
    # cash index against the futures VWAP.
    "future_ltp": 0.0,
    # VWAP BREAKDOWN SNIPER (PE · C2-A): scale-free order-flow / volume / 3-bar CVD
    # divergence telemetry, computed by the CVD engine on every completed 2m candle
    # and folded into market_state on the next publish beat.
    "ofi_imbalance": 0.0,
    "rel_vol_ratio": 1.0,
    "cvd_change_3": 0.0,
}


def market_state_key(now: Optional[datetime] = None) -> str:
    """Return ``market_state:YYYY-MM-DD`` for the current IST trading day."""
    moment = now or datetime.now(IST)
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=IST)
    local = moment.astimezone(IST)
    return f"{MARKET_STATE_KEY_PREFIX}:{local.date().isoformat()}"


def _coerce_number(value: Any, default: float) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return default
    # Treat NaN/Inf (which JSON must never contain, but be defensive) as the default.
    if parsed != parsed or parsed in (float("inf"), float("-inf")):
        return default
    return parsed


def normalize_market_state(data: Any) -> dict:
    """Return a complete, type-safe market-state dict from an arbitrary payload."""
    if not isinstance(data, dict):
        data = {}
    return {
        field: _coerce_number(data.get(field), default)
        for field, default in DEFAULT_MARKET_STATE.items()
    }


def load_market_state(client, now: Optional[datetime] = None) -> dict:
    """Read today's market state from Redis; return defaults when unavailable."""
    if client is None:
        return dict(DEFAULT_MARKET_STATE)
    try:
        raw = client.get(market_state_key(now))
    except Exception:
        return dict(DEFAULT_MARKET_STATE)
    if not raw:
        return dict(DEFAULT_MARKET_STATE)
    try:
        data = json.loads(raw)
    except (TypeError, ValueError):
        return dict(DEFAULT_MARKET_STATE)
    return normalize_market_state(data)


def save_market_state(client, state: dict, now: Optional[datetime] = None) -> bool:
    """Persist ``state`` under today's key with a 24h TTL. Returns success."""
    if client is None:
        return False
    try:
        client.set(
            market_state_key(now),
            json.dumps(normalize_market_state(state), default=str),
            ex=MARKET_STATE_TTL_SECONDS,
        )
        return True
    except Exception:
        return False

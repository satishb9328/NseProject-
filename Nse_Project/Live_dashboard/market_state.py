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

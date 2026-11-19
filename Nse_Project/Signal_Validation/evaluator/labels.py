"""Return and directional-label computation (offline, pure).

Adds spot/CE/PE return columns and per-signal direction columns, then classifies
directional success/failure/neutral against the cost buffer in evaluator.yaml.
"""

from __future__ import annotations

import numpy as np
import pandas as pd


def _col(df: pd.DataFrame, name: str) -> pd.Series:
    """Return ``df[name]`` or an all-NaN Series when the column is absent."""
    if name in df.columns:
        return df[name]
    return pd.Series(np.nan, index=df.index)


def build_returns(df: pd.DataFrame, horizons) -> pd.DataFrame:
    """Add per-horizon return columns: spot (points + bp), CE/PE (%)."""
    df = df.copy()
    spot = _col(df, "spot")
    base_ce = _col(df, "atm_ce_ltp")
    base_pe = _col(df, "atm_pe_ltp")
    for h in horizons:
        spot_h = _col(df, f"spot_{h}s")
        ce_h = _col(df, f"ce_{h}s")
        pe_h = _col(df, f"pe_{h}s")

        df[f"spot_move_{h}_pts"] = spot_h - spot
        with np.errstate(divide="ignore", invalid="ignore"):
            df[f"spot_ret_{h}_bp"] = 10000.0 * np.log(spot_h / spot)
            df[f"ce_ret_{h}_pct"] = 100.0 * (ce_h - base_ce) / base_ce
            df[f"pe_ret_{h}_pct"] = 100.0 * (pe_h - base_pe) / base_pe
    return df


def build_directions(df: pd.DataFrame, config: dict) -> pd.DataFrame:
    """Add per-signal direction columns: +1 bull, -1 bear, 0 = no call."""
    df = df.copy()
    score_cfg = config.get("signals", {}).get("score", {})
    bull = float(score_cfg.get("bull_threshold", 55.0))
    bear = float(score_cfg.get("bear_threshold", 45.0))

    df["dir_score"] = 0
    df.loc[df["score"] >= bull, "dir_score"] = 1
    df.loc[df["score"] <= bear, "dir_score"] = -1

    df["dir_bias"] = np.sign(df["bias"])
    df["dir_momentum"] = np.sign(df["momentum"])
    df["dir_gamma_boost"] = np.sign(df["gamma_boost"])
    df["dir_iv_skew"] = -np.sign(df["iv_skew"])
    return df


def classify(direction, returns, cost: float):
    """Classify each call as success/failure/neutral from a signed return.

    ``direction`` is +1/-1/0; ``returns`` is the raw outcome return in the SAME
    units as ``cost`` (points for spot, % for option). No call (direction 0) or
    missing return -> neutral (excluded from hit-rate).
    """
    direction = np.asarray(direction, dtype=float)
    returns = np.asarray(returns, dtype=float)
    directed = direction * returns
    out = np.full(directed.shape, "neutral", dtype=object)
    out[directed >= cost] = "success"
    out[directed <= -cost] = "failure"
    out[direction == 0] = "neutral"
    out[np.isnan(directed)] = "neutral"
    return out


def build(df: pd.DataFrame, config: dict) -> pd.DataFrame:
    """Add return + direction columns; returns a new DataFrame."""
    df = build_returns(df, config["horizons"])
    return build_directions(df, config)

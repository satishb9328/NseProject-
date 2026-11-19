"""Metric computation: hit_rate, expectancy, profit_factor, drawdown, Sharpe."""

from __future__ import annotations

import numpy as np
import pandas as pd

# (signal name, direction column, bucket labels for +1 / -1)
_SIGNALS = [
    ("score", "dir_score", ("bull", "bear")),
    ("bias", "dir_bias", ("positive", "negative")),
    ("momentum", "dir_momentum", ("positive", "negative")),
    ("gamma_boost", "dir_gamma_boost", ("positive", "negative")),
]


def _epoch_seconds(ts: pd.Series) -> pd.Series:
    """Datetime series -> epoch seconds (NaN for NaT)."""
    ts = pd.to_datetime(ts, errors="coerce")
    epoch = ts.astype("int64").astype(float)
    epoch[ts.isna()] = np.nan
    return epoch / 1e9


def _max_drawdown(values) -> float:
    """Peak-to-trough of a cumulative series, reported as a negative number."""
    values = np.asarray(values, dtype=float)
    values = values[~np.isnan(values)]
    if values.size == 0:
        return 0.0
    cum = np.cumsum(values)
    return float(-np.max(np.maximum.accumulate(cum) - cum))


def _cooldown_sample(ts, values, horizon_seconds: float) -> np.ndarray:
    """Sample trades non-overlapping (>= ``horizon_seconds`` apart), chronological."""
    ts = np.asarray(ts, dtype=float)
    values = np.asarray(values, dtype=float)
    mask = ~np.isnan(ts)
    ts, values = ts[mask], values[mask]
    if values.size == 0:
        return np.array([], dtype=float)
    order = np.argsort(ts)
    ts, values = ts[order], values[order]
    sampled = [values[0]]
    last = ts[0]
    for i in range(1, values.size):
        if ts[i] - last >= horizon_seconds:
            sampled.append(values[i])
            last = ts[i]
    return np.asarray(sampled, dtype=float)


def _compute_one(directed, ts, cost, unit, signal, bucket, horizon, outcome) -> dict:
    directed = pd.to_numeric(directed, errors="coerce")
    ts = pd.to_numeric(ts, errors="coerce")
    valid = directed.notna() & ts.notna()
    directed = directed[valid]
    ts = ts[valid]
    n = int(directed.size)
    if n == 0:
        return None

    arr = directed.to_numpy()
    succ = int((arr >= cost).sum())
    fail = int((arr <= -cost).sum())
    neutral = n - succ - fail

    hit_rate = succ / (succ + fail) if (succ + fail) > 0 else float("nan")
    expectancy = float(arr.mean())

    gains = float(arr[arr > 0].sum())
    losses = float(-arr[arr < 0].sum())
    if losses > 0:
        profit_factor = gains / losses
    elif gains > 0:
        profit_factor = float("inf")
    else:
        profit_factor = 0.0

    max_drawdown = _max_drawdown(arr)  # raw per-call (overlapping 1s ticks)
    sampled = _cooldown_sample(ts.to_numpy(), arr, float(horizon))
    max_drawdown_cooldown = _max_drawdown(sampled)  # non-overlapping equity curve

    std = float(arr.std(ddof=1))
    sharpe = float(expectancy / std) if (std and std == std and std != 0) else float("nan")

    return {
        "signal": signal,
        "bucket": bucket,
        "horizon": int(horizon),
        "outcome": outcome,
        "unit": unit,
        "cost": float(cost),
        "n_calls": n,
        "n_success": succ,
        "n_failure": fail,
        "n_neutral": neutral,
        "hit_rate": hit_rate,
        "expectancy": expectancy,
        "profit_factor": profit_factor,
        "max_drawdown": max_drawdown,
        "max_drawdown_cooldown": max_drawdown_cooldown,
        "n_cooldown_trades": int(sampled.size),
        "sharpe": sharpe,
        "_directed": arr,
    }


def _spot_baselines(df: pd.DataFrame, horizons, cost_spot: float) -> dict:
    """Return ``{horizon: always_long_hit_rate}`` on spot moves."""
    out = {}
    for h in horizons:
        ret = pd.to_numeric(df[f"spot_move_{h}_pts"], errors="coerce").dropna()
        if ret.empty:
            out[h] = float("nan")
            continue
        succ = int((ret >= cost_spot).sum())
        fail = int((ret <= -cost_spot).sum())
        trials = succ + fail
        out[h] = succ / trials if trials else float("nan")
    return out


def compute_metric_rows(df: pd.DataFrame, config: dict) -> list:
    """Compute one metric row per (signal, bucket, horizon, outcome).

    Spot rows carry ``edge_delta`` = hit_rate − always-long hit rate for the
    same dataset and horizon, so incremental edge over the naive long baseline
    is immediately visible.
    """
    horizons = config["horizons"]
    cost_spot = float(config["labels"]["cost"]["spot_points"])
    cost_opt = float(config["labels"]["cost"]["option_pct"])
    baseline = _spot_baselines(df, horizons, cost_spot)
    outcomes = [
        ("spot", "spot_move_{h}_pts", cost_spot, "points"),
        ("ce", "ce_ret_{h}_pct", cost_opt, "%"),
        ("pe", "pe_ret_{h}_pct", cost_opt, "%"),
    ]
    ts = _epoch_seconds(df["_ts"] if "_ts" in df.columns else df["ts"])

    rows = []
    for signal, dir_col, buckets in _SIGNALS:
        for bucket in buckets:
            sign = 1 if bucket in ("bull", "positive") else -1
            mask = df[dir_col] == sign
            sub = df[mask]
            sub_ts = ts[mask]
            for h in horizons:
                for outcome, ret_tpl, cost, unit in outcomes:
                    ret_col = ret_tpl.format(h=h)
                    if ret_col not in df.columns:
                        continue
                    directed = sub[dir_col] * sub[ret_col]
                    row = _compute_one(directed, sub_ts, cost, unit, signal, bucket, h, outcome)
                    if row is not None:
                        if outcome == "spot":
                            row["edge_delta"] = row["hit_rate"] - baseline.get(h, float("nan"))
                        else:
                            row["edge_delta"] = None
                        rows.append(row)
    return rows


def compute_benchmarks(df: pd.DataFrame, config: dict) -> dict:
    """Naive baselines: always-long / always-short / hold-spot on spot moves."""
    horizons = config["horizons"]
    cost_spot = float(config["labels"]["cost"]["spot_points"])
    baseline = _spot_baselines(df, horizons, cost_spot)
    out = {}
    for h in horizons:
        ret = pd.to_numeric(df[f"spot_move_{h}_pts"], errors="coerce").dropna()
        if ret.empty:
            continue
        al = baseline.get(h, float("nan"))
        out[str(h)] = {
            "n": int(ret.size),
            "always_long_hit_rate": al,
            "always_short_hit_rate": (1.0 - al) if al == al else float("nan"),
            "hold_spot_mean_pts": float(ret.mean()),
        }
    return out


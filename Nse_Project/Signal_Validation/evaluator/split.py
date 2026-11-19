"""Chronological train/validation/test split and walk-forward windowing."""

from __future__ import annotations

from typing import Iterator, Tuple

import pandas as pd


def _chronological(df: pd.DataFrame) -> pd.DataFrame:
    if "_ts" not in df.columns:
        df = df.copy()
        df["_ts"] = pd.to_datetime(df["ts"], errors="coerce")
    return df.sort_values("_ts").reset_index(drop=True)


def _drop_boundary(df: pd.DataFrame, gap_seconds: float) -> pd.DataFrame:
    """Drop trailing rows whose forward horizon would cross a split boundary."""
    if gap_seconds <= 0 or df.empty:
        return df
    ts = pd.to_datetime(df["ts"], errors="coerce")
    latest = ts.max()
    if pd.isna(latest):
        return df
    cutoff = latest - pd.Timedelta(seconds=gap_seconds)
    return df[ts <= cutoff].reset_index(drop=True)


def train_validation_test_split(
    df: pd.DataFrame, config: dict
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Chronological, contiguous train/validation/test split.

    Rows within ``split.gap_seconds`` of a boundary are dropped so a forward
    horizon never crosses into the next period (no look-ahead leakage).
    """
    split = config.get("split", {})
    train_r = float(split.get("train_ratio", 0.70))
    val_r = float(split.get("validation_ratio", 0.15))
    gap = float(split.get("gap_seconds", 120.0))

    df = _chronological(df)
    n = len(df)
    if n == 0:
        return df.iloc[:0], df.iloc[:0], df.iloc[:0]

    train_n = int(n * train_r)
    val_n = int(n * val_r)

    train = _drop_boundary(df.iloc[:train_n], gap)
    val = _drop_boundary(df.iloc[train_n:train_n + val_n], gap)
    test = df.iloc[train_n + val_n:]
    return train, val, test


def walk_forward_windows(
    df: pd.DataFrame, config: dict, n_windows: int = 5
) -> Iterator[Tuple[pd.DataFrame, pd.DataFrame]]:
    """Expanding-window walk-forward: train grows, test is the next chunk."""
    split = config.get("split", {})
    gap = float(split.get("gap_seconds", 120.0))

    df = _chronological(df)
    n = len(df)
    if n == 0:
        return

    test_size = max(int(n / (n_windows + 1)), 1)
    start = max(int(n * 0.5), test_size)  # require >=50% data before first test
    while start < n:
        train = _drop_boundary(df.iloc[:start], gap)
        test = df.iloc[start:start + test_size]
        if test.empty:
            break
        yield train, test
        start += test_size

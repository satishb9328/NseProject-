"""JSONL loading, schema validation, quality filtering, raw self-join recovery."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Iterable, Optional

import numpy as np
import pandas as pd

from . import parse_timestamp, to_float

_SIGNAL_COLS = ["bias", "momentum", "gamma_boost", "score", "iv_skew"]
_CORE_COLS = ["spot", "atm_strike", "atm_ce_ltp", "atm_pe_ltp", "seconds_since_last_tick"]
_BOOL_COLS = ["feed_connected", "stale"]


class Loader:
    """Loads enriched JSONL, validates schema, filters quality, recovers gaps."""

    def __init__(self, config: dict, lookup_tolerance: float = 2.0) -> None:
        paths = config.get("paths", {})
        self.raw_dir = Path(paths.get("raw_dir"))
        self.processed_dir = Path(paths.get("processed_dir"))
        self.horizons = config.get("horizons", (15, 30, 60, 120))
        self.lookup_tolerance = float(lookup_tolerance)

    # -- discovery -----------------------------------------------------------
    def discover_dates(self) -> list:
        dates = set()
        for directory in (self.processed_dir, self.raw_dir):
            if directory.exists():
                for path in directory.glob("signal_*.jsonl"):
                    name = path.name.replace(".enriched", "")
                    stem = name[len("signal_"):-len(".jsonl")]
                    if len(stem) == 10:
                        dates.add(stem)
        return sorted(dates)

    # -- loading -------------------------------------------------------------
    def load(self, dates: Iterable[str]) -> pd.DataFrame:
        frames = []
        for date in dates:
            df = self._load_enriched(date)
            if df is None or df.empty:
                continue
            df = self._coerce(df)
            df = self._filter_quality(df)
            df = self._recover_missing(df, date)
            frames.append(df)
        if not frames:
            return pd.DataFrame()
        return pd.concat(frames, ignore_index=True)

    def _load_enriched(self, date: str) -> Optional[pd.DataFrame]:
        path = self.processed_dir / f"signal_{date}.enriched.jsonl"
        if not path.exists():
            path = self.processed_dir / f"signal_{date}.jsonl"
        if not path.exists():
            return None
        rows = self._read_jsonl(path)
        if not rows:
            return None
        return pd.DataFrame(rows)

    @staticmethod
    def _read_jsonl(path: Path) -> list:
        rows = []
        with open(path, "r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if isinstance(obj, dict):
                    rows.append(obj)
        return rows

    # -- validation / coercion ----------------------------------------------
    def _coerce(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        for col in _CORE_COLS + _SIGNAL_COLS:
            if col in df:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        for h in self.horizons:
            for col in (f"spot_{h}s", f"ce_{h}s", f"pe_{h}s"):
                if col in df:
                    df[col] = pd.to_numeric(df[col], errors="coerce")
        for col in _BOOL_COLS:
            if col in df:
                df[col] = df[col].astype(str).str.lower().isin(["true", "1", "yes", "on"])
        if "ts" in df:
            df["_ts"] = pd.to_datetime(df["ts"], errors="coerce")
        return df

    def _filter_quality(self, df: pd.DataFrame) -> pd.DataFrame:
        mask = pd.Series(True, index=df.index)
        if "stale" in df:
            mask &= ~df["stale"].fillna(False).astype(bool)
        if "feed_connected" in df:
            mask &= df["feed_connected"].fillna(True).astype(bool)
        if "data_source" in df:
            mask &= ~df["data_source"].eq("cold_json").fillna(False)
        return df.loc[mask].reset_index(drop=True)


    # -- raw self-join recovery ---------------------------------------------
    def _recover_missing(self, df: pd.DataFrame, date: str) -> pd.DataFrame:
        """Fill null forward values from the raw capture file (post-restart fallback).

        Spot is always recoverable; pinned CE/PE is recoverable only when a
        future raw sample still quotes the same ``atm_strike`` and expiry.
        """
        raw_path = self.raw_dir / f"signal_{date}.jsonl"
        if not raw_path.exists() or df.empty:
            return df
        raw_rows = self._read_jsonl(raw_path)
        if not raw_rows:
            return df

        entries = []
        for r in raw_rows:
            epoch = self._epoch(r.get("ts"))
            if epoch is not None:
                entries.append((epoch, r))
        entries.sort(key=lambda e: e[0])
        if not entries:
            return df
        epochs = np.array([e[0] for e in entries], dtype=float)
        tol = self.lookup_tolerance

        def nearest(target, strike=None, expiry=None):
            pos = int(np.searchsorted(epochs, target))
            best = None
            for idx in (pos - 1, pos, pos + 1):
                if 0 <= idx < len(epochs):
                    dist = abs(epochs[idx] - target)
                    if dist <= tol and (best is None or dist < best[0]):
                        row = entries[idx][1]
                        if strike is not None and to_float(row.get("atm_strike")) != strike:
                            continue
                        if expiry is not None and row.get("active_expiry") not in (None, expiry):
                            continue
                        best = (dist, row)
            return best[1] if best else None

        for h in self.horizons:
            spot_col = f"spot_{h}s"
            if spot_col in df.columns:
                for idx in df.index[df[spot_col].isna()]:
                    t = self._epoch(df.at[idx, "ts"])
                    if t is None:
                        continue
                    r = nearest(t + h)
                    if r is not None and to_float(r.get("spot")) is not None:
                        df.at[idx, spot_col] = float(r["spot"])
            for out, src in (("ce", "atm_ce_ltp"), ("pe", "atm_pe_ltp")):
                col = f"{out}_{h}s"
                if col not in df.columns:
                    continue
                for idx in df.index[df[col].isna()]:
                    t = self._epoch(df.at[idx, "ts"])
                    if t is None:
                        continue
                    strike = to_float(df.at[idx, "atm_strike"])
                    expiry = df.at[idx, "active_expiry"]
                    r = nearest(t + h, strike=strike, expiry=expiry)
                    if r is not None and to_float(r.get(src)) is not None:
                        df.at[idx, col] = float(r[src])
        return df

    @staticmethod
    def _epoch(ts) -> Optional[float]:
        dt = parse_timestamp(ts)
        return dt.timestamp() if dt is not None else None


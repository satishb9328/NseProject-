"""
MTF DATA ENGINE — feeds candlestick_scanner.py
==============================================

Loads PRE-CALCULATED Daily + Weekly parquet files (no resampling) and builds the
exact 12-column DataFrame the scanner expects, with NO look-ahead bias.

Daily path  : D:/Api data/ML_HISTO_DATA/Upstox Data/Day Data   (NSE_EQ_<ISIN>.parquet)
Weekly path : D:/Api data/ML_HISTO_DATA/Upstox Data/Weeks Data (NSE_EQ_<ISIN>.parquet)

Output columns (exact contract with candlestick_scanner.py):

    Open, High, Low, Close,
    Daily_EMA_50, Daily_EMA_200, Daily_RSI,
    Weekly_Open, Weekly_High, Weekly_Low, Weekly_Close, Weekly_EMA_50

No-look-ahead guarantee
-----------------------
Weekly rows are timestamped at the week START (Monday) but only become "visible"
once the week ENDS (Friday). They are shifted to Friday and joined with a
backward as-of merge, so e.g. on a Wednesday only the previous Friday's completed
weekly candle is seen — never the in-progress week's Friday close.

Usage
-----
    from data_engine import build_mtf_dataframe
    from candlestick_scanner import scan_mtf_patterns

    df      = build_mtf_dataframe("INE002A01018")   # ISIN or "NSE_EQ_<ISIN>" stem
    signals = scan_mtf_patterns(df)
"""

from __future__ import annotations

from pathlib import Path
import numpy as np
import pandas as pd

__all__ = ["build_mtf_dataframe", "DAILY_DATA_DIR", "WEEKLY_DATA_DIR"]

# ────────────────────────────────────────────────────────────────────────────────
# PATHS
# ────────────────────────────────────────────────────────────────────────────────

DAILY_DATA_DIR = Path(r"D:\Api data\ML_HISTO_DATA\Upstox Data\Day Data")
WEEKLY_DATA_DIR = Path(r"D:\Api data\ML_HISTO_DATA\Upstox Data\Weeks Data")

# ────────────────────────────────────────────────────────────────────────────────
# CONFIG
# ────────────────────────────────────────────────────────────────────────────────

DAILY_EMA_FAST = 50     # Daily_EMA_50
DAILY_EMA_SLOW = 200    # Daily_EMA_200
RSI_PERIOD = 14         # Daily_RSI (Wilder's)
WEEKLY_EMA = 50         # Weekly_EMA_50

# Exact output contract expected by candlestick_scanner.py (order kept explicit).
OUTPUT_COLUMNS = [
    "Open",
    "High",
    "Low",
    "Close",
    "Daily_EMA_50",
    "Daily_EMA_200",
    "Daily_RSI",
    "Weekly_Open",
    "Weekly_High",
    "Weekly_Low",
    "Weekly_Close",
    "Weekly_EMA_50",
]

_DATE_COLUMN_NAMES = ("timestamp", "datetime", "date", "time", "ts")
_OHLC_ALIASES = {"open": "Open", "high": "High", "low": "Low", "close": "Close"}


# ────────────────────────────────────────────────────────────────────────────────
# INDICATORS (fully vectorized)
# ────────────────────────────────────────────────────────────────────────────────

def _ema(series: pd.Series, span: int) -> pd.Series:
    """Exponential moving average; NaN until ``span`` bars exist (warm-up)."""
    return series.ewm(span=span, adjust=False, min_periods=span).mean()

def _rsi(close: pd.Series, period: int = RSI_PERIOD) -> pd.Series:
    """Wilder's RSI. NaN during the ``period``-bar warm-up (then dropped)."""
    delta = close.diff()
    gain = delta.clip(lower=0.0)
    loss = -delta.clip(upper=0.0)

    avg_gain = gain.ewm(alpha=1.0 / period, min_periods=period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1.0 / period, min_periods=period, adjust=False).mean()

    rs = avg_gain / avg_loss.replace(0.0, np.nan)
    rsi = 100.0 - (100.0 / (1.0 + rs))

    # Fix the divide-by-zero edges: no losses => 100, flat (no gain & no loss) => 50.
    rsi = rsi.where(avg_loss != 0.0, 100.0)
    rsi = rsi.where((avg_loss != 0.0) | (avg_gain != 0.0), 50.0)
    return rsi


# ────────────────────────────────────────────────────────────────────────────────
# LOADING / NORMALISATION
# ────────────────────────────────────────────────────────────────────────────────

def _resolve_file(directory: Path, symbol: str, isin: str | None = None) -> Path:
    """Locate a symbol's parquet file in ``directory``.

    Upstox stores files as ``NSE_EQ_<ISIN>.parquet``. We accept the ISIN, the full
    file stem (``NSE_EQ_<ISIN>``) or any exact ``<symbol>.parquet`` match.
    """
    directory = Path(directory)
    if not directory.is_dir():
        raise FileNotFoundError(f"Data directory not found: {directory}")

    symbol = str(symbol).strip()
    candidates: list[str] = []

    if isin:
        candidates += [f"NSE_EQ_{isin}.parquet", f"{isin}.parquet"]

    candidates.append(f"{symbol}.parquet")
    if not symbol.upper().startswith("NSE_EQ_"):
        candidates += [f"NSE_EQ_{symbol}.parquet", f"NSE_EQ_{symbol.upper()}.parquet"]

    seen: set[str] = set()
    for name in candidates:
        if name in seen:
            continue
        seen.add(name)
        candidate = directory / name
        if candidate.is_file():
            return candidate

    # Case-insensitive fallback scan.
    targets = {symbol.upper(), f"NSE_EQ_{symbol.upper()}"}
    for candidate in sorted(directory.glob("*.parquet")):
        if candidate.stem.upper() in targets:
            return candidate

    raise FileNotFoundError(f"No parquet file for symbol={symbol!r} in {directory}")


def _find_date_column(df: pd.DataFrame) -> str:
    """Return the timestamp column, preferring known names, else a datetime col."""
    for col in df.columns:
        if str(col).strip().lower() in _DATE_COLUMN_NAMES:
            return col
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            return col
    return df.columns[0]


def _canonical_ohlc_rename(columns) -> dict:
    """Map lowercase / mixed-case OHLC names to ``Open/High/Low/Close``."""
    mapping: dict = {}
    for col in columns:
        key = str(col).strip().lower()
        if key in _OHLC_ALIASES and str(col) != _OHLC_ALIASES[key]:
            mapping[col] = _OHLC_ALIASES[key]
    return mapping


def _load_frame(path: Path) -> pd.DataFrame:
    """Read a parquet file into a clean OHLC frame with a datetime index.

    Handles both a datetime index and a ``Timestamp / Date / ...`` column, drops
    unparseable dates, deduplicates and sorts chronologically.
    """
    df = pd.read_parquet(path)
    if df is None or df.empty:
        raise ValueError(f"Empty parquet file: {path}")

    if pd.api.types.is_datetime64_any_dtype(df.index):
        dt_index = pd.DatetimeIndex(df.index)
        df = df.reset_index(drop=True)
    else:
        date_col = _find_date_column(df)
        dt_index = pd.DatetimeIndex(pd.to_datetime(df[date_col], errors="coerce"))
        df = df.drop(columns=[date_col])

    valid = ~dt_index.isna()
    df = df[valid]
    dt_index = dt_index[valid]

    df = df.rename(columns=_canonical_ohlc_rename(df.columns))

    missing = [c for c in ("Open", "High", "Low", "Close") if c not in df.columns]
    if missing:
        raise ValueError(
            f"{path.name}: missing OHLC columns {missing} (have {list(df.columns)})"
        )

    for col in ("Open", "High", "Low", "Close"):
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df[["Open", "High", "Low", "Close"]]
    df.index = dt_index
    df = df[~df.index.duplicated(keep="last")].sort_index()
    return df


# ────────────────────────────────────────────────────────────────────────────────
# MAIN ENTRY POINT
# ────────────────────────────────────────────────────────────────────────────────

def build_mtf_dataframe(
    symbol: str,
    daily_dir: Path | str = DAILY_DATA_DIR,
    weekly_dir: Path | str = WEEKLY_DATA_DIR,
    isin: str | None = None,
) -> pd.DataFrame:
    """Build the 12-column MTF DataFrame for ``symbol``.

    Steps (all vectorized):

    1. Load the pre-calculated Daily + Weekly parquet files (NO resampling).
    2. Daily  : ``Daily_EMA_50``, ``Daily_EMA_200``, ``Daily_RSI`` (14).
    3. Weekly : ``Weekly_EMA_50`` + rename OHLC to ``Weekly_*``.
    4. Align weekly onto daily with a backward as-of join. Weekly bars are
       stamped at the week START (Monday), so they are shifted to the week END
       (Friday) first — a Wednesday only ever sees the previous Friday's
       completed weekly candle (no look-ahead bias).
    5. Drop warm-up NaNs (EMA/RSI lookback) and return exactly ``OUTPUT_COLUMNS``
       on a datetime index.
    """
    day_path = _resolve_file(Path(daily_dir), symbol, isin)
    week_path = _resolve_file(Path(weekly_dir), symbol, isin)

    daily = _load_frame(day_path)
    weekly = _load_frame(week_path)

    # ── Daily indicators ────────────────────────────────────────────────────────
    dclose = daily["Close"]
    daily = daily.assign(
        Daily_EMA_50=_ema(dclose, DAILY_EMA_FAST),
        Daily_EMA_200=_ema(dclose, DAILY_EMA_SLOW),
        Daily_RSI=_rsi(dclose, RSI_PERIOD),
    )

    # ── Weekly indicator + weekly OHLC names ────────────────────────────────────
    wclose = weekly["Close"]
    weekly = weekly.assign(Weekly_EMA_50=_ema(wclose, WEEKLY_EMA))
    weekly = weekly.rename(
        columns={
            "Open": "Weekly_Open",
            "High": "Weekly_High",
            "Low": "Weekly_Low",
            "Close": "Weekly_Close",
        }
    )

    # ── No-look-ahead alignment ─────────────────────────────────────────────────
    # Weekly rows are stamped at the week START (Monday). Shift them to the week
    # END (Friday) so the as-of join never reveals the in-progress week's close.
    weekly = weekly.copy()
    weekly["Date"] = weekly.index + pd.to_timedelta(
        4 - weekly.index.weekday, unit="D"
    )
    daily = daily.copy()
    daily["Date"] = daily.index

    daily = daily.sort_values("Date")
    weekly = weekly.sort_values("Date")

    weekly_cols = [
        "Date",
        "Weekly_Open",
        "Weekly_High",
        "Weekly_Low",
        "Weekly_Close",
        "Weekly_EMA_50",
    ]

    merged = pd.merge_asof(
        daily,
        weekly[weekly_cols],
        on="Date",
        direction="backward",
    )

    merged.index = daily.index
    merged.index.name = "Date"

    # ── Strict output contract + drop EMA/RSI warm-up NaNs ─────────────────────
    result = merged[OUTPUT_COLUMNS].dropna()
    return result


# ────────────────────────────────────────────────────────────────────────────────
# SELF-TEST (run:  python data_engine.py [SYMBOL_OR_ISIN])
# ────────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys

    symbol = sys.argv[1] if len(sys.argv) > 1 else "INE002A01018"

    try:
        df = build_mtf_dataframe(symbol)
    except Exception as exc:  # noqa: BLE001 - surface any failure clearly
        print(f"ERROR: {exc}")
        raise SystemExit(1)

    print(f"Symbol      : {symbol}")
    print(f"Rows        : {len(df)}")
    print(f"Index range : {df.index.min()} .. {df.index.max()}")
    print(f"Columns     : {list(df.columns)}")
    print(f"Strict cols : {list(df.columns) == OUTPUT_COLUMNS}")
    print(f"Any NaN     : {bool(df.isna().any().any())}")
    print(f"Sorted      : {bool(df.index.is_monotonic_increasing)}")
    print("\nHead:")
    print(df.head(3).to_string())
    print("\nTail:")
    print(df.tail(3).to_string())

    # Optional end-to-end check against the scanner.
    try:
        from candlestick_scanner import scan_mtf_patterns

        signals = scan_mtf_patterns(df)
        print("\nScanner signal counts:")
        print(signals.sum().to_string())
    except ImportError:
        print("\n(candlestick_scanner not importable — skipping scanner check)")

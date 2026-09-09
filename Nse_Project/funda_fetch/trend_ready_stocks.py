import os
import pandas as pd

# ──────────────────────────────────────────────────────────────────────────────
# CONFIGURATION
# ──────────────────────────────────────────────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

CANDIDATES_FILE = os.path.join(BASE_DIR, "nifty500_swing_candidates.csv")
UNIVERSE_FILE = os.path.join(BASE_DIR, "ind_nifty500list.csv")
OUTPUT_FILE = os.path.join(BASE_DIR, "breakout_ready_targets.csv")

DAY_DIR = r"D:\Api data\ML_HISTO_DATA\Upstox Data\Day Data"
WEEK_DIR = r"D:\Api data\ML_HISTO_DATA\Upstox Data\Weeks Data"

# Columns required to run the resistance-proximity calculation.
REQUIRED_COLUMNS = {"close", "high", "volume"}


def _normalize_columns(df):
    """Lowercase/strip column names and map common OHLCV aliases to canonical
    lowercase names. Handles CSV/Parquet files with unexpected headers."""
    df = df.copy()
    df.columns = [str(c).strip().split("(")[0].strip().lower() for c in df.columns]

    aliases = {
        "o": "open", "open price": "open",
        "h": "high", "high price": "high",
        "l": "low", "low price": "low",
        "c": "close", "close price": "close", "last": "close",
        "ltp": "close", "adj close": "close",
        "v": "volume", "vol": "volume", "qty": "volume",
        "volume traded": "volume", "traded volume": "volume",
    }
    return df.rename(columns=aliases)


def _read_ohlcv(path):
    """Read a CSV or Parquet OHLCV file into a normalized DataFrame.

    Returns a DataFrame with at least ``close``/``high``/``volume`` columns,
    sorted chronologically when a timestamp column exists, or ``None`` if the
    file is missing, unreadable, empty, or lacks the required columns.
    """
    if not path or not os.path.exists(path):
        return None

    try:
        if str(path).lower().endswith(".parquet"):
            df = pd.read_parquet(path)
        else:
            df = pd.read_csv(path)
    except Exception as exc:
        print(f"  ⚠️  Could not read {os.path.basename(path)}: {exc}")
        return None

    if df is None or df.empty:
        return None

    df = _normalize_columns(df)

    # Ensure chronological order (important for rolling calculations).
    date_col = next(
        (c for c in df.columns if c in ("timestamp", "date", "datetime", "time")),
        None,
    )
    if date_col is not None:
        df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
        df = df.dropna(subset=[date_col]).sort_values(by=date_col)

    missing = REQUIRED_COLUMNS - set(df.columns)
    if missing:
        print(
            f"  ⚠️  {os.path.basename(path)} is missing required column(s): "
            f"{sorted(missing)}. Available: {sorted(df.columns)}"
        )
        return None

    for col in REQUIRED_COLUMNS:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.dropna(subset=["close", "volume"])
    return df if len(df) else None


def _find_symbol_file(folder, symbol, isin=None):
    """Locate a symbol's data file in ``folder``.

    Upstox data is stored as ``NSE_EQ_<ISIN>.parquet``; we also accept ISIN- or
    symbol-named files in either Parquet or CSV form.
    """
    candidates = []
    if isin:
        candidates.extend([
            f"NSE_EQ_{isin}.parquet",
            f"NSE_EQ_{isin}.csv",
            f"{isin}.parquet",
            f"{isin}.csv",
        ])
    candidates.extend([
        f"{symbol}.parquet",
        f"{symbol}.csv",
    ])

    for name in candidates:
        path = os.path.join(folder, name)
        if os.path.exists(path):
            return path
    return None


def _load_isin_map():
    """Build a {symbol: isin} mapping from the Nifty-500 universe CSV if present."""
    if not os.path.exists(UNIVERSE_FILE):
        return {}

    try:
        df = pd.read_csv(UNIVERSE_FILE)
        df.columns = [str(c).strip() for c in df.columns]

        sym_col = next((c for c in df.columns if c.lower() == "symbol"), None)
        isin_col = next((c for c in df.columns if "isin" in c.lower()), None)
        if sym_col is None or isin_col is None:
            return {}

        mapping = {}
        for sym, isin in zip(df[sym_col], df[isin_col]):
            key = str(sym).strip().upper()
            if key:
                mapping[key] = str(isin).strip()
        return mapping
    except Exception as exc:
        print(f"  ⚠️  Could not load ISIN mapping from {UNIVERSE_FILE}: {exc}")
        return {}


def calculate_resistance_proximity(df, window, price_col='close', high_col='high',
                                   volume_col='volume'):
    """Calculates the recent resistance and how far the current price is from it.

    Returns a ``(resistance, last_close, distance_pct, volume_ratio)`` tuple, or
    ``None`` when the frame is empty, too short, or lacks valid price/volume data.
    """
    if df is None or df.empty:
        return None

    for col in (price_col, high_col, volume_col):
        if col not in df.columns:
            print(f"  ⚠️  Missing column '{col}'. Available: {list(df.columns)}")
            return None

    n = len(df)
    if window > n:
        window = n  # Fallback if history is short
    if window <= 0:
        return None

    # Highest high over the rolling window.
    resistance = df[high_col].rolling(window=window).max().iloc[-1]
    last_close = df[price_col].iloc[-1]
    last_volume = df[volume_col].iloc[-1]
    avg_volume = df[volume_col].rolling(window=min(20, n)).mean().iloc[-1]

    if pd.isna(last_close) or last_close <= 0:
        return None
    if pd.isna(resistance) or resistance <= 0:
        return None

    # Percentage distance to resistance.
    distance_pct = ((resistance - last_close) / last_close) * 100

    if pd.isna(last_volume) or pd.isna(avg_volume) or avg_volume <= 0:
        volume_ratio = float('nan')
    else:
        volume_ratio = last_volume / avg_volume

    return resistance, last_close, distance_pct, volume_ratio

def find_trend_ready_stocks():
    # 1. Load your fundamental shortlist.
    if not os.path.exists(CANDIDATES_FILE):
        print(f"Error: Could not find {CANDIDATES_FILE}")
        return

    candidates_df = pd.read_csv(CANDIDATES_FILE)
    candidates_df.columns = [str(c).strip() for c in candidates_df.columns]

    # Assuming your CSV has a column named 'Symbol' or 'symbol'.
    symbol_col = next((c for c in candidates_df.columns if c.lower() == "symbol"), None)
    if symbol_col is None:
        symbol_col = candidates_df.columns[0]
    symbols = candidates_df[symbol_col].astype(str).str.strip().str.upper().unique()

    # 2. Build symbol -> ISIN mapping (Upstox files use ISIN-based names).
    isin_map = _load_isin_map()

    results = []
    missing_files = 0

    for symbol in symbols:
        isin = isin_map.get(symbol)
        day_file = _find_symbol_file(DAY_DIR, symbol, isin)
        week_file = _find_symbol_file(WEEK_DIR, symbol, isin)

        if not day_file or not week_file:
            missing_files += 1
            continue  # Skip if historical data is missing

        try:
            day_df = _read_ohlcv(day_file)
            week_df = _read_ohlcv(week_file)

            if day_df is None or week_df is None:
                missing_files += 1
                continue

            # Calculate 20-Day Micro Resistance.
            day_prox = calculate_resistance_proximity(day_df, window=20)

            # Calculate 26-Week Macro Resistance (~6 months).
            week_prox = calculate_resistance_proximity(week_df, window=26)

            if day_prox is None or week_prox is None:
                print(f"  ⚠️  Skipping {symbol}: insufficient or invalid OHLCV data")
                continue

            day_res, close_price, day_dist, vol_ratio = day_prox
            week_res, _, week_dist, _ = week_prox

            results.append({
                'Symbol': symbol,
                'Close': close_price,
                'Daily_Res_20d': day_res,
                'Dist_to_Daily_Res_%': round(day_dist, 2),
                'Weekly_Res_26w': week_res,
                'Dist_to_Weekly_Res_%': round(week_dist, 2),
                'Vol_vs_Avg': round(vol_ratio, 2) if pd.notna(vol_ratio) else None,
            })
        except Exception as e:
            print(f"  ⚠️  Failed processing {symbol}: {e}")

    # 3. Build the results DataFrame and exit gracefully if nothing processed.
    results_df = pd.DataFrame(results)

    if results_df.empty:
        print("\n⚠️  No data processed. Check if CSV files exist in the specified day/week paths and match the symbol names.")
        print(f"    Day data dir : {DAY_DIR}")
        print(f"    Week data dir: {WEEK_DIR}")
        print("    Expected naming: NSE_EQ_<ISIN>.parquet, <ISIN>.parquet, <symbol>.parquet, or <symbol>.csv")
        if missing_files:
            print(f"    ({missing_files} symbol(s) had no matching data file.)")
        return

    # 4. Filter and Rank the Results.
    # THE STRATEGY: Filter for stocks coiled within 4% of daily and 5% of weekly resistance.
    coiled_springs = results_df[
        (results_df['Dist_to_Daily_Res_%'] > 0) & (results_df['Dist_to_Daily_Res_%'] <= 4.0) &
        (results_df['Dist_to_Weekly_Res_%'] > 0) & (results_df['Dist_to_Weekly_Res_%'] <= 5.0)
    ].copy()

    # Sort by the tightest daily compression.
    coiled_springs.sort_values(by='Dist_to_Daily_Res_%', ascending=True, inplace=True)

    print("\n🚀 TOP 'READY TO TREND' SWING CANDIDATES (COILED SPRINGS) 🚀")

    if coiled_springs.empty:
        print("No setups matched the coiled-spring resistance filters.")
    else:
        print(coiled_springs.to_string(index=False))

    coiled_springs.to_csv(OUTPUT_FILE, index=False)
    print(f"\nSaved actionable targets to {OUTPUT_FILE}")


if __name__ == "__main__":
    find_trend_ready_stocks()
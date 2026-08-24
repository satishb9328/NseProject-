"""
Institutional Spoofing Detector (Bull/Bear Trap) backtester.

Reads every tick-by-tick pre-open CSV/TXT file from the Fari Preopen folder,
computes daily Max/Min/Final IEP, flags spoofing traps where large intraday
swings reverse before the 9:07 AM settlement print, filters those traps
through a mock Gamma Wall confluence check (previous close +/- 100 points),
and simulates a strict 09:15 -> 10:30 time-based exit using yfinance
15-minute intraday bars.
"""
import logging
from datetime import datetime, time, timedelta
from pathlib import Path

import pandas as pd
import yfinance as yf

SOURCE_DIR = Path(r"C:\Users\ADMIN\Desktop\Fari Preopen")
FILE_EXTENSIONS = (".csv", ".txt")
ENCODINGS_TO_TRY = ("utf-8-sig", "utf-8", "latin1")
TRAP_THRESHOLD = 50

NIFTY_TICKER = "^NSEI"
GAMMA_WALL_OFFSET = 100
ENTRY_TIME = time(9, 15)
EXIT_TIME = time(10, 15)  # 15m bar labeled 10:15 covers 10:15-10:30, so its Close = 10:30 price.
MARKET_TZ = "Asia/Kolkata"

DATETIME_COL_HINTS = ("datetime", "date and time", "date", "timestamp")
PRICE_COL_HINTS = ("preopen_nifty 50", "preopen_nifty50", "nifty 50", "nifty50", "iep", "price")

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-7s | %(message)s")
logger = logging.getLogger("spoofing_backtester")


def find_column(columns, hints) -> str | None:
    normalized = {col: str(col).strip().lower().replace("_", " ") for col in columns}
    for hint in hints:
        for col, clean in normalized.items():
            if clean == hint:
                return col
    for hint in hints:
        for col, clean in normalized.items():
            if hint in clean:
                return col
    return None


def read_file(file_path: Path) -> pd.DataFrame | None:
    for encoding in ENCODINGS_TO_TRY:
        try:
            return pd.read_csv(file_path, encoding=encoding, sep=None, engine="python")
        except Exception:
            continue
    return None


def load_all_ticks() -> pd.DataFrame:
    if not SOURCE_DIR.exists():
        raise FileNotFoundError(f"Source directory not found: {SOURCE_DIR}")

    source_files = [p for p in SOURCE_DIR.rglob("*") if p.is_file() and p.suffix.lower() in FILE_EXTENSIONS]
    logger.info(f"Found {len(source_files)} candidate files under {SOURCE_DIR}")

    frames = []
    for file_path in source_files:
        df = read_file(file_path)
        if df is None or df.empty:
            logger.warning(f"Skipping {file_path}: empty or unreadable.")
            continue

        datetime_col = find_column(df.columns, DATETIME_COL_HINTS)
        price_col = find_column(df.columns, PRICE_COL_HINTS)
        if datetime_col is None or price_col is None:
            logger.warning(f"Skipping {file_path}: missing DateTime/price column (columns={list(df.columns)}).")
            continue

        subset = df[[datetime_col, price_col]].rename(columns={datetime_col: "DateTime", price_col: "preOpen_NIFTY 50"})
        frames.append(subset)

    if not frames:
        raise ValueError("No valid tick data found to backtest.")

    combined = pd.concat(frames, ignore_index=True)
    return combined


def build_daily_trap_stats(ticks: pd.DataFrame) -> pd.DataFrame:
    ticks["DateTime"] = pd.to_datetime(ticks["DateTime"], errors="coerce")
    ticks["preOpen_NIFTY 50"] = pd.to_numeric(ticks["preOpen_NIFTY 50"], errors="coerce")
    ticks = ticks.dropna(subset=["DateTime", "preOpen_NIFTY 50"])
    ticks["Date"] = ticks["DateTime"].dt.date
    ticks = ticks.sort_values("DateTime")

    daily = ticks.groupby("Date").agg(
        Max_IEP=("preOpen_NIFTY 50", "max"),
        Min_IEP=("preOpen_NIFTY 50", "min"),
        Final_IEP=("preOpen_NIFTY 50", "last"),
    ).reset_index()

    daily["Bull_Trap"] = (daily["Max_IEP"] - daily["Final_IEP"]) > TRAP_THRESHOLD
    daily["Bear_Trap"] = (daily["Final_IEP"] - daily["Min_IEP"]) > TRAP_THRESHOLD
    return daily.sort_values("Date").reset_index(drop=True)


def fetch_daily_ohlc_with_prev_close(min_date, max_date) -> pd.DataFrame:
    # Pull a lookback buffer so the first requested day still has a valid previous close.
    buffered_start = min_date - timedelta(days=10)
    raw = yf.download(NIFTY_TICKER, start=buffered_start, end=max_date + timedelta(days=1), progress=False)
    if raw.empty:
        raise ValueError("yfinance returned no Nifty 50 OHLC data for the requested date range.")

    if isinstance(raw.columns, pd.MultiIndex):
        raw.columns = raw.columns.get_level_values(0)

    ohlc = raw[["Open", "High", "Low", "Close"]].reset_index()
    ohlc["Date"] = pd.to_datetime(ohlc["Date"]).dt.date
    ohlc = ohlc.sort_values("Date")
    ohlc["Prev_Close"] = ohlc["Close"].shift(1)
    return ohlc[ohlc["Date"] >= min_date].reset_index(drop=True)


def apply_gamma_wall_confluence(daily: pd.DataFrame, ohlc: pd.DataFrame) -> pd.DataFrame:
    merged = daily.merge(ohlc, on="Date", how="inner").dropna(subset=["Prev_Close"])
    merged["Mock_Call_Wall"] = merged["Prev_Close"] + GAMMA_WALL_OFFSET
    merged["Mock_Put_Wall"] = merged["Prev_Close"] - GAMMA_WALL_OFFSET

    def classify(row) -> str:
        if row["Bull_Trap"] and row["Open"] > row["Mock_Call_Wall"]:
            return "HIGH_PROB_BUY_PE"
        if row["Bear_Trap"] and row["Open"] < row["Mock_Put_Wall"]:
            return "HIGH_PROB_BUY_CE"
        return "NO_SIGNAL"

    merged["Signal"] = merged.apply(classify, axis=1)
    return merged


def fetch_time_based_exit(trade_date) -> tuple[float, float] | None:
    """Fetch 15m bars for one session and return (Entry_915, Exit_1030) closes."""
    start = datetime.combine(trade_date, time.min)
    end = start + timedelta(days=1)
    try:
        raw = yf.download(NIFTY_TICKER, start=start, end=end, interval="15m", progress=False)
    except Exception as exc:
        logger.warning(f"Intraday fetch failed for {trade_date}: {exc}")
        return None

    if raw.empty:
        logger.warning(f"No 15m intraday bars available for {trade_date} (likely outside yfinance's ~60-day window).")
        return None

    if isinstance(raw.columns, pd.MultiIndex):
        raw.columns = raw.columns.get_level_values(0)

    index = raw.index
    if index.tz is not None:
        index = index.tz_convert(MARKET_TZ)
    raw = raw.set_axis(index)

    entry_rows = raw[raw.index.time == ENTRY_TIME]
    exit_rows = raw[raw.index.time == EXIT_TIME]
    if entry_rows.empty or exit_rows.empty:
        logger.warning(f"Missing 09:15 or 10:15 bar for {trade_date}; skipping time-based exit.")
        return None

    return float(entry_rows["Open"].iloc[0]), float(exit_rows["Close"].iloc[0])


def simulate_time_based_trades(trades: pd.DataFrame) -> pd.DataFrame:
    entries, exits, points = [], [], []
    for _, row in trades.iterrows():
        result = fetch_time_based_exit(row["Date"])
        if result is None:
            entries.append(None)
            exits.append(None)
            points.append(None)
            continue

        entry_price, exit_price = result
        entries.append(entry_price)
        exits.append(exit_price)
        if row["Signal"] == "HIGH_PROB_BUY_CE":
            points.append(exit_price - entry_price)
        else:  # HIGH_PROB_BUY_PE
            points.append(entry_price - exit_price)

    trades = trades.copy()
    trades["Entry_915"] = entries
    trades["Exit_1030"] = exits
    trades["Points_Captured"] = points
    return trades.dropna(subset=["Points_Captured"]).reset_index(drop=True)


def main() -> None:
    ticks = load_all_ticks()
    daily = build_daily_trap_stats(ticks)

    trapped = daily[daily["Bull_Trap"] | daily["Bear_Trap"]].copy()
    if trapped.empty:
        print("No trap days detected; nothing to backtest.")
        return

    ohlc = fetch_daily_ohlc_with_prev_close(trapped["Date"].min(), trapped["Date"].max())
    confluence = apply_gamma_wall_confluence(trapped, ohlc)

    high_prob = confluence[confluence["Signal"] != "NO_SIGNAL"].copy()
    if high_prob.empty:
        print("No HIGH_PROB confluence trades found (trap + gamma wall breach never coincided).")
        return

    trades = simulate_time_based_trades(high_prob)
    if trades.empty:
        print("HIGH_PROB signals found, but no 15m intraday data was available to price them "
              "(yfinance only serves ~60 days of intraday history).")
        return

    trades["Points_Captured"] = trades["Points_Captured"].round(2)

    pd.set_option("display.max_rows", None)
    pd.set_option("display.width", 160)
    print("\n=== God Mode: HIGH_PROB Confluence Trades (09:15 -> 10:30 Time Exit) ===")
    print(trades[["Date", "Signal", "Mock_Call_Wall", "Mock_Put_Wall", "Open", "Entry_915", "Exit_1030", "Points_Captured"]].to_string(index=False))

    total_trades = len(trades)
    total_wins = int((trades["Points_Captured"] > 0).sum())
    win_rate = (total_wins / total_trades * 100) if total_trades else 0.0
    net_points = trades["Points_Captured"].sum()

    print("\n=== Summary Report ===")
    print(f"Total Trades       : {total_trades}")
    print(f"Total Wins         : {total_wins}")
    print(f"Total Losses       : {total_trades - total_wins}")
    print(f"Win Rate           : {win_rate:.2f}%")
    print(f"Net Points Captured: {net_points:.2f}")


if __name__ == "__main__":
    main()

"""
Organize raw pre-open tick data files by their ACTUAL internal trade date.

Recursively scans SOURCE_DIR for .csv/.txt files, reads each file's first valid
data row, extracts the trading date from a date/time column, and copies the
file into TARGET_DIR renamed as preopen_tick_YYYY-MM-DD.csv.
"""
import logging
import re
import shutil
from datetime import datetime
from pathlib import Path

import pandas as pd
from dateutil import parser as dateutil_parser

SOURCE_DIR = Path(r"C:\Users\ADMIN\Desktop\preopen")
TARGET_DIR = Path(r"C:\Users\ADMIN\Desktop\Fari Preopen")
FILE_EXTENSIONS = (".csv", ".txt")
ENCODINGS_TO_TRY = ("utf-8-sig", "utf-8", "latin1")
DATE_COLUMN_HINTS = ("datetime", "date and time", "date", "timestamp", "time")
ISO_DATE_PREFIX = re.compile(r"^(\d{4})[-/](\d{1,2})[-/](\d{1,2})")

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-7s | %(message)s")
logger = logging.getLogger("organize_preopen_data")


def find_date_column(columns) -> str | None:
    normalized = {col: str(col).strip().lower().replace("_", " ") for col in columns}
    for hint in DATE_COLUMN_HINTS:
        for col, clean in normalized.items():
            if clean == hint:
                return col
    for hint in DATE_COLUMN_HINTS:
        for col, clean in normalized.items():
            if hint in clean:
                return col
    return None


def read_dataframe(file_path: Path) -> pd.DataFrame | None:
    for encoding in ENCODINGS_TO_TRY:
        try:
            return pd.read_csv(file_path, encoding=encoding, sep=None, engine="python")
        except Exception:
            continue
    return None


def parse_trade_datetime(raw_value: str) -> datetime:
    text = str(raw_value).strip()
    # Dates already in YYYY-MM-DD(/) form are unambiguous; parsing them with
    # dayfirst=True would wrongly swap month/day, so handle them explicitly.
    iso_match = ISO_DATE_PREFIX.match(text)
    if iso_match:
        year, month, day = (int(part) for part in iso_match.groups())
        return datetime(year, month, day)
    # Otherwise assume DD-MM-YYYY, the common NSE export convention.
    return dateutil_parser.parse(text, dayfirst=True)


def extract_trade_date(file_path: Path) -> str | None:
    df = read_dataframe(file_path)
    if df is None or df.empty:
        logger.warning(f"Skipping {file_path}: empty or unreadable.")
        return None

    date_col = find_date_column(df.columns)
    if date_col is None:
        logger.warning(f"Skipping {file_path}: no date/time column found (columns={list(df.columns)}).")
        return None

    for raw_value in df[date_col]:
        if pd.isna(raw_value):
            continue
        try:
            parsed = parse_trade_datetime(raw_value)
            return parsed.strftime("%Y-%m-%d")
        except (ValueError, OverflowError):
            continue

    logger.warning(f"Skipping {file_path}: no parseable date found in column '{date_col}'.")
    return None


def build_target_path(trade_date: str) -> Path:
    candidate = TARGET_DIR / f"preopen_tick_{trade_date}.csv"
    counter = 1
    while candidate.exists():
        candidate = TARGET_DIR / f"preopen_tick_{trade_date}_{counter}.csv"
        counter += 1
    return candidate


def main() -> None:
    TARGET_DIR.mkdir(parents=True, exist_ok=True)

    if not SOURCE_DIR.exists():
        logger.error(f"Source directory not found: {SOURCE_DIR}")
        return

    source_files = [p for p in SOURCE_DIR.rglob("*") if p.is_file() and p.suffix.lower() in FILE_EXTENSIONS]
    logger.info(f"Found {len(source_files)} candidate data files under {SOURCE_DIR}")

    processed = 0
    skipped = 0

    for file_path in source_files:
        try:
            trade_date = extract_trade_date(file_path)
            if not trade_date:
                skipped += 1
                continue

            target_path = build_target_path(trade_date)
            shutil.copy2(file_path, target_path)
            logger.info(f"Copied {file_path} -> {target_path}")
            processed += 1
        except Exception as exc:
            logger.warning(f"Skipping {file_path}: unexpected error: {exc}")
            skipped += 1

    print(f"\nSuccessfully processed and moved {processed} files to '{TARGET_DIR.name}'. Skipped {skipped} file(s).")


if __name__ == "__main__":
    main()

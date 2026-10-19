import csv
import glob
import os
import re
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

import django

# ---------------------------------------------------------------------------
# Environment bootstrap
# ---------------------------------------------------------------------------
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
repository_root = os.path.dirname(project_root)

# Central location for preopen CSVs (same env var the fetcher and dashboard use,
# so the three can never point at different folders again). Anchored to the repo
# root via __file__ so it works on Windows (local) and Linux (Render).
PREOPEN_CSV_DIR = os.getenv(
    "PREOPEN_TBT_DIR", str(Path(__file__).resolve().parents[2] / "preopen")
)

os.chdir(project_root)
sys.path.insert(0, repository_root)
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "Nse_Project.settings")
django.setup()

from Nse_Project.Preopen.models import PreopenSummary

IST = timezone(timedelta(hours=5, minutes=30))

# Formats accepted for the CSV datetime column (manual uploads vary).
_DATETIME_FORMATS = (
    "%d-%m-%Y %H:%M", "%d-%m-%Y %H:%M:%S",
    "%d/%m/%Y %H:%M", "%d/%m/%Y %H:%M:%S",
    "%m/%d/%Y %H:%M", "%m/%d/%Y %H:%M:%S",
    "%d-%m-%y %H:%M", "%d/%m/%y %H:%M",
    "%Y-%m-%d %H:%M", "%Y-%m-%d %H:%M:%S",
    "%Y/%m/%d %H:%M", "%Y/%m/%d %H:%M:%S",
    "%d-%m-%Y %I:%M:%S %p", "%d/%m/%Y %I:%M:%S %p",
)

_UPDATE_FIELDS = ("open", "high", "low", "close")


def parse_datetime(dtstr):
    """Parse a datetime string against all supported formats (naive)."""
    for fmt in _DATETIME_FORMATS:
        try:
            return datetime.strptime(dtstr, fmt)
        except ValueError:
            continue
    return None


def parse_date_from_name(path):
    """Extract a ``date`` from a CSV filename, or ``None`` if none is embedded."""
    name = os.path.basename(path)
    for pattern, fmt in (
        (r"(\d{2})(\d{2})(\d{4})", "%d%m%Y"),            # DDMMYYYY
        (r"(\d{4})(\d{2})(\d{2})", "%Y%m%d"),            # YYYYMMDD
        (r"(\d{4})[-_](\d{2})[-_](\d{2})", "%Y-%m-%d"),  # YYYY-MM-DD / YYYY_MM_DD
        (r"(\d{2})[-_](\d{2})[-_](\d{4})", "%d-%m-%Y"),  # DD-MM-YYYY / DD_MM_YYYY
    ):
        m = re.search(pattern, name)
        if m:
            raw = m.group(0).replace("_", "-")
            try:
                return datetime.strptime(raw, fmt).date()
            except ValueError:
                continue
    return None


def _detect_columns(fieldnames):
    """Return ``(datetime_col, date_col, time_col, value_cols)`` for a CSV header."""
    datetime_col = None
    date_col = None
    time_col = None
    value_cols = []

    for col in fieldnames:
        col = str(col)
        col_clean = col.lower().replace("_", "").replace(" ", "")
        if col_clean in ("datetime", "dateandtime"):
            datetime_col = col
        elif col_clean == "date" and datetime_col is None:
            date_col = col
        elif col_clean == "time" and datetime_col is None:
            time_col = col

        if col_clean in ("preopennifty50", "preopennifty_50", "preopennifty", "preopenniftyopen"):
            value_cols.append(col)
        if col_clean in ("nifty50", "nifty_50", "nifty"):
            value_cols.append(col)

    return datetime_col, date_col, time_col, value_cols


def _read_csv_ticks(csv_path):
    """Stream ``csv_path`` and return ``{date: [(epoch_ms, price), ...]}``.

    Returns ``None`` when the file is unreadable or contains no usable rows.
    """
    try:
        with open(csv_path, "r", encoding="utf-8-sig", newline="") as fh:
            reader = csv.DictReader(fh)
            if not reader.fieldnames:
                print(f"Skipping empty CSV: {csv_path}")
                return None

            datetime_col, date_col, time_col, value_cols = _detect_columns(reader.fieldnames)
            if not datetime_col and not (date_col and time_col):
                print(f"Could not find datetime columns in CSV: {csv_path}")
                return None
            if not value_cols:
                print(f"Could not find value columns in CSV: {csv_path}")
                return None

            grouped = defaultdict(list)
            valid = 0
            for row in reader:
                if datetime_col:
                    dt_raw = row.get(datetime_col, "")
                else:
                    dt_raw = f"{row.get(date_col, '')} {row.get(time_col, '')}"

                dt_val = parse_datetime(dt_raw.strip()) if dt_raw else None
                if dt_val is None:
                    continue

                price = None
                for vcol in value_cols:
                    raw = (row.get(vcol) or "").strip()
                    if raw:
                        try:
                            price = float(raw)
                            break
                        except ValueError:
                            continue
                if price is None:
                    continue

                ts = int(dt_val.replace(tzinfo=IST).timestamp() * 1000)
                grouped[dt_val.date()].append((ts, price))
                valid += 1

            if valid == 0:
                print(f"No valid rows found in CSV: {csv_path}")
                return None
            return grouped

    except OSError as exc:
        print(f"ERROR: could not read {csv_path}: {exc}")
        return None
    except Exception as exc:  # noqa: BLE001 - keep the batch alive on any bad file
        print(f"ERROR: unexpected issue reading {csv_path}: {exc}")
        return None


def _compute_ohlc(ticks):
    """Compute ``(open, high, low, close)`` from a tick series in one pass."""
    ticks = sorted(ticks, key=lambda t: t[0])  # ensure chronological order
    if not ticks:
        return None

    open_val = ticks[0][1]
    close_val = ticks[-1][1]
    high_val = low_val = ticks[0][1]
    for _, price in ticks:
        if price > high_val:
            high_val = price
        if price < low_val:
            low_val = price
    return open_val, high_val, low_val, close_val


def import_preopen_data_to_db():
    """Import fresh pre-open CSV(s) into ``PreopenSummary``.

    Returns ``True`` if at least one row was created/updated, ``False`` otherwise.
    """
    csv_directory = PREOPEN_CSV_DIR
    today_str = datetime.now(IST).date().strftime("%Y-%m-%d")

    # Priority: today's dated file, then the active file, then any other CSV
    # dropped directly in this folder (manual uploads). The archive/ subfolder is
    # deliberately excluded because it holds historical backups, not fresh input.
    priority_names = [f"nifty50_graph_data_{today_str}.csv", "nifty50_graph_data.csv"]
    csv_files = []
    seen = set()
    for name in priority_names:
        path = os.path.abspath(os.path.join(csv_directory, name))
        if path in seen:
            continue
        seen.add(path)
        if os.path.isfile(path):
            csv_files.append(path)

    for path in sorted(glob.glob(os.path.join(csv_directory, "*.csv"))):
        path = os.path.abspath(path)
        if path not in seen:
            seen.add(path)
            csv_files.append(path)

    if not csv_files:
        print(f"No CSV files found in {csv_directory}")
        return False

    # Newest first (embedded filename date, falling back to file mtime).
    scored = sorted(
        ((parse_date_from_name(f) or datetime.min.date(), os.path.getmtime(f), f) for f in csv_files),
        key=lambda x: (x[0], x[1]),
        reverse=True,
    )

    print("CSV import order (newest first):")
    for i, (d, mt, f) in enumerate(scored):
        mt_readable = datetime.fromtimestamp(mt).strftime("%Y-%m-%d %H:%M:%S")
        print(f"  #{i + 1}: {f} | name-date={d} | mtime={mt_readable}")

    # Collect per-date tick series, preferring the newest file for a given date.
    per_date_data = {}
    for _date_in_name, mtime, csv_path in scored:
        grouped = _read_csv_ticks(csv_path)
        if not grouped:
            continue
        for date_key, ticks in grouped.items():
            existing = per_date_data.get(date_key)
            if existing and existing["mtime"] >= mtime:
                print(f"Skipping {csv_path} for {date_key}: newer or same file already used")
                continue
            per_date_data[date_key] = {"ticks": ticks, "mtime": mtime, "source": csv_path}

    if not per_date_data:
        print("No usable data found in CSV files")
        return False

    # Single-pass OHLC per date.
    summaries = {}
    for date_key, info in per_date_data.items():
        ohlc = _compute_ohlc(info["ticks"])
        if ohlc is None:
            print(f"Skipping {date_key}: no usable ticks")
            continue
        summaries[date_key] = ohlc

    if not summaries:
        print("No OHLC computed from CSV files")
        return False

    # ---- Database write (bulk instead of one round-trip per row) ----
    try:
        existing = {
            obj.date: obj
            for obj in PreopenSummary.objects.filter(date__in=list(summaries.keys()))
        }
    except Exception as exc:
        print(f"ERROR: failed to query existing PreopenSummary rows: {exc}")
        return False

    to_create = []
    to_update = []
    for date_key, (open_val, high_val, low_val, close_val) in summaries.items():
        if date_key in existing:
            obj = existing[date_key]
            obj.open = open_val
            obj.high = high_val
            obj.low = low_val
            obj.close = close_val
            to_update.append(obj)
        else:
            to_create.append(
                PreopenSummary(
                    date=date_key,
                    open=open_val,
                    high=high_val,
                    low=low_val,
                    close=close_val,
                )
            )

    try:
        if to_create:
            PreopenSummary.objects.bulk_create(to_create)
        if to_update:
            PreopenSummary.objects.bulk_update(to_update, _UPDATE_FIELDS)
    except Exception as exc:
        print(f"ERROR: failed to write PreopenSummary rows: {exc}")
        return False

    for date_key in sorted(summaries):
        action = "Updated" if date_key in existing else "Created"
        print(f"{action} PreopenSummary for {date_key} (source: {per_date_data[date_key]['source']})")

    return True


def main():
    """Import today's fresh pre-open CSV into the DB.

    Returns 0 on success, 1 when no fresh CSV was imported.
    """
    print(f"Checking for CSV in: {PREOPEN_CSV_DIR}")
    try:
        imported = import_preopen_data_to_db()
    except Exception as exc:
        print(f"FAILED: unexpected error during import: {exc}")
        return 1

    if not imported:
        print("FAILED: no fresh pre-open CSV was imported.")
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

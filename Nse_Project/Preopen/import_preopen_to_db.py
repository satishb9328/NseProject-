import os
import sys
import django
import shutil
import json
from datetime import datetime, timedelta, timezone
# Ensure project root is in sys.path and set working directory
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
repository_root = os.path.dirname(project_root)
# Central location for preopen CSVs (same env var the fetcher and dashboard use,
# so the three can never point at different folders again).
PREOPEN_CSV_DIR = os.getenv("PREOPEN_TBT_DIR", r"C:\Users\ADMIN\Desktop\preopen")
os.chdir(project_root)
sys.path.insert(0, repository_root)
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'Nse_Project.settings')
django.setup()

from Nse_Project.Preopen.models import PreopenSummary

# Path to the "current" JSON file that fetch_preopen.py is expected to create
# (Previously pointed inside pre_hist_data with a leading slash that broke on Windows)
current_json_path = os.path.join(project_root, 'Preopen', 'data', 'preopen_data.json')
# Fallback (legacy) location inside history folder if current not present
legacy_json_path = os.path.join(project_root, 'Preopen', 'data', 'pre_hist_data', 'preopen_data.json')

# Resolve which JSON to use
if os.path.exists(current_json_path):
    json_path = current_json_path
elif os.path.exists(legacy_json_path):
    json_path = legacy_json_path
else:
    print(f"ERROR: preopen_data.json not found. Looked at: \n  {current_json_path}\n  {legacy_json_path}\nEnsure fetch script created the file.")
    json_path = None

# Load JSON data if available
data = {}
if json_path:
    try:
        with open(json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except FileNotFoundError:
        print(f"ERROR: File disappeared before reading: {json_path}")
    except json.JSONDecodeError as e:
        print(f"ERROR: JSON decode failed for {json_path}: {e}")
    except Exception as e:
        print(f"ERROR: Unexpected issue reading {json_path}: {e}")

# Now you can access the data, for example:
# print(data)
# You can add your logic here to upload data to the model

def parse_time(timestr):
    try:
        return datetime.strptime(timestr, "%H:%M:%S").time()
    except Exception:
        return None

def parse_date(datestr):
    try:
        return datetime.strptime(datestr, "%Y-%m-%d").date()
    except Exception:
        return None

def parse_datetime(dtstr):
    try:
        # Accept format: DD-MM-YYYY HH:MM
        return datetime.strptime(dtstr, "%d-%m-%Y %H:%M")
    except Exception:
        return None

def process_preopen_nifty50_from_json(grapth_data):
    # grapth_data is expected to be a list of [timestamp, value] pairs
    if not grapth_data:
        print('No data to process.')
        return
    IST = timezone(timedelta(hours=5, minutes=30))
    # Extract values and times
    values = [v for _, v in grapth_data]
    timestamps = [ts for ts, _ in grapth_data]
    datetimes = [
        datetime.fromtimestamp(ts/1000.0, timezone.utc).astimezone(IST)
        for ts in timestamps
    ]
    times = [dt.time() for dt in datetimes]
    date = datetimes[0].date() if datetimes else None
    open_val = values[0]
    open_time = times[0]
    high_val = max(values)
    high_idx = values.index(high_val)
    high_time = times[high_idx]
    low_val = min(values)
    low_idx = values.index(low_val)
    low_time = times[low_idx]
    close_val = values[-1]
    close_time = times[-1]
    avg_val = sum(values) / len(values)
    above_indices = [i for i, v in enumerate(values) if v > avg_val]
    below_indices = [i for i, v in enumerate(values) if v < avg_val]
    if above_indices:
        high_above_val = max([values[i] for i in above_indices])
        high_above_idx = above_indices[[values[i] for i in above_indices].index(high_above_val)]
        high_above_time = times[high_above_idx]
        low_above_val = min([values[i] for i in above_indices])
        low_above_idx = above_indices[[values[i] for i in above_indices].index(low_above_val)]
        low_above_time = times[low_above_idx]
        avg_above = (high_above_val + low_above_val) / 2
    else:
        high_above_val = None
        high_above_time = None
        low_above_val = None
        low_above_time = None
        avg_above = None
    if below_indices:
        high_below_val = max([values[i] for i in below_indices])
        high_below_idx = below_indices[[values[i] for i in below_indices].index(high_below_val)]
        high_below_time = times[high_below_idx]
        low_below_val = min([values[i] for i in below_indices])
        low_below_idx = below_indices[[values[i] for i in below_indices].index(low_below_val)]
        low_below_time = times[low_below_idx]
        avg_below = (high_below_val + low_below_val) / 2
    else:
        high_below_val = None
        high_below_time = None
        low_below_val = None
        low_below_time = None
        avg_below = None
    obj, created = PreopenSummary.objects.update_or_create(
        date=date,
        defaults={
            'open': open_val,
            'high': high_val,
            'low': low_val,
            'close': close_val,
        }
    )
    print(f"{'Created' if created else 'Updated'} PreopenSummary for {date}")

def import_preopen_data_to_db():
    import glob
    import csv
    import re
    from collections import defaultdict
    csv_directory = PREOPEN_CSV_DIR
    IST = timezone(timedelta(hours=5, minutes=30))

    # The fetcher (fetch_preopen.py) writes two files:
    #   * nifty50_graph_data.csv                    -> ACTIVE file (today's data)
    #   * archive/nifty50_graph_data_YYYY-MM-DD.csv -> dated history (NOT read here)
    # Prefer today's dated file / the active file, then fall back to any other CSV
    # dropped directly in this folder (manual uploads). The archive/ subfolder is
    # deliberately excluded because it holds historical backups, not fresh input.
    today_str = datetime.now(IST).date().strftime("%Y-%m-%d")
    priority_names = [
        f"nifty50_graph_data_{today_str}.csv",
        "nifty50_graph_data.csv",
    ]

    csv_files = []
    seen = set()
    for name in priority_names:
        path = os.path.abspath(os.path.join(csv_directory, name))
        if path in seen:
            continue
        seen.add(path)
        if os.path.isfile(path):
            csv_files.append(path)

    for path in sorted(glob.glob(os.path.join(csv_directory, '*.csv'))):
        path = os.path.abspath(path)
        if path not in seen:
            seen.add(path)
            csv_files.append(path)

    if not csv_files:
        print('No CSV files found in', csv_directory)
        return False

    def parse_date_from_name(path):
        name = os.path.basename(path)
        # Try DDMMYYYY
        m = re.search(r'(\d{2})(\d{2})(\d{4})', name)
        if m:
            try:
                return datetime.strptime(''.join(m.groups()), '%d%m%Y').date()
            except Exception:
                pass
        # Try YYYYMMDD
        m = re.search(r'(\d{4})(\d{2})(\d{2})', name)
        if m:
            try:
                return datetime.strptime(''.join(m.groups()), '%Y%m%d').date()
            except Exception:
                pass
        # Try YYYY-MM-DD or YYYY_MM_DD
        m = re.search(r'(\d{4})[-_](\d{2})[-_](\d{2})', name)
        if m:
            try:
                return datetime.strptime('-'.join(m.groups()), '%Y-%m-%d').date()
            except Exception:
                pass
        # Try DD-MM-YYYY or DD_MM_YYYY
        m = re.search(r'(\d{2})[-_](\d{2})[-_](\d{4})', name)
        if m:
            try:
                return datetime.strptime('-'.join(m.groups()), '%d-%m-%Y').date()
            except Exception:
                pass
        return None

    # Sort all CSVs newest first (by embedded date fallback mtime)
    scored = []
    for f in csv_files:
        date_in_name = parse_date_from_name(f)
        mtime = os.path.getmtime(f)
        scored.append((date_in_name, mtime, f))
    scored_sorted = sorted(scored, key=lambda x: (x[0] or datetime.min.date(), x[1]), reverse=True)
    print('CSV import order (newest first):')
    for i, (d, mt, f) in enumerate(scored_sorted):
        mt_readable = datetime.fromtimestamp(mt).strftime('%Y-%m-%d %H:%M:%S')
        print(f"  #{i+1}: {f} | name-date={d} | mtime={mt_readable}")

    def parse_datetime(dtstr):
        # Accept multiple common date/time formats (manual uploads vary)
        formats = [
            '%d-%m-%Y %H:%M', '%d-%m-%Y %H:%M:%S', '%d/%m/%Y %H:%M', '%d/%m/%Y %H:%M:%S',
            '%m/%d/%Y %H:%M', '%m/%d/%Y %H:%M:%S', '%d-%m-%y %H:%M', '%d/%m/%y %H:%M',
            '%Y-%m-%d %H:%M', '%Y-%m-%d %H:%M:%S', '%Y/%m/%d %H:%M', '%Y/%m/%d %H:%M:%S',
            '%d-%m-%Y %I:%M:%S %p', '%d/%m/%Y %I:%M:%S %p'
        ]
        for fmt in formats:
            try:
                return datetime.strptime(dtstr, fmt)
            except Exception:
                continue
        return None

    # Collect per-date graph data, preferring newer files for the same date
    per_date_data = {}

    for rank, (date_in_name, mtime, csv_path) in enumerate(scored_sorted, start=1):
        with open(csv_path, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        if not rows:
            print(f'Skipping empty CSV: {csv_path}')
            continue

        datetime_col = None
        value_cols = []
        date_col = None
        time_col = None
        for col in rows[0].keys():
            col_clean = col.lower().replace('_', '').replace(' ', '')
            if col_clean in ['datetime', 'dateandtime'] or col.strip().lower() == 'datetime':
                datetime_col = col
            if col_clean in ['date'] and not datetime_col:
                date_col = col
            if col_clean in ['time'] and not datetime_col:
                time_col = col
            if col_clean in ['preopennifty50', 'preopennifty_50', 'preopennifty', 'preopenniftyopen'] or col.strip().lower() == 'pre open nifty 50':
                value_cols.append(col)
            if col_clean in ['nifty50', 'nifty_50', 'nifty'] or col.strip().lower() == 'nifty 50':
                value_cols.append(col)
        if not datetime_col and date_col and time_col:
            print(f"No single datetime column found; using date='{date_col}' and time='{time_col}' for {csv_path}")
        if (not datetime_col and not (date_col and time_col)) or not value_cols:
            print(f'Could not find datetime or value columns in CSV: {csv_path}')
            continue

        grouped = defaultdict(list)
        valid_row_count = 0
        for idx, row in enumerate(rows):
            try:
                if datetime_col:
                    dt_raw = row.get(datetime_col, '')
                elif date_col and time_col:
                    dt_raw = f"{row.get(date_col, '')} {row.get(time_col, '')}"
                else:
                    dt_raw = ''
                dt_val = parse_datetime(dt_raw.strip()) if dt_raw else None
                if not dt_val:
                    continue
                nifty_val = None
                for vcol in value_cols:
                    val = row.get(vcol, '').strip()
                    if val:
                        try:
                            nifty_val = float(val)
                            break
                        except Exception:
                            pass
                if nifty_val is None:
                    continue
                ts = int(dt_val.replace(tzinfo=IST).timestamp() * 1000)
                grouped[dt_val.date()].append([ts, nifty_val])
                valid_row_count += 1
            except Exception:
                pass
        if valid_row_count == 0:
            print(f'No valid rows found in CSV: {csv_path}')
            continue

        for date_key, graph_data in grouped.items():
            existing = per_date_data.get(date_key)
            if existing and existing['mtime'] >= mtime:
                print(f"Skipping {csv_path} for {date_key}: newer or same file already used")
                continue
            per_date_data[date_key] = {'graph': graph_data, 'mtime': mtime, 'source': csv_path}

    if not per_date_data:
        print('No usable data found in CSV files')
        return False

    # Process dates in chronological order for consistency
    for date in sorted(per_date_data.keys()):
        grapth_data = sorted(per_date_data[date]['graph'], key=lambda x: x[0])  # ensure chronological order
        values = [v for _, v in grapth_data]
        timestamps = [ts for ts, _ in grapth_data]
        datetimes = [datetime.fromtimestamp(ts/1000.0, timezone.utc).astimezone(IST) for ts in timestamps]
        times = [dt.time() for dt in datetimes]
        open_val = values[0]
        open_time = times[0]
        high_val = max(values)
        high_idx = values.index(high_val)
        high_time = times[high_idx]
        low_val = min(values)
        low_idx = values.index(low_val)
        low_time = times[low_idx]
        close_val = values[-1]
        close_time = times[-1]
        avg_val = sum(values) / len(values)
        above_indices = [i for i, v in enumerate(values) if v > avg_val]
        below_indices = [i for i, v in enumerate(values) if v < avg_val]
        if above_indices:
            high_above_val = max([values[i] for i in above_indices])
            high_above_idx = above_indices[[values[i] for i in above_indices].index(high_above_val)]
            high_above_time = times[high_above_idx]
            low_above_val = min([values[i] for i in above_indices])
            low_above_idx = above_indices[[values[i] for i in above_indices].index(low_above_val)]
            low_above_time = times[low_above_idx]
            avg_above = (high_above_val + low_above_val) / 2
        else:
            high_above_val = None
            high_above_time = None
            low_above_val = None
            low_above_time = None
            avg_above = None
        if below_indices:
            high_below_val = max([values[i] for i in below_indices])
            high_below_idx = below_indices[[values[i] for i in below_indices].index(high_below_val)]
            high_below_time = times[high_below_idx]
            low_below_val = min([values[i] for i in below_indices])
            low_below_idx = below_indices[[values[i] for i in below_indices].index(low_below_val)]
            low_below_time = times[low_below_idx]
            avg_below = (high_below_val + low_below_val) / 2
        else:
            high_below_val = None
            high_below_time = None
            low_below_val = None
            low_below_time = None
            avg_below = None
        obj, created = PreopenSummary.objects.update_or_create(
            date=date,
            defaults={
                'open': open_val,
                'high': high_val,
                'low': low_val,
                'close': close_val,
            }
        )
        print(("Created" if created else "Updated") + f' PreopenSummary for {date} (source: {per_date_data[date]["source"]})')
    return True


def main():
    """Import today's fresh pre-open CSV into the DB.

    Returns 0 on success, 1 when no fresh CSV was imported. The legacy
    ``pre_hist_data/preopen_data.json`` archive (historical dates, e.g. 2025-07-30)
    is deliberately NOT used here — importing it was the cause of the false
    "Updated PreopenSummary for 2025-07-30" SUCCESS.
    """
    manual_folder = PREOPEN_CSV_DIR
    print(f"Checking for CSV in: {manual_folder}")
    imported = import_preopen_data_to_db()

    if not imported:
        print("FAILED: no fresh pre-open CSV was imported.")
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

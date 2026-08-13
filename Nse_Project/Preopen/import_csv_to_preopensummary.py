import os
import csv
import django
import sys
from datetime import datetime, timedelta, timezone
import glob

# Setup Django environment
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.chdir(project_root)
sys.path.insert(0, project_root)
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'myproject.settings')
django.setup()

from Preopen.models import PreopenSummary

# CSV directory path (update as needed)
csv_directory = r"C:\Users\ADMIN\Desktop\preopen"

IST = timezone(timedelta(hours=5, minutes=30))

def parse_datetime(dtstr):
    for fmt in ("%d-%m-%Y %H:%M", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            return datetime.strptime(dtstr, fmt)
        except Exception:
            continue
    return None

def import_csv_to_preopensummary(csv_path):
    with open(csv_path, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    if not rows:
        return

    # Accept columns with spaces and different casing
    datetime_col = None
    value_cols = []
    for col in rows[0].keys():
        col_clean = col.lower().replace('_', '').replace(' ', '')
        if col_clean in ['datetime', 'dateandtime'] or col.strip().lower() == 'datetime':
            datetime_col = col
        if col_clean in ['preopennifty50', 'preopennifty_50', 'preopennifty', 'preopenniftyopen'] or col.strip().lower() == 'pre open nifty 50':
            value_cols.append(col)
        if col_clean in ['nifty50', 'nifty_50', 'nifty'] or col.strip().lower() == 'nifty 50':
            value_cols.append(col)

    if not datetime_col or not value_cols:
        return

    from collections import defaultdict
    grouped = defaultdict(list)
    valid_row_count = 0

    for idx, row in enumerate(rows):
        try:
            dt_raw = row.get(datetime_col, '')
            dt_val = parse_datetime(dt_raw)
            if not dt_val:
                continue

            # Pick the first non-empty value from value_cols
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
        return

    # Process each date
    for date, grapth_data in grouped.items():
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
    return True

def import_all_csv_files():
    """Import all CSV files from the directory."""
    # Find all CSV files in the directory
    all_items = os.listdir(csv_directory)
    csv_files = []

    for item in all_items:
        item_path = os.path.join(csv_directory, item)
        # Only include files (not directories) and ensure they have .csv extension
        if os.path.isfile(item_path) and item.lower().endswith('.csv'):
            csv_files.append(item_path)

    if not csv_files:
        print(f"No CSV files found in {csv_directory}")
        return

    print(f"Found {len(csv_files)} CSV files to process")

    files_processed = 0
    for i, csv_file in enumerate(csv_files, 1):
        print(f"Uploading CSV file: {csv_file}")
        try:
            result = import_csv_to_preopensummary(csv_file)
            if result:
                files_processed += 1
        except Exception as e:
            print(f"Error processing {os.path.basename(csv_file)}: {str(e)}")

    print(f"CSV import completed. Processed {files_processed} out of {len(csv_files)} CSV files")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        # If a CSV file path is provided as an argument, import that file
        csv_path = sys.argv[1]
        print(f"Uploading CSV file: {csv_path}")
        try:
            result = import_csv_to_preopensummary(csv_path)
            if result:
                print(f"CSV import completed for {csv_path}")
            else:
                print(f"No valid data imported from {csv_path}")
        except Exception as e:
            print(f"Error processing {os.path.basename(csv_path)}: {str(e)}")
    else:
        # If no argument is provided, import all CSV files from the default directory
        import_all_csv_files()

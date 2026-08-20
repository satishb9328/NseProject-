import argparse
import datetime as dt
import io
import os
import time

import pandas as pd
import requests

# Output directory for participant OI files
OUTPUT_DIR = r"D:\FNO DATA\Participant data"

# NSE archive URL template for daily participant OI CSV
NSE_URL_TEMPLATE = 'https://archives.nseindia.com/content/nsccl/fao_participant_oi_{ddmmyyyy}.csv'

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
    "Accept": "text/csv,application/csv,*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.nseindia.com/",
}

SESSION_PRIME_URL = 'https://www.nseindia.com/'


def format_ddmmyyyy(date_obj: dt.date) -> str:
    return date_obj.strftime('%d%m%Y')


def format_yyyymmdd(date_obj: dt.date) -> str:
    return date_obj.strftime('%Y%m%d')


def parse_date(value: str) -> dt.date:
    return dt.datetime.strptime(value, '%Y-%m-%d').date()


def output_filename(date_obj: dt.date) -> str:
    return f'participant_oi_{format_yyyymmdd(date_obj)}.csv'


def build_url(date_obj: dt.date) -> str:
    return NSE_URL_TEMPLATE.format(ddmmyyyy=format_ddmmyyyy(date_obj))


def fetch_participant_oi(date_obj: dt.date) -> None:
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    output_path = os.path.join(OUTPUT_DIR, output_filename(date_obj))
    if os.path.exists(output_path):
        return

    session = requests.Session()
    session.headers.update(HEADERS)
    try:
        session.get(SESSION_PRIME_URL, timeout=20)
    except Exception:
        pass

    response = session.get(build_url(date_obj), timeout=30)
    if response.status_code != 200:
        return

    content = response.content
    text_preview = content[:200].decode('utf-8', errors='replace').strip()
    if text_preview.startswith('{') or text_preview.lower().startswith('<html') or 'error' in text_preview.lower():
        return

    try:
        df = pd.read_csv(io.BytesIO(content))
    except Exception:
        return

    df.to_csv(output_path, index=False)
    time.sleep(1)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Download NSE participant OI CSV for a given date')
    parser.add_argument('--date', type=str, default=dt.date.today().strftime('%Y-%m-%d'),
                        help='Date to download in YYYY-MM-DD format')
    args = parser.parse_args()
    date_obj = parse_date(args.date)
    fetch_participant_oi(date_obj)

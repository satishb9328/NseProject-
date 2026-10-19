import argparse
import datetime as dt
import os
import re
import time

import requests

# రెండు ఫైల్స్ సేవ్ చేయడానికి అవసరమైన ఫోల్డర్ పాత్‌లు
OUTPUT_DIR_OI = r"D:\FNO DATA\Participant data\OpenInterest"
OUTPUT_DIR_VOL = r"D:\FNO DATA\Participant data\Volume"

# రెండు ఫైల్స్ డౌన్‌లోడ్ చేసుకోవడానికి NSE URL టెంప్లేట్స్ 
URL_TEMPLATE_OI = 'https://archives.nseindia.com/content/nsccl/fao_participant_oi_{ddmmyyyy}.csv'
URL_TEMPLATE_VOL = 'https://archives.nseindia.com/content/nsccl/fao_participant_vol_{ddmmyyyy}.csv'

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
    "Accept": "text/csv,application/csv,*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.nseindia.com/",
}

SESSION_PRIME_URL = 'https://www.nseindia.com/'

# Add or update NSE trading holidays for the current year here.
NSE_HOLIDAYS = set()

# ── Network resilience ──────────────────────────────────────────────
REQUEST_TIMEOUT = 30
MAX_RETRIES = 3
BACKOFF_SECONDS = 2.0
RETRYABLE_STATUS = {403, 408, 429, 500, 502, 503, 504}


def _get_with_retry(session, url, timeout=REQUEST_TIMEOUT):
    """GET with exponential backoff for NSE's flaky archive endpoints."""
    attempt = 0
    while True:
        attempt += 1
        try:
            resp = session.get(url, timeout=timeout)
            if resp.status_code == 200:
                return resp
            if resp.status_code not in RETRYABLE_STATUS:
                return resp
            msg = f'HTTP {resp.status_code}'
        except requests.RequestException as exc:
            msg = str(exc)
            if attempt >= MAX_RETRIES:
                raise
        if attempt >= MAX_RETRIES:
            raise requests.RequestException(f'Exhausted retries: {msg}')
        delay = BACKOFF_SECONDS * (2 ** (attempt - 1))
        print(f'   -> {msg} (attempt {attempt}/{MAX_RETRIES}); retrying in {delay:.0f}s...')
        time.sleep(delay)


def _is_valid_participant_csv(content: bytes) -> bool:
    """Confirm the payload is a real participant OI/Volume CSV.

    Finds the header row (first cell starts with 'client') and requires at
    least one data row after it. Works for both OI and Volume files.
    """
    text = content.decode('utf-8-sig', errors='replace')
    if not text.strip():
        return False
    lines = text.splitlines()
    header_idx = None
    for i, line in enumerate(lines):
        first = line.split(',', 1)[0].strip().lower()
        if first.startswith('client'):
            header_idx = i
            break
    if header_idx is None:
        return False
    return any(line.strip() for line in lines[header_idx + 1:])


def format_ddmmyyyy(date_obj: dt.date) -> str:
    return date_obj.strftime('%d%m%Y')


def format_yyyymmdd(date_obj: dt.date) -> str:
    return date_obj.strftime('%Y%m%d')


def parse_date(value: str) -> dt.date:
    return dt.datetime.strptime(value, '%Y-%m-%d').date()


def get_last_downloaded_date(directory: str) -> dt.date | None:
    """Find the latest valid participant OI date already saved locally."""
    latest_date = None
    try:
        filenames = os.listdir(directory)
    except FileNotFoundError:
        return None

    for filename in filenames:
        match = re.fullmatch(r'participant_oi_(\d{8})\.csv', filename, flags=re.IGNORECASE)
        if not match:
            continue
        try:
            file_date = dt.datetime.strptime(match.group(1), '%Y%m%d').date()
        except ValueError:
            continue
        if latest_date is None or file_date > latest_date:
            latest_date = file_date
    return latest_date


def get_missing_trading_dates(today: dt.date | None = None) -> list[dt.date]:
    """Return weekdays after the latest OI file, or the last 7 days if empty."""
    today = today or dt.date.today()
    last_downloaded = get_last_downloaded_date(OUTPUT_DIR_OI)
    start_date = last_downloaded + dt.timedelta(days=1) if last_downloaded else today - dt.timedelta(days=7)

    valid_dates = []
    current_date = start_date
    while current_date <= today:
        if current_date.weekday() < 5 and current_date not in NSE_HOLIDAYS:
            valid_dates.append(current_date)
        current_date += dt.timedelta(days=1)
    return valid_dates


# ఈ ఫంక్షన్ OI మరియు Volume ఫైల్స్ రెండింటికీ పనిచేస్తుంది
def download_report(date_obj: dt.date, url_template: str, output_dir: str, file_prefix: str, session: requests.Session) -> None:
    os.makedirs(output_dir, exist_ok=True)
    file_name = f'{file_prefix}_{format_yyyymmdd(date_obj)}.csv'
    output_path = os.path.join(output_dir, file_name)
    
    if os.path.exists(output_path):
        print(f"ఫైల్ ఇప్పటికే ఉంది: {output_path}")
        return

    url = url_template.format(ddmmyyyy=format_ddmmyyyy(date_obj))
    print(f"డౌన్‌లోడ్ అవుతోంది: {url}")

    try:
        response = _get_with_retry(session, url)
    except requests.RequestException as exc:
        print(f"డౌన్‌లోడ్ అభ్యర్థన విఫలమైంది: {exc}")
        return
    if response.status_code != 200:
        print(f"డౌన్‌లోడ్ విఫలమైంది (Status: {response.status_code}) - బహుశా ఆ రోజు డేటా ఇంకా రాలేదు.")
        return

    content = response.content
    text_preview = content[:200].decode('utf-8', errors='replace').strip()
    if text_preview.startswith('{') or text_preview.lower().startswith('<html') or 'error' in text_preview.lower():
        print(f"తప్పు ఫార్మాట్ ఫైల్ వచ్చింది: {file_name}")
        return

    # Data integrity: only save if it looks like a real participant CSV.
    if not _is_valid_participant_csv(content):
        print(f"Invalid CSV payload (bad header), skipping: {file_name}")
        return

    # Save raw bytes unchanged so the engine's preamble-aware parser works.
    with open(output_path, 'wb') as f:
        f.write(content)
    print(f"Saved successfully: {output_path}")


def create_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(HEADERS)

    # కుకీస్ (Cookies) జనరేట్ చేయడం కోసం ముందుగా ప్రధాన పేజీని లోడ్ చేయడం
    try:
        session.get(SESSION_PRIME_URL, timeout=20)
    except Exception:
        pass
    return session


def fetch_participant_data(date_obj: dt.date, session: requests.Session) -> None:

    print(f"\n--- {date_obj} తేదీకి సంబంధించిన ఫైల్స్ కోసం వెతుకుతోంది ---")
    
    # 1. మొదట Open Interest డేటాను డౌన్‌లోడ్ చేస్తుంది
    download_report(date_obj, URL_TEMPLATE_OI, OUTPUT_DIR_OI, "participant_oi", session)

    # 2. ఆ వెంటనే Trading Volume డేటాను డౌన్‌లోడ్ చేస్తుంది
    download_report(date_obj, URL_TEMPLATE_VOL, OUTPUT_DIR_VOL, "participant_vol", session)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Download NSE participant OI and Volume CSVs for a given date')
    parser.add_argument('--date', type=str, default=None,
                        help='Download one date (YYYY-MM-DD); omit to backfill missing trading days')
    args = parser.parse_args()
    session = create_session()
    dates = [parse_date(args.date)] if args.date else get_missing_trading_dates()

    if not dates:
        print('No missing trading dates to download.')
    for index, date_obj in enumerate(dates):
        fetch_participant_data(date_obj, session)
        if index < len(dates) - 1:
            time.sleep(2)
    session.close()

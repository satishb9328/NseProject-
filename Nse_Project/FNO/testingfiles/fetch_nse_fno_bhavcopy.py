import requests
import datetime
import os
import time
import zipfile
from io import BytesIO


# Target output folder
OUTPUT_DIR = r"D:\FNO DATA\FUTUERE DATA"
os.makedirs(OUTPUT_DIR, exist_ok=True)

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36',
    'Accept': '*/*',
    'Accept-Language': 'en-US,en;q=0.9',
    'Accept-Encoding': 'gzip, deflate, br',
    'Referer': 'https://www.nseindia.com/',
}

# NSE archive derivatives daily bhav CSV URL
# Example: https://archives.nseindia.com/content/historical/DERIVATIVES/2026/MAR/fo20032026bhav.csv

START_DATE = datetime.date(2021, 4, 1)
END_DATE = datetime.date.today()


# Indian market holidays hardcoded 2026 (fill more if needed)
MARKET_HOLIDAYS = {
    datetime.date(2026, 1, 26),
    datetime.date(2026, 3, 8),
    datetime.date(2026, 3, 10),
    datetime.date(2026, 3, 25),
    datetime.date(2026, 4, 2),
    datetime.date(2026, 4, 14),
    datetime.date(2026, 4, 21),
    datetime.date(2026, 5, 1),
    datetime.date(2026, 6, 29),
    datetime.date(2026, 8, 15),
    datetime.date(2026, 10, 2),
    datetime.date(2026, 10, 25),
    datetime.date(2026, 10, 26),
    datetime.date(2026, 11, 12),
    datetime.date(2026, 12, 25),
}


def is_working_day(d: datetime.date):
    return d.weekday() < 5 and d not in MARKET_HOLIDAYS


def fetch_date(date: datetime.date, session: requests.Session):
    ymd = date.strftime('%Y%m%d')
    zip_name = f'BhavCopy_NSE_FO_0_0_0_{ymd}_F_0000.csv.zip'
    url = f'https://nsearchives.nseindia.com/content/fo/{zip_name}'

    csv_name = f'fo{ymd}bhav.csv'
    local_path = os.path.join(OUTPUT_DIR, csv_name)
    if os.path.exists(local_path):
        print(f'Already downloaded: {csv_name}')
        return True

    try:
        resp = session.get(url, headers=HEADERS, timeout=30)
        if resp.status_code != 200:
            print(f'No data for {date} (status={resp.status_code})')
            return False

        z = zipfile.ZipFile(BytesIO(resp.content))
        candidates = [n for n in z.namelist() if n.lower().endswith('.csv')]
        if not candidates:
            print(f'No CSV member in zip for {date}')
            return False

        # Usually the first file
        inner_name = candidates[0]
        data = z.read(inner_name)
        with open(local_path, 'wb') as f:
            f.write(data)

        print(f'Downloaded: {csv_name}')
        return True
    except Exception as e:
        print(f'Error fetching {url}: {e}')
        return False


def main():
    session = requests.Session()
    session.headers.update(HEADERS)

    # seed cookies and headers by visiting NSE
    try:
        session.get('https://www.nseindia.com', timeout=20)
    except Exception as e:
        print('Warning: Could not access NSE homepage', e)

    cur = START_DATE
    downloaded = 0
    skipped = 0

    while cur <= END_DATE:
        if not is_working_day(cur):
            skipped += 1
            cur += datetime.timedelta(days=1)
            continue

        ok = fetch_date(cur, session)
        if ok:
            downloaded += 1
        else:
            skipped += 1

        # Avoid getting blocked by NSE
        time.sleep(1.5)
        cur += datetime.timedelta(days=1)

    # Verify last 10 business days are present
    last_10_days = []
    check_date = END_DATE
    while len(last_10_days) < 10:
        check_date -= datetime.timedelta(days=1)
        if is_working_day(check_date):
            last_10_days.append(check_date)

    print('\nCheck last 10 working days:')
    all_good = True
    for d in reversed(last_10_days):
        fname = f'fo{d.strftime("%d%m%Y")}bhav.csv'
        exists = os.path.exists(os.path.join(OUTPUT_DIR, fname))
        print(f'{d:%Y-%m-%d}:', 'OK' if exists else 'MISSING')
        if not exists:
            all_good = False

    print('\nSummary: downloaded=', downloaded, 'skipped=', skipped)
    if all_good:
        print('All last 10 business days are present')
    else:
        print('Some of the last 10 business days are missing; run script again.')


if __name__ == '__main__':
    main()

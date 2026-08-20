import os
import datetime
import time
import requests
import zipfile
from io import BytesIO

OUTPUT_DIR = r"D:\FNO DATA\FUTUERE DATA"
os.makedirs(OUTPUT_DIR, exist_ok=True)

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36',
    'Accept': '*/*',
    'Accept-Language': 'en-US,en;q=0.9',
    'Referer': 'https://www.nseindia.com',
}

START_DATE = datetime.date(2026, 1, 1)
END_DATE = datetime.date.today()


def download_fno_bhav(date: datetime.date) -> bool:
    ymd = date.strftime('%Y%m%d')
    filename = f'fo{ymd}bhav.csv'
    destination = os.path.join(OUTPUT_DIR, filename)

    if os.path.exists(destination):
        print(f'Skipping existing {filename}')
        return True

    url = f'https://nsearchives.nseindia.com/content/fo/BhavCopy_NSE_FO_0_0_0_{ymd}_F_0000.csv.zip'

    try:
        r = requests.get(url, headers=HEADERS, timeout=30)
    except Exception as e:
        print(f'Error fetching {date} - {e}')
        return False

    if r.status_code != 200:
        print(f'No record for {date} (status {r.status_code})')
        return False

    try:
        z = zipfile.ZipFile(BytesIO(r.content))
        member = z.namelist()[0]
        df_bytes = z.read(member)
        with open(destination, 'wb') as f:
            f.write(df_bytes)
        print(f'Downloaded {filename}')
        return True
    except Exception as e:
        print(f'Failed to unzip/save {date}: {e}')
        return False


def main():
    current = START_DATE
    success = 0
    failed = 0

    while current <= END_DATE:
        if current.weekday() >= 5:
            current += datetime.timedelta(days=1)
            continue

        ok = download_fno_bhav(current)
        if ok:
            success += 1
        else:
            failed += 1

        current += datetime.timedelta(days=1)
        time.sleep(1.5)

    print(f'Completed. success={success}, failed={failed}')


if __name__ == '__main__':
    main()

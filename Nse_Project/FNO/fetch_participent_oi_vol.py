import argparse
import datetime as dt
import io
import os
import time

import pandas as pd
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


def format_ddmmyyyy(date_obj: dt.date) -> str:
    return date_obj.strftime('%d%m%Y')


def format_yyyymmdd(date_obj: dt.date) -> str:
    return date_obj.strftime('%Y%m%d')


def parse_date(value: str) -> dt.date:
    return dt.datetime.strptime(value, '%Y-%m-%d').date()


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
    
    response = session.get(url, timeout=30)
    if response.status_code != 200:
        print(f"డౌన్‌లోడ్ విఫలమైంది (Status: {response.status_code}) - బహుశా ఆ రోజు డేటా ఇంకా రాలేదు.")
        return

    content = response.content
    text_preview = content[:200].decode('utf-8', errors='replace').strip()
    if text_preview.startswith('{') or text_preview.lower().startswith('<html') or 'error' in text_preview.lower():
        print(f"తప్పు ఫార్మాట్ ఫైల్ వచ్చింది: {file_name}")
        return

    try:
        df = pd.read_csv(io.BytesIO(content))
    except Exception as e:
        print(f"CSV పార్సింగ్ ఎర్రర్: {e}")
        return

    df.to_csv(output_path, index=False)
    print(f"సక్సెస్‌ఫుల్‌గా సేవ్ చేయబడింది: {output_path}")
    time.sleep(1)


def fetch_participant_data(date_obj: dt.date) -> None:
    session = requests.Session()
    session.headers.update(HEADERS)
    
    # కుకీస్ (Cookies) జనరేట్ చేయడం కోసం ముందుగా ప్రధాన పేజీని లోడ్ చేయడం
    try:
        session.get(SESSION_PRIME_URL, timeout=20)
    except Exception:
        pass

    print(f"\n--- {date_obj} తేదీకి సంబంధించిన ఫైల్స్ కోసం వెతుకుతోంది ---")
    
    # 1. మొదట Open Interest డేటాను డౌన్‌లోడ్ చేస్తుంది
    download_report(date_obj, URL_TEMPLATE_OI, OUTPUT_DIR_OI, "participant_oi", session)

    # 2. ఆ వెంటనే Trading Volume డేటాను డౌన్‌లోడ్ చేస్తుంది
    download_report(date_obj, URL_TEMPLATE_VOL, OUTPUT_DIR_VOL, "participant_vol", session)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Download NSE participant OI and Volume CSVs for a given date')
    parser.add_argument('--date', type=str, default=dt.date.today().strftime('%Y-%m-%d'),
                        help='Date to download in YYYY-MM-DD format')
    args = parser.parse_args()
    date_obj = parse_date(args.date)
    
    fetch_participant_data(date_obj)

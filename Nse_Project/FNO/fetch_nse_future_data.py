import os
import sys
import time
import requests
import pandas as pd
from datetime import datetime, timedelta

# Settings
OUTPUT_DIR = r"D:\FNO DATA\Nifty Future"
os.makedirs(OUTPUT_DIR, exist_ok=True)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate",
    "Referer": "https://www.nseindia.com/",
    "X-Requested-With": "XMLHttpRequest",
}

LIVE_DERIVATIVES_URL = "https://www.nseindia.com/api/liveEquity-derivatives?index=nse50_fut"

HISTORICAL_CSV_URL_TEMPLATE = (
    "https://www.nseindia.com/api/NextApi/apiClient/GetQuoteApi?"
    "functionName=getDerivativesHistoricalData&symbol=NIFTY&instrumentType=FUTIDX&year=&expiryDate={expiry}"
    "&strikePrice=&optionType=&fromDate={from_date}&toDate={to_date}&csv=true"
)
HISTORICAL_DATE_COL = "FH_TIMESTAMP"
REQUEST_DELAY = 2          # polite pause between expiry requests
MAX_RETRIES = 3
BACKOFF_SECONDS = 2.0
RETRYABLE_STATUS = {403, 408, 429, 500, 502, 503, 504}


def _get_with_retry(session, url, timeout=30):
    """GET with exponential backoff for NSE's flaky API endpoints."""
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


def get_session():
    session = requests.Session()
    session.headers.update(HEADERS)
    try:
        session.get("https://www.nseindia.com", timeout=15)
    except Exception:
        pass
    return session


def fetch_live_derivatives(session):
    resp = _get_with_retry(session, LIVE_DERIVATIVES_URL, timeout=20)
    resp.raise_for_status()
    payload = resp.json()
    if not isinstance(payload, dict) or not payload.get("data"):
        raise ValueError("Live derivatives payload missing 'data' list.")
    return payload


def select_three_expiries(df):
    if "expiryDate" not in df.columns:
        return []
    df = df.copy()
    df["expiry_parsed"] = pd.to_datetime(df["expiryDate"], format="%d-%b-%Y", errors="coerce")
    df = df[df["expiry_parsed"].notna()]
    unique = df[["expiryDate", "expiry_parsed"]].drop_duplicates(subset=["expiryDate"])
    result = unique.sort_values("expiry_parsed")["expiryDate"].head(3).tolist()
    return result


def get_history_csv_path(expiry):
    return os.path.join(OUTPUT_DIR, f"nifty_futidx_historical_{_safe_expiry_filename(expiry)}.csv")


def parse_date(date_str):
    return datetime.strptime(date_str, "%d-%m-%Y").date()


def read_existing_history(csv_path):
    if not os.path.exists(csv_path):
        return None
    try:
        df = pd.read_csv(csv_path, parse_dates=[HISTORICAL_DATE_COL], dayfirst=True)
    except Exception:
        return None
    if HISTORICAL_DATE_COL not in df.columns:
        return None
    df[HISTORICAL_DATE_COL] = pd.to_datetime(df[HISTORICAL_DATE_COL], errors="coerce")
    return df.sort_values(HISTORICAL_DATE_COL)


def fetch_historical_data(session, expiry, from_date, to_date):
    if from_date > to_date:
        return pd.DataFrame()

    url = HISTORICAL_CSV_URL_TEMPLATE.format(
        expiry=expiry,
        from_date=from_date.strftime("%d-%m-%Y"),
        to_date=to_date.strftime("%d-%m-%Y"),
    )
    resp = _get_with_retry(session, url, timeout=30)
    if resp.status_code != 200:
        return pd.DataFrame()
    try:
        payload = resp.json()
    except ValueError as exc:
        print(f"   -> Non-JSON response for expiry {expiry}: {exc}")
        return pd.DataFrame()
    if not isinstance(payload, list) or len(payload) == 0:
        return pd.DataFrame()
    df = pd.DataFrame(payload)
    if HISTORICAL_DATE_COL not in df.columns:
        return pd.DataFrame()
    df[HISTORICAL_DATE_COL] = pd.to_datetime(df[HISTORICAL_DATE_COL], errors="coerce")
    return df.dropna(subset=[HISTORICAL_DATE_COL])


def append_history(csv_path, existing_df, new_df):
    if existing_df is None or existing_df.empty:
        out = new_df
    else:
        out = pd.concat([existing_df, new_df], ignore_index=True)
        if HISTORICAL_DATE_COL in out.columns:
            out = out.drop_duplicates(subset=[HISTORICAL_DATE_COL], keep="last")
            out = out.sort_values(HISTORICAL_DATE_COL)
    out.to_csv(csv_path, index=False)
    return out


def _safe_expiry_filename(expiry: str) -> str:
    return expiry.replace("-", "")


def main():
    session = get_session()
    try:
        live_payload = fetch_live_derivatives(session)
    except (requests.RequestException, ValueError) as exc:
        print(f"Could not fetch live derivatives data: {exc}")
        return
    live_df = pd.DataFrame(live_payload.get("data", []))

    expiries = select_three_expiries(live_df)
    if not expiries:
        print("Could not detect expiry dates from live data.")
        return

    requested_from_date = parse_date(sys.argv[1]) if len(sys.argv) > 1 else datetime(2020, 1, 1).date()
    requested_to_date = parse_date(sys.argv[2]) if len(sys.argv) > 2 else datetime.now().date()

    for expiry in expiries:
        csv_path = get_history_csv_path(expiry)
        existing_df = read_existing_history(csv_path)
        if existing_df is not None and not existing_df.empty:
            last_date = existing_df[HISTORICAL_DATE_COL].max().date()
            from_date = last_date + timedelta(days=1)
            if from_date > requested_to_date:
                print(f"Expiry {expiry} is already up to date through {requested_to_date.strftime('%d-%m-%Y')}. Skipping.")
                continue
            print(f"Expiry {expiry} exists. Fetching missing data from {from_date.strftime('%d-%m-%Y')} to {requested_to_date.strftime('%d-%m-%Y')}")
        else:
            from_date = requested_from_date
            print(f"Expiry {expiry} file not found. Downloading full range from {from_date.strftime('%d-%m-%Y')} to {requested_to_date.strftime('%d-%m-%Y')}")

        try:
            new_df = fetch_historical_data(session, expiry, from_date, requested_to_date)
            if new_df.empty:
                print(f"No new historical data for expiry {expiry} from {from_date.strftime('%d-%m-%Y')} to {requested_to_date.strftime('%d-%m-%Y')}")
                continue
            combined_df = append_history(csv_path, existing_df, new_df)
            print(f"Updated CSV for expiry {expiry}: {csv_path} ({len(combined_df)} rows)")
        except Exception as exc:
            print(f"Failed to update historical CSV for expiry {expiry}: {exc}")
        time.sleep(REQUEST_DELAY)


if __name__ == '__main__':
    main()

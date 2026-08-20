import os
import sys
import time
import json
import requests
import pandas as pd
from datetime import datetime, timedelta
from io import StringIO

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
DETAIL_URL_TEMPLATE = (
    "https://www.nseindia.com/api/NextApi/apiClient/GetQuoteApi?"
    "functionName=getSymbolDerivativesData&symbol=NIFTY&identifier={identifier}"
    "&instrumentType=FUT&expiryDt={expiry}"
)

HISTORICAL_CSV_URL_TEMPLATE = (
    "https://www.nseindia.com/api/NextApi/apiClient/GetQuoteApi?"
    "functionName=getDerivativesHistoricalData&symbol=NIFTY&instrumentType=FUTIDX&year=&expiryDate={expiry}"
    "&strikePrice=&optionType=&fromDate={from_date}&toDate={to_date}&csv=true"
)
HISTORICAL_DATE_COL = "FH_TIMESTAMP"
REQUEST_DELAY = 2
MAX_RETRIES = 3
RETRY_DELAY = 5


def get_session():
    session = requests.Session()
    session.headers.update(HEADERS)
    try:
        session.get("https://www.nseindia.com", timeout=15)
    except Exception:
        pass
    return session


def fetch_live_derivatives(session):
    resp = session.get(LIVE_DERIVATIVES_URL, timeout=20)
    resp.raise_for_status()
    return resp.json()


def select_three_expiries(df):
    if "expiryDate" not in df.columns:
        return []
    df = df.copy()
    df["expiry_parsed"] = pd.to_datetime(df["expiryDate"], format="%d-%b-%Y", errors="coerce")
    df = df[df["expiry_parsed"].notna()]
    unique = df[["expiryDate", "expiry_parsed"]].drop_duplicates(subset=["expiryDate"])
    result = unique.sort_values("expiry_parsed")["expiryDate"].head(3).tolist()
    return result


def choose_identifier_for_expiry(df, expiry):
    candidates = df[df["expiryDate"] == expiry]
    if candidates.empty:
        return None
    if "instrumentType" in candidates.columns:
        filt = candidates[candidates["instrumentType"].astype(str).str.upper() == "FUTIDX"]
        if not filt.empty:
            candidates = filt
    identifiers = candidates["identifier"].dropna().unique()
    return identifiers[0] if len(identifiers) > 0 else None


def fetch_detail_for_identifier(session, identifier, expiry):
    url = DETAIL_URL_TEMPLATE.format(identifier=identifier, expiry=expiry)
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = session.get(url, timeout=20)
            resp.raise_for_status()
            return resp.json()
        except requests.HTTPError as exc:
            if exc.response is not None and exc.response.status_code == 503 and attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY)
                continue
            raise
        except Exception:
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY)
                continue
            raise


def payload_to_df(payload, expiry, identifier):
    if not isinstance(payload, dict):
        return pd.DataFrame()
    data = payload.get("data")
    if data is None:
        return pd.DataFrame()
    if isinstance(data, dict):
        data = [data]
    df = pd.DataFrame(data)
    df["expiryDate"] = expiry
    df["identifier"] = identifier
    return df


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
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = session.get(url, timeout=30)
            resp.raise_for_status()
            payload = resp.json()
            if not isinstance(payload, list) or len(payload) == 0:
                return pd.DataFrame()
            df = pd.DataFrame(payload)
            if HISTORICAL_DATE_COL in df.columns:
                df[HISTORICAL_DATE_COL] = pd.to_datetime(df[HISTORICAL_DATE_COL], errors="coerce")
            return df
        except Exception as exc:
            print(f"Attempt {attempt} failed for historical fetch expiry {expiry}: {exc}")
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY)
                continue
            raise


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


def save_expiry_files(payload, expiry, identifier):
    safe_name = _safe_expiry_filename(expiry)
    json_path = os.path.join(OUTPUT_DIR, f"nifty_future_{safe_name}.json")
    csv_path = os.path.join(OUTPUT_DIR, f"nifty_future_{safe_name}.csv")

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)

    df = payload_to_df(payload, expiry, identifier)
    if not df.empty:
        df.to_csv(csv_path, index=False)
        return json_path, csv_path

    return json_path, None


def main():
    session = get_session()
    live_payload = fetch_live_derivatives(session)
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


if __name__ == '__main__':
    main()

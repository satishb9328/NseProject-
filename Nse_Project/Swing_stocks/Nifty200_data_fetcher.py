import os
import sys
import time
import datetime
import json
import urllib.parse
from pathlib import Path

import requests
import pandas as pd

# Console hardening: this script prints emoji/box-drawing characters and is now a
# GATING pipeline step, so a cp1252 console must not crash it with
# UnicodeEncodeError (which would return a non-zero code and halt the launcher).
for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

# =====================================================================
# 1. CONFIGURATION  (BASE_DIR-relative — no hardcoded absolute paths)
# =====================================================================

# ...\MyProject2\Nse_Project\Swing_stocks
BASE_DIR = Path(__file__).resolve().parent
# ...\MyProject2\Nse_Project
PROJECT_ROOT = BASE_DIR.parent
# ...\MyProject2
REPO_ROOT = PROJECT_ROOT.parent

TOKEN_DIR = PROJECT_ROOT / "Brokers" / "upstox"

# అన్ని తెలిసిన టోకెన్ లొకేషన్స్ (freshest valid one wins).
# Mirrors the convention used by Live_dashboard/upstox_live_feed.py,
# Swing_ML/ml_stockdata_collector.py and test_upstox_token.py.
TOKEN_FILE_PATHS = [
    TOKEN_DIR / "upstox_v3_token.json",
    TOKEN_DIR / "upstox" / "upstox_token.json",
    TOKEN_DIR / "upstox_login_details.json",
    REPO_ROOT / "upstox" / "upstox_token.json",
]

# Shared data contract with nifty_advanced_strategy.py.
# Override for both processes at once via the UPSTOX_HISTORY_DIR env var.
DEFAULT_HISTORY_DIR = r"D:\Api data\ML_HISTO_DATA\Upstox_History"
SAVE_PATH = os.environ.get("UPSTOX_HISTORY_DIR", DEFAULT_HISTORY_DIR)

# Process exit codes (consumed by start_trading_system.bat)
EXIT_OK = 0
EXIT_INIT_FAILED = 1
EXIT_NO_TOKEN = 2
EXIT_TOKEN_EXPIRED = 3
EXIT_ALL_FAILED = 4

def _read_token_file(path: Path) -> dict | None:
    """ఒక టోకెన్ JSON ఫైల్‌ను చదివి, dict గా return చేస్తుంది."""
    try:
        with path.open("r", encoding="utf-8") as file:
            data = json.load(file)
        return data if isinstance(data, dict) else None
    except Exception as exc:
        print(f"   [WARN] Could not parse {path.name}: {exc}")
        return None


def _token_expiry(payload: dict):
    """'expires_at' ను datetime గా return చేస్తుంది (తెలియకపోతే None)."""
    raw = payload.get("expires_at")
    if not raw:
        return None
    try:
        return datetime.datetime.fromisoformat(str(raw))
    except Exception:
        return None


def get_access_token():
    """తెలిసిన అన్ని లొకేషన్స్ నుండి చెల్లుబాటు అయ్యే (non-expired) టోకెన్‌ను వెతుకుతుంది.

    Returns
    -------
    (token, path, expiry) on success, else (None, None, None).
    Prefers the newest *unexpired* token; falls back to the newest expired one
    only to produce a precise error message.
    """
    now = datetime.datetime.now()
    valid, expired, missing = [], [], []

    for path in TOKEN_FILE_PATHS:
        if not path.exists():
            missing.append(path)
            continue

        payload = _read_token_file(path)
        if not payload:
            continue

        token = payload.get("access_token")
        if not token:
            print(f"   [WARN] {path.name} exists but has no 'access_token'.")
            continue

        expiry = _token_expiry(payload)
        # Sort key: prefer explicit expiry, else file mtime.
        sort_key = expiry.timestamp() if expiry else path.stat().st_mtime

        if expiry is not None and now >= expiry:
            expired.append((sort_key, path, token, expiry))
        else:
            valid.append((sort_key, path, token, expiry))

    if valid:
        valid.sort(key=lambda item: item[0], reverse=True)
        _, path, token, expiry = valid[0]
        exp_txt = expiry.strftime("%Y-%m-%d %H:%M:%S") if expiry else "unknown"
        print("✅ Upstox token loaded successfully!")
        print(f"   Source : {path}")
        print(f"   Expires: {exp_txt}\n")
        return token, path, expiry

    # ── Nothing usable — emit an actionable diagnosis ────────────────────
    print("\n" + "!" * 70)
    print("❌ FATAL: No VALID Upstox access token found.")
    print("!" * 70)

    if expired:
        expired.sort(key=lambda item: item[0], reverse=True)
        print("   Found token file(s), but ALL are EXPIRED:")
        for _, path, _, expiry in expired:
            exp_txt = expiry.strftime("%Y-%m-%d %H:%M:%S") if expiry else "unknown"
            print(f"     - {path}  (expired at {exp_txt})")
        print("\n   ACTION: Regenerate the token, then re-run:")
        print("     python Nse_Project\\Brokers\\upstox\\get_upstox_token_manual.py")
    else:
        print("   No token file exists at any known location:")
        for path in missing:
            print(f"     - {path}")
        print("\n   ACTION: Generate a token first:")
        print("     python Nse_Project\\Brokers\\upstox\\get_upstox_token_manual.py")

    print("!" * 70 + "\n")
    return None, None, None

# =====================================================================
# 2. HELPER FUNCTIONS
# =====================================================================

def get_nifty200_symbols():
    """NSE అఫీషియల్ వెబ్‌సైట్ నుండి లేటెస్ట్ NIFTY 200 స్టాక్స్ లిస్ట్ తీసుకునే ఫంక్షన్."""
    print("📥 Fetching NIFTY 200 list from NSE...")
    try:
        url = "https://archives.nseindia.com/content/indices/ind_nifty200list.csv"
        df = pd.read_csv(url)
        symbols = df['Symbol'].tolist()
        print(f"✅ Successfully loaded {len(symbols)} NIFTY 200 stocks.\n")
        return symbols
    except Exception as e:
        print(f"❌ Error fetching NIFTY 200 list: {e}")
        return []

def get_upstox_instrument_dict():
    """Upstox మాస్టర్ ఫైల్ నుండి అన్ని స్టాక్స్ యొక్క Instrument Keys ని తీసుకురావడం."""
    print("📥 Downloading Upstox Master Instrument Keys (This takes a few seconds)...")
    try:
        url = "https://assets.upstox.com/market-quote/instruments/exchange/NSE.csv.gz"
        df = pd.read_csv(url)
        instrument_dict = pd.Series(df.instrument_key.values, index=df.tradingsymbol).to_dict()
        print("✅ Upstox Keys loaded successfully!\n")
        return instrument_dict
    except Exception as e:
        print(f"❌ Error fetching instrument keys: {e}")
        return {}

def fetch_stock_5m_data(symbol, instrument_key, headers, existing_last_date=None):
    """అవసరమైన డేటాని మాత్రమే (Incremental) ఫెచ్ చేసే ఫంక్షన్."""
    all_candles = []
    end_date = datetime.date.today()
    
    interval = "1minute" 
    safe_instrument_key = urllib.parse.quote(instrument_key)
    
    # 🔴 డేటా ఎక్కడి నుండి ఫెచ్ చేయాలో నిర్ణయించడం
    if existing_last_date:
        # పాత డేటా ఉంటే, ఆ లాస్ట్ డేట్ నుండి ఈరోజు వరకు...
        final_start_date = existing_last_date.date()
    else:
        # పాత డేటా లేకపోతే, 1 సంవత్సరం (360 రోజులు) వెనక్కి వెళ్తాం
        final_start_date = end_date - datetime.timedelta(days=360)

    # డేటా ఇప్పటికే అప్‌డేట్ అయి ఉంటే తిరిగి పంపేయడం
    if end_date <= final_start_date:
        return pd.DataFrame() # ఎంప్టీ DataFrame పంపిస్తే అప్‌డేట్ అవసరం లేదని అర్థం

    current_end = end_date
    
    # గ్యాప్ కవర్ అయ్యే వరకు 30 రోజుల చొప్పున లూప్ రన్ అవుతుంది
    while current_end > final_start_date:
        current_start = max(current_end - datetime.timedelta(days=30), final_start_date)
        
        url = f"https://api.upstox.com/v2/historical-candle/{safe_instrument_key}/{interval}/{current_end.strftime('%Y-%m-%d')}/{current_start.strftime('%Y-%m-%d')}"
        
        try:
            response = requests.get(url, headers=headers)
            
            if response.status_code == 200:
                data = response.json()
                if 'data' in data and 'candles' in data['data'] and data['data']['candles']:
                    all_candles.extend(data['data']['candles'])
            
            elif response.status_code == 401:
                print(f"   ❌ Token Expired or Invalid! దయచేసి కొత్త టోకెన్ జనరేట్ చేయండి.")
                raise RuntimeError("TOKEN_EXPIRED")
            
            elif response.status_code == 429:
                print(f"   ⚠️ Rate Limit Hit for {symbol}. Sleeping for 2 seconds...")
                time.sleep(2)
            
            elif response.status_code == 400 and "UDAPI1148" in response.text:
                # హిస్టరీ లిమిట్ దాటిపోతే లూప్ ఆపేస్తాం
                break
            else:
                print(f"   ❌ API Error {response.status_code}: {response.text}")
                
        except Exception as e:
            print(f"   ❌ Network error for {symbol}: {e}")
            
        current_end = current_start - datetime.timedelta(days=1)
        time.sleep(0.5)

    if all_candles:
        columns = ['Datetime', 'Open', 'High', 'Low', 'Close', 'Volume', 'OI']
        df = pd.DataFrame(all_candles, columns=columns)
        
        df['Datetime'] = pd.to_datetime(df['Datetime']).dt.tz_localize(None)
        df = df.sort_values(by="Datetime").drop_duplicates(subset=['Datetime'])
        
        df.set_index('Datetime', inplace=True)
        df_5m = df.resample('5min').agg({
            'Open': 'first',
            'High': 'max',
            'Low': 'min',
            'Close': 'last',
            'Volume': 'sum',
            'OI': 'last'
        }).dropna()
        
        df_5m.reset_index(inplace=True)
        return df_5m
    
    return pd.DataFrame() # డేటా ఏమీ రాకపోతే

# =====================================================================
# 3. MAIN EXECUTION ENGINE
# =====================================================================

def main():
    ACCESS_TOKEN, token_path, token_expiry = get_access_token()
    if not ACCESS_TOKEN:
        # 🔴 Non-zero exit so start_trading_system.bat can halt the pipeline.
        return EXIT_NO_TOKEN

    try:
        os.makedirs(SAVE_PATH, exist_ok=True)
    except Exception as exc:
        print(f"❌ Cannot create/access SAVE_PATH '{SAVE_PATH}': {exc}")
        return EXIT_INIT_FAILED

    print(f"📁 Save Directory Ready: {SAVE_PATH}\n")
    
    headers = {
        'Accept': 'application/json',
        'Authorization': f'Bearer {ACCESS_TOKEN}'
    }

    symbols = get_nifty200_symbols()
    upstox_keys = get_upstox_instrument_dict()
    
    if not symbols or not upstox_keys:
        print("❌ Initialization failed (no symbols or no instrument keys). Exiting...")
        return EXIT_INIT_FAILED

    print("🚀 Starting Daily EOD Data Fetch for NIFTY 200...")
    print("=" * 60)
    
    successful = 0
    updated = 0
    up_to_date = 0
    failed = 0
    token_died = False

    for idx, symbol in enumerate(symbols, 1):
        instrument_key = upstox_keys.get(symbol)
        
        if not instrument_key:
            print(f"[{idx:3d}/{len(symbols)}] ⚠️ {symbol:12s} | No Upstox Key Found. Skipping.")
            failed += 1
            continue
            
        filename = os.path.join(SAVE_PATH, f"{symbol}_5m.parquet")
        existing_df = None
        existing_last_date = None
        
        # 🔴 1. లోకల్ ఫైల్ ఉందో లేదో చెక్ చేయడం
        if os.path.exists(filename):
            try:
                existing_df = pd.read_parquet(filename)
                if not existing_df.empty:
                    existing_last_date = existing_df['Datetime'].max()
            except Exception as e:
                print(f"   ⚠️ Could not read existing file for {symbol}: {e}")

        if existing_last_date:
            print(f"[{idx:3d}/{len(symbols)}] ⏳ {symbol:12s} | Updating from {existing_last_date.date()} to Today...")
        else:
            print(f"[{idx:3d}/{len(symbols)}] ⏳ {symbol:12s} | Fetching full 1-Year 5m data...")
            
        # 🔴 2. కొత్త డేటాని మాత్రమే ఫెచ్ చేయడం
        try:
            new_df = fetch_stock_5m_data(symbol, instrument_key, headers, existing_last_date)
        except RuntimeError as e:
            if str(e) == "TOKEN_EXPIRED":
                print("\n🚨 STOPPING PROCESS: Upstox token expired mid-run.")
                print("   Regenerate the token and re-run this fetcher before scanning.")
                token_died = True
                break
            raise
            
        # 🔴 3. డేటాని కలపడం (Merge / Append)
        if new_df is not None and not new_df.empty:
            if existing_df is not None:
                # పాత డేటా + కొత్త డేటా కలుపుతున్నాం
                combined_df = pd.concat([existing_df, new_df])
                # డూప్లికేట్స్ ఉంటే తీసేసి, ఆర్డర్ లో పెడుతున్నాం
                combined_df.drop_duplicates(subset=['Datetime'], keep='last', inplace=True)
                combined_df.sort_values(by="Datetime", inplace=True)
                
                combined_df.to_parquet(filename, index=False)
                print(f"            ✅ Updated! Added {len(new_df)} new candles. Total: {len(combined_df)}")
                updated += 1
            else:
                # మొదటిసారి సేవ్ చేస్తున్నప్పుడు
                new_df.to_parquet(filename, index=False)
                print(f"            ✅ Saved full data ({len(new_df)} candles).")
                successful += 1
        else:
            if existing_last_date and (datetime.date.today() == existing_last_date.date() or datetime.date.today().weekday() >= 5):
                print(f"            ⚡ Already Up-to-Date! (No new market data)")
                up_to_date += 1
            else:
                print(f"            ❌ Failed to fetch new data.")
                failed += 1
            
    print("=" * 60)
    print("🎉 EOD DATA FETCH COMPLETED!")
    print(f"📊 Full 1-Year Downloads : {successful} stocks")
    print(f"🔄 Incrementally Updated : {updated} stocks")
    print(f"⚡ Already Up-to-Date    : {up_to_date} stocks")
    print(f"⚠️ Failed / Skipped      : {failed} stocks")
    print(f"📁 All files saved in    : {SAVE_PATH}")
    print("=" * 60)

    # ── FRESHNESS VERIFICATION ──────────────────────────────────────────
    # The scanner rejects data older than MAX_DATA_AGE_DAYS, so prove here
    # that the corpus actually advanced instead of silently reporting success.
    newest = None
    try:
        for name in os.listdir(SAVE_PATH):
            if not name.endswith("_5m.parquet"):
                continue
            try:
                sample = pd.read_parquet(os.path.join(SAVE_PATH, name), columns=["Datetime"])
                if sample.empty:
                    continue
                last_ts = pd.to_datetime(sample["Datetime"], errors="coerce").max()
                if pd.notna(last_ts) and (newest is None or last_ts > newest):
                    newest = last_ts
            except Exception:
                continue
    except Exception as exc:
        print(f"⚠️ Could not verify corpus freshness: {exc}")

    if newest is not None:
        age_days = (datetime.date.today() - newest.date()).days
        print(f"🕒 Newest candle in corpus : {newest}  (age: {age_days} calendar day(s))")
    print("=" * 60)

    # ── EXIT CODE ───────────────────────────────────────────────────────
    if token_died:
        return EXIT_TOKEN_EXPIRED

    # Nothing written AND nothing already current => hard failure.
    if (successful + updated + up_to_date) == 0:
        print("❌ No symbol was downloaded, updated, or already current.")
        return EXIT_ALL_FAILED

    return EXIT_OK


if __name__ == "__main__":
    # 🔴 Propagate the status to the shell so the launcher can gate the scanner.
    sys.exit(main())
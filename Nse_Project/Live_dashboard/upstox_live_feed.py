import asyncio
import csv
import gzip
import io
import json
import logging
import os
import pathlib
import signal
import sys
import urllib.parse
from datetime import datetime, date, timedelta, timezone
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from Nse_Project.graceful_shutdown import is_shutdown_requested, setup_signal_handlers

import pandas as pd
import redis
import requests
from upstox_client import ApiClient, Configuration
from upstox_client.feeder.market_data_streamer_v3 import MarketDataStreamerV3

# Resolve the absolute path to the repository root (same level as manage.py)
BASE_DIR = pathlib.Path(__file__).resolve().parent.parent.parent
TOKEN_DIR = BASE_DIR / 'Nse_Project' / 'Brokers' / 'upstox'
JSON_PATH = os.path.join(BASE_DIR, 'live_options_data.json')
BASELINE_PATH = os.path.join(BASE_DIR, 'live_options_baseline.json')
MASTER_INSTRUMENTS_URL = "https://assets.upstox.com/market-quote/instruments/exchange/NSE.csv.gz"
REDIS_URL = "redis://127.0.0.1:6379/0"
REDIS_DATA_KEY = "live_options_data"
REDIS_CHANNEL = "gamma:live_options_updated"

TOKEN_FILE_PATHS = [
    TOKEN_DIR / "upstox_v3_token.json",
]

LOG_FORMAT = "%(asctime)s | %(levelname)-8s | %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT, datefmt="%Y-%m-%d %H:%M:%S")
logger = logging.getLogger("LiveUpstoxFeed")
redis_client = redis.Redis.from_url(
    REDIS_URL,
    decode_responses=True,
    socket_connect_timeout=1,
    socket_timeout=1,
)

state_lock = asyncio.Lock()
live_state = {
    "updated_at": None,
    "expiry_date": None,
    "baseline_date": None,
    "baseline_source": None,
    "instrument_data": {},
}
baseline_data: dict[str, object] = {}
INSTRUMENT_METADATA: dict[str, dict] = {}


def read_access_token() -> str | None:
    for path in TOKEN_FILE_PATHS:
        try:
            if path.exists():
                with path.open("r", encoding="utf-8") as token_file:
                    payload = json.load(token_file)
                    token = payload.get("access_token")
                    if token:
                        logger.info(f"✅ Loaded access token from {path}")
                        return token
                    logger.warning(f"Token file exists but no access_token field: {path}")
        except Exception as exc:
            logger.warning(f"Failed to read token file {path}: {exc}")
    logger.error("No Upstox token JSON file found in Brokers/upstox.")
    return None


def parse_instruments_csv(compressed_bytes: bytes) -> list[dict]:
    with gzip.open(io.BytesIO(compressed_bytes), mode="rt", encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile)
        return [row for row in reader]


def find_nifty_weekly_option_keys() -> tuple[date, list[str]]:
    try:
        logger.info("📥 Downloading Upstox NSE instrument master file...")
        response = requests.get(MASTER_INSTRUMENTS_URL, timeout=20)
        response.raise_for_status()
        content = gzip.decompress(response.content)
        df = pd.read_csv(io.BytesIO(content))
        logger.info("✅ Upstox instrument master file downloaded successfully.")
        logger.info("🔍 Master file columns: %s", list(df.columns[:20]))
        logger.info("📄 Master file preview:\n%s", df.head(5).to_dict(orient='records'))
    except Exception as exc:
        logger.error(f"❌ Unable to load Upstox instrument master data: {exc}")
        return None, []

    # Keep original column names for robust mapping
    columns_lower = {col.lower().strip(): col for col in df.columns}
    symbol_col = columns_lower.get('tradingsymbol') or next((col for key, col in columns_lower.items() if 'tradingsymbol' in key or key == 'symbol'), None)
    type_col = columns_lower.get('instrument_type') or next((col for key, col in columns_lower.items() if 'instrument_type' in key or key == 'type'), None)
    expiry_col = columns_lower.get('expiry') or next((col for key, col in columns_lower.items() if 'expiry' in key), None)
    strike_col = columns_lower.get('strike') or next((col for key, col in columns_lower.items() if 'strike' in key), None)
    option_type_col = columns_lower.get('option_type') or next((col for key, col in columns_lower.items() if 'option_type' in key or key == 'optiontype'), None)
    key_col = columns_lower.get('instrument_key') or next((col for key, col in columns_lower.items() if 'instrument_key' in key or 'instrumentkey' in key), None)

    if not symbol_col or not type_col or not expiry_col or not strike_col or not key_col:
        logger.error("❌ Upstox instrument master file is missing expected columns. Available columns: %s", list(df.columns[:20]))
        return None, []

    df[symbol_col] = df[symbol_col].astype(str).str.strip()
    df[type_col] = df[type_col].astype(str)
    df[expiry_col] = pd.to_datetime(df[expiry_col], errors='coerce')

    symbol_upper = df[symbol_col].str.upper().fillna('')
    df = df[
        symbol_upper.str.startswith('NIFTY') &
        ~symbol_upper.str.startswith('BANKNIFTY') &
        df[type_col].str.upper().str.contains('OPTIDX', na=False)
    ]
    if df.empty:
        logger.error("❌ No NIFTY OPTIDX records found in the instrument master file. Sample symbols: %s; sample types: %s",
                     df[symbol_col].dropna().unique()[:10].tolist(),
                     df[type_col].dropna().unique()[:10].tolist())
        return None, []

    df['expiry'] = df[expiry_col].dt.date
    today = datetime.now(timezone(timedelta(hours=5, minutes=30))).date()
    df = df[df['expiry'] >= today]
    if df.empty:
        logger.error("❌ No NIFTY OPTIDX expiry date found greater than or equal to today. Candidate expiries: %s",
                     sorted(df['expiry'].dropna().unique().tolist()))
        return None, []

    expiry_date = df['expiry'].min()
    filtered = df[df['expiry'] == expiry_date]
    INSTRUMENT_METADATA.clear()

    for _, row in filtered.iterrows():
        key = str(row[key_col])
        strike = row.get(strike_col)
        if pd.isna(strike):
            continue
        option_side = None
        if option_type_col and row.get(option_type_col) is not None:
            option_side = str(row.get(option_type_col)).upper()
        INSTRUMENT_METADATA[key] = {
            'strike': int(strike) if pd.notna(strike) and float(strike).is_integer() else float(strike),
            'option_type': option_side,
            'tradingsymbol': str(row[symbol_col]),
            'expiry': row['expiry'],
        }

    instrument_keys = [k for k in filtered[key_col].dropna().astype(str).unique().tolist() if k in INSTRUMENT_METADATA]
    instrument_keys.sort(key=lambda key: INSTRUMENT_METADATA[key]['strike'])

    logger.info(f"✅ Found {len(instrument_keys)} NIFTY option keys for expiry {expiry_date}")
    return expiry_date, instrument_keys


def flatten_payload(payload: object, prefix: str = "") -> dict:
    flattened = {}
    if isinstance(payload, dict):
        for key, value in payload.items():
            name = f"{prefix}.{key}" if prefix else str(key)
            flattened.update(flatten_payload(value, name))
    elif isinstance(payload, list):
        for index, value in enumerate(payload):
            name = f"{prefix}.{index}" if prefix else str(index)
            flattened.update(flatten_payload(value, name))
    else:
        flattened[prefix] = payload
    return flattened


def atomic_write_json(path: str, data: dict) -> None:
    tmp_path = f"{path}.tmp"
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(tmp_path, "w", encoding="utf-8") as fh:
        json.dump(data, fh, indent=2, default=str)
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp_path, path)


def get_local_today() -> date:
    return datetime.now(timezone(timedelta(hours=5, minutes=30))).date()


def get_yesterday_date() -> date:
    return get_local_today() - timedelta(days=1)


def is_baseline_for_yesterday(baseline_date: str | None) -> bool:
    if not isinstance(baseline_date, str):
        return False
    try:
        return date.fromisoformat(baseline_date) == get_yesterday_date()
    except ValueError:
        return False


def load_baseline_data() -> dict:
    if not os.path.exists(BASELINE_PATH):
        return {}
    try:
        with open(BASELINE_PATH, "r", encoding="utf-8") as fh:
            data = json.load(fh)
        if not isinstance(data, dict):
            return {}
        return data
    except Exception as exc:
        logger.warning(f"⚠️ Failed to read baseline file: {exc}")
        return {}


def save_baseline_data(data: dict) -> None:
    try:
        atomic_write_json(BASELINE_PATH, data)
        logger.info(f"✅ Saved baseline OI data to {BASELINE_PATH}")
    except Exception as exc:
        logger.warning(f"⚠️ Failed to write baseline file: {exc}")


def get_baseline_oi_for_instrument(instrument_key: str) -> float | None:
    if not baseline_data:
        return None
    instrument_info = baseline_data.get("instrument_data", {}).get(instrument_key)
    if isinstance(instrument_info, dict):
        oi = instrument_info.get("oi")
        if isinstance(oi, (int, float)):
            return float(oi)
    return None


def parse_historical_oi_row(row):
    if isinstance(row, (list, tuple)) and len(row) > 0:
        return row[-1]
    if isinstance(row, dict):
        for key in ("oi", "openInterest", "open_interest"):
            if key in row:
                return row[key]
    return None


def fetch_yesterday_historical_oi(instrument_key: str, headers: dict) -> float | None:
    yesterday = get_yesterday_date().strftime("%Y-%m-%d")
    encoded_key = urllib.parse.quote(str(instrument_key), safe="")
    for interval in ("day", "1day", "daily"):
        url = f"https://api.upstox.com/v2/historical-candle/{encoded_key}/{interval}/{yesterday}/{yesterday}"
        try:
            response = requests.get(url, headers=headers, timeout=15)
            if response.status_code != 200:
                continue
            payload = response.json()
            candles = None
            if isinstance(payload, dict):
                candles = payload.get("data", {}).get("candles") or payload.get("candles")
            if not candles:
                continue
            last_row = candles[-1]
            oi_value = parse_historical_oi_row(last_row)
            if isinstance(oi_value, (int, float)):
                return float(oi_value)
            if isinstance(oi_value, str):
                try:
                    return float(oi_value.replace(",", ""))
                except ValueError:
                    continue
        except Exception:
            continue
    return None


def fetch_yesterday_baseline(instrument_keys: list[str]) -> dict:
    token = read_access_token()
    if not token:
        return {}

    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
    }
    baseline_date = get_yesterday_date().isoformat()
    output = {"baseline_date": baseline_date, "source": "upstox_historical", "instrument_data": {}}
    for instrument_key in instrument_keys:
        if not instrument_key:
            continue
        oi = fetch_yesterday_historical_oi(instrument_key, headers)
        if oi is not None:
            output["instrument_data"][instrument_key] = {"oi": oi}
        fetched_count = len(output["instrument_data"])
        if fetched_count > 0 and fetched_count % 20 == 0:
            logger.info(f"Fetched baseline OI for {fetched_count} instruments...")

    fetched_count = len(output["instrument_data"])
    if fetched_count == 0:
        logger.warning(
            "No historical OI was returned for %d instruments for %s; "
            "live COI will use the current-session baseline.",
            len(instrument_keys),
            baseline_date,
        )
    else:
        logger.info("Fetched baseline OI for %d instruments.", fetched_count)
    return output


def initialize_baseline(instrument_keys: list[str]) -> None:
    global baseline_data
    baseline = load_baseline_data()
    if baseline and is_baseline_for_yesterday(baseline.get("baseline_date")):
        baseline_data = baseline
        live_state["baseline_date"] = baseline.get("baseline_date")
        live_state["baseline_source"] = baseline.get("source", "baseline_file")
        logger.info(f"✅ Loaded yesterday's baseline data from {BASELINE_PATH}")
        return

    logger.info("📥 Baseline file missing or stale, attempting to fetch yesterday's OI from Upstox...")
    baseline = fetch_yesterday_baseline(instrument_keys)
    if baseline.get("instrument_data"):
        baseline_data = baseline
        live_state["baseline_date"] = baseline["baseline_date"]
        live_state["baseline_source"] = baseline.get("source")
        save_baseline_data(baseline)
        logger.info("✅ Baseline data loaded and persisted for tomorrow's session.")
    else:
        baseline_data = {}
        live_state["baseline_date"] = None
        live_state["baseline_source"] = None
        logger.warning("⚠️ Unable to load a valid yesterday baseline; live COI will fallback to the current session start.")


def extract_live_values(decoded: dict, instrument_key: str | None = None) -> dict:
    flattened = flatten_payload(decoded)
    instrument_id = instrument_key
    ltp = None
    oi = None
    volume = None

    for key, value in flattened.items():
        lower_key = key.lower()
        if instrument_id is None and any(token in lower_key for token in ["instrumentkey", "instrument_key", "instrument", "symbol", "tradingsymbol"]):
            if isinstance(value, str) and value:
                instrument_id = value
        if ltp is None and any(token in lower_key for token in ["lasttradedprice", "ltp", "lastprice", "ltpc.ltp", "atp"]):
            if isinstance(value, (int, float)):
                ltp = value
        if oi is None and ("openinterest" in lower_key or lower_key == 'oi' or lower_key.endswith('.oi') or '.oi.' in lower_key):
            if isinstance(value, (int, float)):
                oi = value
        if volume is None and any(token in lower_key for token in ["volume", "tradedvolume", "lasttradedqty", "qty", "vtt"]):
            if isinstance(value, (int, float)):
                volume = value

    return {"instrument_key": instrument_id, "ltp": ltp, "oi": oi, "volume": volume, "raw": decoded}


async def write_live_state() -> None:
    try:
        atomic_write_json(JSON_PATH, live_state)
    except Exception as exc:
        logger.warning(f"⚠️ Failed to write live state file: {exc}")

    try:
        serialized_state = json.dumps(live_state, default=str, separators=(",", ":"))
        redis_client.set(REDIS_DATA_KEY, serialized_state)
        redis_client.publish(REDIS_CHANNEL, "updated")
    except redis.RedisError as exc:
        logger.warning(f"⚠️ Failed to publish live state to Redis: {exc}")


async def update_instrument_state(instrument: str, values: dict) -> None:
    ltp = values.get("ltp")
    oi = values.get("oi")
    volume = values.get("volume")

    async with state_lock:
        meta = INSTRUMENT_METADATA.get(instrument, {})
        entry = live_state["instrument_data"].setdefault(instrument, {
            'strike': meta.get('strike'),
            'option_type': meta.get('option_type'),
            'tradingsymbol': meta.get('tradingsymbol'),
        })
        if ltp is not None:
            entry["ltp"] = ltp
            if oi is not None:
                entry["oi"] = oi
            baseline_oi = get_baseline_oi_for_instrument(instrument)
            if baseline_oi is None:
                if 'initial_oi' not in entry or entry.get('initial_oi') is None:
                    entry['initial_oi'] = oi
                baseline_oi = entry.get('initial_oi')
            if isinstance(baseline_oi, (int, float)) and baseline_oi != 0:
                coi = oi - baseline_oi
                entry["coi"] = coi
                entry["coi_pct"] = round((coi / baseline_oi) * 100, 2)
            else:
                entry["coi"] = None
                entry["coi_pct"] = None
        if volume is not None:
            entry["volume"] = volume
        if 'strike' not in entry and meta.get('strike') is not None:
            entry['strike'] = meta['strike']
        if 'option_type' not in entry and meta.get('option_type') is not None:
            entry['option_type'] = meta['option_type']
        if 'tradingsymbol' not in entry and meta.get('tradingsymbol'):
            entry['tradingsymbol'] = meta['tradingsymbol']
        entry["latest_tick_at"] = datetime.now(timezone(timedelta(hours=5, minutes=30))).isoformat()
        entry["raw_sample"] = values["raw"]
        await write_live_state()

    logger.info(f"💾 Wrote live state for {instrument} to JSON file.")


async def update_live_state_from_message(message: dict) -> None:
    if not isinstance(message, dict):
        logger.debug("Received unexpected message type from Upstox feed.")
        return

    feeds = None
    if isinstance(message.get("data"), dict) and isinstance(message["data"].get("feeds"), dict):
        feeds = message["data"]["feeds"]
    elif isinstance(message.get("feeds"), dict):
        feeds = message["feeds"]

    if feeds:
        for instrument, feed_payload in feeds.items():
            values = extract_live_values(feed_payload, instrument_key=instrument)
            await update_instrument_state(instrument, values)
        return

    payload = message.get("data") if isinstance(message.get("data"), (dict, list)) else message
    values = extract_live_values(payload)
    instrument = values.get("instrument_key")

    if not instrument:
        logger.debug("Received message without instrument_key.")
        return

    await update_instrument_state(instrument, values)


async def dump_live_state() -> None:
    while True:
        await asyncio.sleep(5)
        async with state_lock:
            live_state["updated_at"] = datetime.now(timezone(timedelta(hours=5, minutes=30))).isoformat()
            try:
                atomic_write_json(JSON_PATH, live_state)
            except Exception as exc:
                logger.warning(f"⚠️ Failed to write live state file: {exc}")


def build_streamer(token: str, instrument_keys: list[str], expiry_date: date, loop: asyncio.AbstractEventLoop) -> MarketDataStreamerV3:
    config = Configuration()
    config.access_token = token
    api_client = ApiClient(configuration=config)

    streamer = MarketDataStreamerV3(
        api_client=api_client,
        instrumentKeys=instrument_keys,
        mode=MarketDataStreamerV3.Mode["FULL"],
    )

    def on_open(*args):
        logger.info("✅ Upstox feed socket opened.")
        if not instrument_keys:
            logger.error("❌ No instrument keys available to subscribe.")
            return
        try:
            streamer.subscribe(instrument_keys, mode=MarketDataStreamerV3.Mode["FULL"])
            logger.info(f"📡 Subscribed to {len(instrument_keys)} instruments on full mode.")
            logger.info(f"📡 Sample instrument keys: {instrument_keys[:10]}")
        except Exception as exc:
            logger.error(f"❌ Failed to subscribe on open: {exc}")

    def on_message(*args):
        message = args[-1] if args else None
        print("📥 Raw tick received:", message)
        logger.info("✅ Received tick data, updating JSON...")
        if message is None:
            logger.warning("⚠️ Upstox feed MESSAGE received with no payload.")
            return
        asyncio.run_coroutine_threadsafe(update_live_state_from_message(message), loop)

    def on_error(*args):
        error = args[-1] if args else None
        logger.error(f"❌ Upstox feed error: {error}")

    def on_close(*args):
        code = args[0] if len(args) > 0 else None
        msg = args[1] if len(args) > 1 else None
        logger.warning(f"🔌 Upstox feed closed: {code} {msg}")

    streamer.on(streamer.Event["OPEN"], on_open)
    streamer.on(streamer.Event["MESSAGE"], on_message)
    streamer.on(streamer.Event["ERROR"], on_error)
    streamer.on(streamer.Event["CLOSE"], on_close)
    streamer.auto_reconnect(True, interval=5, retry_count=5)
    return streamer


async def run_streamer(instrument_keys: list[str], expiry_date: date) -> None:
    token = read_access_token()
    if not token:
        return

    live_state["expiry_date"] = expiry_date.isoformat()
    live_state["instrument_data"] = {}
    initialize_baseline(instrument_keys)

    loop = asyncio.get_running_loop()
    streamer = build_streamer(token, instrument_keys, expiry_date, loop)
    logger.info(f"🔌 Connecting to Upstox live feed for {len(instrument_keys)} instruments...")
    streamer.connect()

    try:
        while not is_shutdown_requested():
            await asyncio.sleep(1)
    finally:
        if hasattr(streamer, "disconnect"):
            try:
                streamer.disconnect()
            except Exception:
                pass
        elif hasattr(streamer, "close"):
            try:
                streamer.close()
            except Exception:
                pass


async def main() -> None:
    setup_signal_handlers()
    expiry_date, instrument_keys = find_nifty_weekly_option_keys()
    if not instrument_keys or expiry_date is None:
        logger.error("No NIFTY option instrument keys found for the next valid expiry.")
        return

    dump_task = asyncio.create_task(dump_live_state())
    try:
        await run_streamer(instrument_keys, expiry_date)
    finally:
        dump_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await dump_task


if __name__ == "__main__":
    import contextlib

    setup_signal_handlers()
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("🛑 Upstox live feed stopped by user.")

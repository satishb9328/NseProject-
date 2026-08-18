import asyncio
import csv
import gzip
import io
import json
import logging
from datetime import datetime, date, timedelta, timezone
from pathlib import Path

import pandas as pd
import requests
from upstox_client import ApiClient, Configuration
from upstox_client.feeder.market_data_streamer_v3 import MarketDataStreamerV3

# Resolve the absolute path to the Nse_Project directory
BASE_DIR = Path(__file__).resolve().parent.parent
TOKEN_DIR = BASE_DIR / 'Brokers' / 'upstox'
LIVE_STATE_FILE = Path(__file__).resolve().parent / "live_options_data.json"
MASTER_INSTRUMENTS_URL = "https://assets.upstox.com/market-quote/instruments/exchange/NSE.csv.gz"

TOKEN_FILE_PATHS = [
    TOKEN_DIR / "upstox_v3_token.json",
    TOKEN_DIR / "upstox_login_details.json",
    TOKEN_DIR / "upstox" / "upstox_token.json",
]

LOG_FORMAT = "%(asctime)s | %(levelname)-8s | %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT, datefmt="%Y-%m-%d %H:%M:%S")
logger = logging.getLogger("LiveUpstoxFeed")

state_lock = asyncio.Lock()
live_state = {
    "updated_at": None,
    "expiry_date": None,
    "instrument_data": {},
}


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
    except Exception as exc:
        logger.error(f"❌ Unable to load Upstox instrument master data: {exc}")
        return None, []

    df.columns = [col.strip().lower() for col in df.columns]
    if "name" not in df.columns or "instrument_type" not in df.columns or "expiry" not in df.columns or "instrument_key" not in df.columns:
        logger.error("❌ Upstox instrument master file is missing expected columns.")
        return None, []

    df = df[(df["name"].str.upper() == "NIFTY") & (df["instrument_type"].str.upper() == "OPTIDX")]
    if df.empty:
        logger.error("❌ No NIFTY OPTIDX records found in the instrument master file.")
        return None, []

    df["expiry"] = pd.to_datetime(df["expiry"], errors="coerce").dt.date
    today = datetime.now(timezone(timedelta(hours=5, minutes=30))).date()
    df = df[df["expiry"] >= today]
    if df.empty:
        logger.error("❌ No NIFTY OPTIDX expiry date found greater than or equal to today.")
        return None, []

    expiry_date = df["expiry"].min()
    filtered = df[df["expiry"] == expiry_date]
    instrument_keys = filtered["instrument_key"].dropna().astype(str).unique().tolist()
    instrument_keys.sort()

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


def extract_live_values(decoded: dict) -> dict:
    flattened = flatten_payload(decoded)
    instrument_id = None
    ltp = None
    oi = None

    for key, value in flattened.items():
        lower_key = key.lower()
        if instrument_id is None and any(token in lower_key for token in ["instrumentkey", "instrument_key", "instrument", "symbol"]):
            if isinstance(value, str) and value:
                instrument_id = value
        if ltp is None and any(token in lower_key for token in ["lasttradedprice", "ltp", "lastprice"]):
            if isinstance(value, (int, float)):
                ltp = value
        if oi is None and "openinterest" in lower_key:
            if isinstance(value, (int, float)):
                oi = value

    return {"instrument_key": instrument_id, "ltp": ltp, "oi": oi, "raw": decoded}


async def dump_live_state() -> None:
    while True:
        await asyncio.sleep(5)
        async with state_lock:
            live_state["updated_at"] = datetime.now(timezone(timedelta(hours=5, minutes=30))).isoformat()
            try:
                LIVE_STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
                with LIVE_STATE_FILE.open("w", encoding="utf-8") as fh:
                    json.dump(live_state, fh, indent=2, default=str)
            except Exception as exc:
                logger.warning(f"⚠️ Failed to write live state file: {exc}")


async def update_live_state_from_message(message: dict) -> None:
    if not isinstance(message, dict):
        logger.debug("Received unexpected message type from Upstox feed.")
        return

    payload = message.get("data") if isinstance(message.get("data"), (dict, list)) else message
    values = extract_live_values(payload)
    instrument = values.get("instrument_key")
    ltp = values.get("ltp")
    oi = values.get("oi")

    if not instrument:
        logger.debug("Received message without instrument_key.")
        return

    async with state_lock:
        entry = live_state["instrument_data"].setdefault(instrument, {})
        if ltp is not None:
            entry["ltp"] = ltp
        if oi is not None:
            entry["oi"] = oi
        entry["latest_tick_at"] = datetime.now(timezone(timedelta(hours=5, minutes=30))).isoformat()
        entry["raw_sample"] = values["raw"]

    logger.info(f"✅ Updated live state for {instrument}: LTP={ltp} OI={oi}")


def build_streamer(token: str, instrument_keys: list[str], expiry_date: date, loop: asyncio.AbstractEventLoop) -> MarketDataStreamerV3:
    config = Configuration()
    config.access_token = token
    api_client = ApiClient(configuration=config)

    streamer = MarketDataStreamerV3(
        api_client=api_client,
        instrumentKeys=instrument_keys,
        mode=MarketDataStreamerV3.Mode["FULL"],
    )

    def on_open(ws):
        logger.info("✅ Upstox feed socket opened.")

    def on_message(ws, message):
        logger.debug("📩 Upstox feed MESSAGE received.")
        asyncio.run_coroutine_threadsafe(update_live_state_from_message(message), loop)

    def on_error(ws, error):
        logger.error(f"❌ Upstox feed error: {error}")

    def on_close(ws, code, msg):
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

    loop = asyncio.get_running_loop()
    streamer = build_streamer(token, instrument_keys, expiry_date, loop)
    logger.info(f"🔌 Connecting to Upstox live feed for {len(instrument_keys)} instruments...")
    streamer.connect()

    while True:
        await asyncio.sleep(1)


async def main() -> None:
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

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("🛑 Upstox live feed stopped by user.")

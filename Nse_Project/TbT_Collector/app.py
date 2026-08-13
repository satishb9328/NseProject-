"""
Upstox Cloud Collector with Token Catcher Dashboard & Supabase
- Hosts a web page for easy mobile login.
- Background thread collects TBT data automatically.
- Saves/Reads Access Token from Supabase to bypass Gunicorn memory limits.
- FIXED: Correct V3 WebSocket subscription format + protobuf decoding
"""
import asyncio
import websockets
import json
import logging
import os
import time
import requests
import threading
import psycopg2
import struct
import blackboxprotobuf
from psycopg2.extras import execute_values
from datetime import datetime, timezone, timedelta
from flask import Flask, redirect, request

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-8s | %(message)s",
                    datefmt="%Y-%m-%d %H:%M:%S")
logger = logging.getLogger("CloudCollector")

UPSTOX_API_KEY = os.getenv("UPSTOX_API_KEY", "0a02ae84-2ac8-4c54-974a-af1202562a43")
UPSTOX_API_SECRET = os.getenv("UPSTOX_API_SECRET", "ca5k88sap1")
REDIRECT_URI = os.getenv("REDIRECT_URI", "https://nseproject.onrender.com/callback")

# Instrument keys to subscribe to (Futures)
# Using known working keys directly - the instrument search API endpoint is deprecated
INSTRUMENT_KEYS = [
    "NSE_FO|43842",  # Nifty 50 Future (verified working from collector.py)
]
# Always subscribe to the index too - it NEVER expires and always has data
SPOT_INDEX_KEY = "NSE_INDEX|Nifty 50"
WS_URL = "wss://api.upstox.com/v3/feed/market-data-feed"
app = Flask(__name__)
collector_state = {"status": "Idle - Waiting for login"}

# ---------------------------------------------------------------------------
# Database Manager (Supabase / PostgreSQL)
# ---------------------------------------------------------------------------
def get_db_connection():
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise Exception("DATABASE_URL environment variable not set!")
    return psycopg2.connect(db_url)

db_batch = []
BATCH_SIZE = 10  # Lowered to 10 for instant DB testing

def _execute_db_insert(batch):
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            query = "INSERT INTO raw_ticks (timestamp, size, data) VALUES %s"
            execute_values(cur, query, batch)
            conn.commit()
            logger.info(f"💾 Saved {len(batch)} raw packets to Supabase")
    except Exception as e:
        logger.error(f"❌ DB Error: {e}")
    finally:
        if conn:
            conn.close()

async def save_raw_to_db(timestamp: str, data: bytes):
    global db_batch
    db_batch.append((timestamp, len(data), data))
    if len(db_batch) >= BATCH_SIZE:
        batch_to_save = db_batch.copy()
        db_batch.clear()
        await asyncio.to_thread(_execute_db_insert, batch_to_save)

def save_token_to_db(token: str):
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO config (key, value) VALUES ('access_token', %s)
                ON CONFLICT (key) DO UPDATE SET value = %s;
            """, (token, token))
            conn.commit()
            logger.info("🔐 Token saved to Supabase config table!")
    except Exception as e:
        logger.error(f"❌ Error saving token: {e}")
    finally:
        if conn:
            conn.close()

def get_token_from_db():
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute("SELECT value FROM config WHERE key = 'access_token'")
            row = cur.fetchone()
            if row:
                return row[0]
    except Exception as e:
        logger.error(f"❌ Error fetching token: {e}")
    finally:
        if conn:
            conn.close()
    return None

# ---------------------------------------------------------------------------
# Upstox V3 Binary Decoder
# ---------------------------------------------------------------------------
def decode_upstox_binary(raw_bytes: bytes):
    """
    Decode Upstox V3 binary protobuf data using blackboxprotobuf.
    The V3 feed uses protobuf encoding for market data.
    """
    try:
        if len(raw_bytes) < 17:
            return None

        # Decode the protobuf message
        decoded, typedef = blackboxprotobuf.decode_message(raw_bytes)
        return decoded
    except Exception as e:
        logger.error(f"❌ Decode error: {e}")
        return None


# ---------------------------------------------------------------------------
# WebSocket Listener (Background Task)
# ---------------------------------------------------------------------------

async def websocket_listener():

    while True:
        IST = timezone(timedelta(hours=5, minutes=30))
        now = datetime.now(IST)
        market_open = now.replace(hour=9, minute=15, second=0, microsecond=0)
        market_close = now.replace(hour=15, minute=30, second=0, microsecond=0)

        if not (market_open <= now <= market_close) or now.weekday() >= 5:
            collector_state["status"] = "Market Closed. Sleeping..."
            logger.info("Market Closed. Sleeping...")
            await asyncio.sleep(60)
            continue

        token = get_token_from_db()
        if not token:
            collector_state["status"] = "Idle - Waiting for login"
            logger.info("Idle - Waiting for login (Checked DB, no token)...")
            await asyncio.sleep(10)
            continue

        collector_state["status"] = "Connecting to WebSocket..."

        headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}


        try:
            # Use additional_headers (websockets >= 11) - same as TESTING/collector.py
            ws = await websockets.connect(
                WS_URL,
                additional_headers=headers,
                max_size=2 ** 24,
                ping_interval=20,
                ping_timeout=20
            )

            async with ws:
                collector_state["status"] = "✅ Live: Collecting TBT Data"
                logger.info("✅ V3 WebSocket Connected!")

                # CORRECT V3 Subscription Payload Format
                # Subscribe to BOTH index AND futures together - same as TESTING/collector.py
                # This is the verified working format from collector.py
                all_keys = [SPOT_INDEX_KEY] + list(INSTRUMENT_KEYS)
                sub_msg = {
                    "guid": f"tbt-{int(time.time())}",
                    "method": "sub",
                    "data": {
                        "mode": "full",
                        "instrumentKeys": all_keys
                    }
                }
                await ws.send(json.dumps(sub_msg))
                logger.info(f"📡 Subscribed to: {all_keys} (mode: full)")

                while True:
                    try:
                        message = await asyncio.wait_for(ws.recv(), timeout=60.0)

                        if isinstance(message, bytes):
                            # Log EVERY packet - do NOT skip any!
                            logger.info(f"📦 Received packet! Size: {len(message)} bytes")

                            # Decode the binary data to understand what it contains
                            decoded = decode_upstox_binary(message)
                            if decoded:
                                logger.info(f"📊 Decoded: {json.dumps(decoded, default=str)[:300]}")

                            # Save raw packet to DB
                            ts = datetime.now().isoformat()
                            await save_raw_to_db(ts, message)

                        elif isinstance(message, str):
                            logger.info(f"Text Msg: {message}")


                    except asyncio.TimeoutError:
                        logger.warning("⏳ No ticks in 60s. Reconnecting...")
                        break
                    except websockets.exceptions.ConnectionClosed as ce:
                        logger.warning(f"🔌 Connection closed: {ce}. Reconnecting...")
                        break

        except Exception as e:
            logger.error(f"❌ WebSocket Error: {e}")
            collector_state["status"] = f"Error: {e}. Retrying in 5s..."
            await asyncio.sleep(5)


def run_asyncio_loop():
    try:
        asyncio.run(websocket_listener())
    except Exception as e:
        logger.error(f"❌ BACKGROUND THREAD CRASHED: {e}")

# ---------------------------------------------------------------------------
# Flask Web Server (Token Catcher)
# ---------------------------------------------------------------------------
@app.route("/")
def home():
    return f"""
    <h1>Upstox TBT Cloud Collector</h1>
    <p><b>Status:</b> {collector_state["status"]}</p>
    {'<a href="/login"><button>Login to Upstox</button></a>' if not get_token_from_db() else '<p>✅ Token Active</p>'}
    """

@app.route("/login")
def login():
    auth_url = f"https://api.upstox.com/v2/login/authorization/dialog?response_type=code&client_id={UPSTOX_API_KEY}&redirect_uri={REDIRECT_URI}"
    return redirect(auth_url)

@app.route("/callback")
def callback():
    code = request.args.get("code")
    if not code:
        return "Error: No code provided", 400

    url = "https://api.upstox.com/v2/login/authorization/token"
    headers = {"Content-Type": "application/x-www-form-urlencoded", "Accept": "application/json"}
    data = {
        "code": code,
        "client_id": UPSTOX_API_KEY,
        "client_secret": UPSTOX_API_SECRET,
        "grant_type": "authorization_code",
        "redirect_uri": REDIRECT_URI
    }

    resp = requests.post(url, headers=headers, data=data)
    if resp.status_code == 200:
        token = resp.json().get("access_token")
        save_token_to_db(token)
        collector_state["status"] = "Token Received! Starting collector..."
        return "✅ Login Successful! You can close this tab. Data collection will start automatically."
    return f"❌ Error: {resp.text}", 500

# ---------------------------------------------------------------------------
# Main Execution
# ---------------------------------------------------------------------------
threading.Thread(target=run_asyncio_loop, daemon=True).start()
logger.info("🚀 Background Data Collector thread started...")

if __name__ == "__main__":
    logger.info("Starting Cloud Web Server (Local)...")
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)

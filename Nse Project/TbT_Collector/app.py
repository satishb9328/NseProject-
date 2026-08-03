"""
Upstox Cloud Collector with Token Catcher Dashboard & Supabase
- Hosts a web page for easy mobile login.
- Background thread collects TBT data automatically.
- Saves raw binary ticks directly to Supabase PostgreSQL.
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
from psycopg2.extras import execute_values
from datetime import datetime
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
REDIRECT_URI = os.getenv("REDIRECT_URI", "https://upstox-tbt-collector.onrender.com/callback")

SPOT_KEY = "NSE_INDEX|Nifty 50"
FUT_KEY = "NSE_FO|43842"  # Update to current expiry if needed!
WS_URL = "wss://api.upstox.com/v3/feed/market-data-feed"

# Global State
app = Flask(__name__)
collector_state = {
    "access_token": None,
    "last_login": "Never",
    "status": "Idle - Waiting for login"
}


# ---------------------------------------------------------------------------
# Database Manager (Supabase / PostgreSQL)
# ---------------------------------------------------------------------------
def get_db_connection():
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise Exception("DATABASE_URL environment variable not set!")
    return psycopg2.connect(db_url)


db_batch = []
BATCH_SIZE = 500


async def save_raw_to_db(timestamp: str, data: bytes):
    global db_batch
    db_batch.append((timestamp, len(data), data))

    if len(db_batch) >= BATCH_SIZE:
        conn = None
        try:
            conn = get_db_connection()
            with conn.cursor() as cur:
                query = "INSERT INTO raw_ticks (timestamp, size, data) VALUES %s"
                execute_values(cur, query, db_batch)
                conn.commit()
                logger.info(f"💾 Saved {len(db_batch)} raw packets to Supabase")
        except Exception as e:
            logger.error(f"DB Error: {e}")
        finally:
            if conn:
                conn.close()
            db_batch.clear()


# ---------------------------------------------------------------------------
# WebSocket Listener (Background Task)
# ---------------------------------------------------------------------------
async def websocket_listener():
    global collector_state

    while True:
        # Check market hours (9:15 AM to 3:30 PM IST, Mon-Fri)
        # Force the server to use Indian Standard Time (UTC + 5:30)
        IST = timezone(timedelta(hours=5, minutes=30))
        now = datetime.now(IST)
        market_open = now.replace(hour=9, minute=15, second=0, microsecond=0)
        market_close = now.replace(hour=15, minute=30, second=0, microsecond=0)

        if not (market_open <= now <= market_close) or now.weekday() >= 5:
            collector_state["status"] = "Market Closed. Sleeping..."
            await asyncio.sleep(60)
            continue

        if not collector_state["access_token"]:
            collector_state["status"] = "Idle - Waiting for login"
            await asyncio.sleep(10)
            continue

        collector_state["status"] = "Connecting to WebSocket..."
        headers = {"Authorization": f"Bearer {collector_state['access_token']}", "Accept": "application/json"}

        try:
            async with websockets.connect(WS_URL, additional_headers=headers, max_size=2 ** 24) as ws:
                collector_state["status"] = "✅ Live: Collecting TBT Data"
                logger.info("✅ V3 WebSocket Connected!")

                sub_msg = {
                    "guid": f"tbt-{int(time.time())}",
                    "method": "sub",
                    "data": {"mode": "full", "instrumentKeys": [SPOT_KEY, FUT_KEY]}
                }
                await ws.send(json.dumps(sub_msg))

                while True:
                    message = await ws.recv()
                    if isinstance(message, bytes):
                        ts = datetime.now().isoformat()
                        await save_raw_to_db(ts, message)

        except Exception as e:
            logger.error(f"WebSocket Error: {e}")
            collector_state["status"] = f"Error: {e}. Retrying in 5s..."
            await asyncio.sleep(5)


def run_asyncio_loop():
    """Runs the websocket listener in a background thread."""
    asyncio.run(websocket_listener())


# ---------------------------------------------------------------------------
# Flask Web Server (Token Catcher)
# ---------------------------------------------------------------------------
@app.route("/")
def home():
    status = collector_state["status"]
    login_time = collector_state["last_login"]
    return f"""
    <h1>Upstox TBT Cloud Collector</h1>
    <p><b>Status:</b> {status}</p>
    <p><b>Last Login:</b> {login_time}</p>
    {'<a href="/login"><button>Login to Upstox</button></a>' if not collector_state['access_token'] else '<p>✅ Token Active</p>'}
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
        collector_state["access_token"] = resp.json().get("access_token")
        collector_state["last_login"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        collector_state["status"] = "Token Received! Starting collector..."
        return "✅ Login Successful! You can close this tab. Data collection will start automatically."
    return f"❌ Error: {resp.text}", 500


# ---------------------------------------------------------------------------
# Main Execution
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    # Start the background data collector thread
    threading.Thread(target=run_asyncio_loop, daemon=True).start()
    logger.info("🚀 Starting Cloud Web Server...")

    # Start the Flask web server (Render looks for port 8080 or $PORT)
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
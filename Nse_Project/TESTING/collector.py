"""
Cloud-Ready Upstox V3 Raw TBT Collector
- Connects to V3 WebSocket.
- Auto-refreshes token daily.
- Saves ALL raw binary packets to SQLite for later decoding.
"""
import asyncio
import websockets
import json
import sqlite3
import logging
import os
import time
import requests
from datetime import datetime
from apscheduler.schedulers.asyncio import AsyncIOScheduler

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-8s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
logger = logging.getLogger("CloudCollector")

UPSTOX_API_KEY = os.getenv("UPSTOX_API_KEY", "0a02ae84-2ac8-4c54-974a-af1202562a43")
UPSTOX_API_SECRET = os.getenv("UPSTOX_API_SECRET", "ca5k88sap1")
UPSTOX_REFRESH_TOKEN = os.getenv("eyJ0eXAiOiJKV1QiLCJrZXlfaWQiOiJza192MS4wIiwiYWxnIjoiSFMyNTYifQ.eyJzdWIiOiI1NUFGR1ciLCJqdGkiOiI2YTY5NzIzNDBjNDllZDY5NTMxZjU3NzkiLCJpc011bHRpQ2xpZW50IjpmYWxzZSwiaXNQbHVzUGxhbiI6dHJ1ZSwiaWF0IjoxNzg1Mjk1NDEyLCJpc3MiOiJ1ZGFwaS1nYXRld2F5LXNlcnZpY2UiLCJleHAiOjE3ODUzNjI0MDB9.2hwIFSIJL7vuZroi2oS87Kgbifq9lrPq1E52_zN4Y5M")

SPOT_KEY = "NSE_INDEX|Nifty 50"
FUT_KEY = "NSE_FO|43842"  # Update to current expiry!
WS_URL = "wss://api.upstox.com/v3/feed/market-data-feed"

ACCESS_TOKEN = None
is_market_open = False

# ---------------------------------------------------------------------------
# Auth Manager
# ---------------------------------------------------------------------------
async def refresh_access_token():
    global ACCESS_TOKEN
    logger.info("🔄 Refreshing Upstox Access Token...")
    url = "https://api.upstox.com/v2/login/authorization/token"
    headers = {"Content-Type": "application/x-www-form-urlencoded", "Accept": "application/json"}
    data = {
        "grant_type": "refresh_token",
        "refresh_token": UPSTOX_REFRESH_TOKEN,
        "client_id": UPSTOX_API_KEY,
        "client_secret": UPSTOX_API_SECRET
    }
    try:
        response = requests.post(url, headers=headers, data=data)
        response.raise_for_status()
        ACCESS_TOKEN = response.json().get("access_token")
        if ACCESS_TOKEN:
            logger.info("✅ Access Token refreshed successfully!")
            return True
        logger.error(f"❌ Token refresh failed: {response.json()}")
        return False
    except Exception as e:
        logger.error(f"❌ Auth Error: {e}")
        return False

# ---------------------------------------------------------------------------
# Database Manager (Saves Raw Bytes)
# ---------------------------------------------------------------------------
def get_db_connection():
    today_str = datetime.now().strftime("%Y-%m-%d")
    db_path = f"tbt_data_{today_str}.db"
    conn = sqlite3.connect(db_path)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS raw_ticks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            size INTEGER,
            data BLOB
        )
    """)
    return conn

db_batch = []
BATCH_SIZE = 500

async def save_raw_to_db(timestamp: str, data: bytes):
    global db_batch
    db_batch.append((timestamp, len(data), data))

    if len(db_batch) >= BATCH_SIZE:
        conn = get_db_connection()
        try:
            conn.executemany("""
                INSERT INTO raw_ticks (timestamp, size, data)
                VALUES (?, ?, ?)
            """, db_batch)
            conn.commit()
            logger.info(f"💾 Saved {len(db_batch)} raw packets to DB")
        except Exception as e:
            logger.error(f"DB Error: {e}")
        finally:
            conn.close()
            db_batch.clear()

# ---------------------------------------------------------------------------
# WebSocket Listener
# ---------------------------------------------------------------------------
async def websocket_listener():
    global ACCESS_TOKEN, is_market_open

    # Check market hours (9:15 AM to 3:30 PM IST, Mon-Fri)
    now = datetime.now()
    market_open = now.replace(hour=9, minute=15, second=0, microsecond=0)
    market_close = now.replace(hour=15, minute=30, second=0, microsecond=0)

    if not (market_open <= now <= market_close) or now.weekday() >= 5:
        is_market_open = False
        logger.info("Market is closed. Sleeping for 60 seconds...")
        await asyncio.sleep(60)
        return

    is_market_open = True

    if not ACCESS_TOKEN:
        await refresh_access_token()
        if not ACCESS_TOKEN:
            await asyncio.sleep(300)
            return

    headers = {"Authorization": f"Bearer {ACCESS_TOKEN}", "Accept": "application/json"}

    try:
        async with websockets.connect(WS_URL, additional_headers=headers, max_size=2**24) as ws:
            logger.info("✅ V3 WebSocket Connected!")

            sub_msg = {
                "guid": f"tbt-{int(time.time())}",
                "method": "sub",
                "data": {"mode": "full", "instrumentKeys": [SPOT_KEY, FUT_KEY]}
            }
            await ws.send(json.dumps(sub_msg))
            logger.info(f"📡 Subscribed to: {SPOT_KEY} + {FUT_KEY}")

            while is_market_open:
                message = await ws.recv()
                if isinstance(message, bytes):
                    # Save the raw binary data directly
                    ts = datetime.now().isoformat()
                    await save_raw_to_db(ts, message)

    except Exception as e:
        logger.error(f"WebSocket Error: {e}")
        await asyncio.sleep(5)

# ---------------------------------------------------------------------------
# Main Loop
# ---------------------------------------------------------------------------
async def main():
    logger.info("🚀 Starting Cloud V3 Raw Auto-Collector...")

    # Auto-refresh token at 8:30 AM IST (03:00 AM UTC) every day
    scheduler = AsyncIOScheduler()
    scheduler.add_job(refresh_access_token, 'cron', hour=3, minute=0, timezone='UTC')
    scheduler.start()
    logger.info("Scheduler started: Token will refresh daily at 8:30 AM IST")

    await refresh_access_token()

    while True:
        await websocket_listener()
        await asyncio.sleep(1)

if __name__ == "__main__":
    asyncio.run(main())
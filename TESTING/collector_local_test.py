"""
Upstox TBT Local Test Collector
- Uses provided access token for immediate testing.
- Saves ticks to a local SQLite database.
"""

import asyncio
import websockets
import json
import struct
import sqlite3
import logging
import time
from datetime import datetime, timezone

# ---------------------------------------------------------------------------
# Configuration & Credentials
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("TBT_Test")

# Your Credentials
ACCESS_TOKEN = "eyJ0eXAiOiJKV1QiLCJrZXlfaWQiOiJza192MS4wIiwiYWxnIjoiSFMyNTYifQ.eyJzdWIiOiI1NUFGR1ciLCJqdGkiOiI2YTY5NzIzNDBjNDllZDY5NTMxZjU3NzkiLCJpc011bHRpQ2xpZW50IjpmYWxzZSwiaXNQbHVzUGxhbiI6dHJ1ZSwiaWF0IjoxNzg1Mjk1NDEyLCJpc3MiOiJ1ZGFwaS1nYXRld2F5LXNlcnZpY2UiLCJleHAiOjE3ODUzNjI0MDB9.2hwIFSIJL7vuZroi2oS87Kgbifq9lrPq1E52_zN4Y5M"

# Instruments
SPOT_KEY = "NSE_INDEX|Nifty 50"
FUT_KEY = "NSE_FO|43842"  # Make sure this is the current expiry instrument key!
WS_URL = "wss://api.upstox.com/v3/feed/market-data-feed"

# Database Setup
DB_PATH = "tbt_local_test.db"
db_batch = []


# ---------------------------------------------------------------------------
# Database Manager
# ---------------------------------------------------------------------------
def init_db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS ticks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            instrument TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            ltp REAL,
            volume INTEGER,
            bid REAL,
            ask REAL,
            bid_qty INTEGER,
            ask_qty INTEGER,
            oi INTEGER
        )
    """)
    conn.commit()
    conn.close()
    logger.info(f"📊 Database initialized: {DB_PATH}")


async def save_tick_to_db(tick_data: dict):
    global db_batch
    db_batch.append(tick_data)

    if len(db_batch) >= 100:  # Smaller batch size for testing
        conn = sqlite3.connect(DB_PATH)
        try:
            conn.executemany("""
                INSERT INTO ticks (instrument, timestamp, ltp, volume, bid, ask, bid_qty, ask_qty, oi)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [(t['instrument'], t['timestamp'], t['ltp'], t['volume'],
                   t['bid'], t['ask'], t['bid_qty'], t['ask_qty'], t['oi']) for t in db_batch])
            conn.commit()
            logger.info(f"💾 Saved {len(db_batch)} ticks to DB")
        except Exception as e:
            logger.error(f"DB Error: {e}")
        finally:
            conn.close()
            db_batch.clear()


# ---------------------------------------------------------------------------
# Upstox WebSocket Parser & Client
# ---------------------------------------------------------------------------
def parse_upstox_binary(raw_bytes: bytes):
    """Simplified parser for Upstox binary TBT data."""
    try:
        if len(raw_bytes) < 17:
            return None

        # Extract LTP (bytes 9-17 as uint64 scaled by 100)
        ltp_raw = struct.unpack('<Q', raw_bytes[9:17])[0]
        ltp = ltp_raw / 100.0

        # Extract Volume (if available)
        volume = 0
        if len(raw_bytes) >= 33:
            volume = struct.unpack('<Q', raw_bytes[25:33])[0]

        return {
            "ltp": ltp,
            "volume": volume,
            "bid": 0.0, "ask": 0.0, "bid_qty": 0, "ask_qty": 0, "oi": 0
        }
    except Exception as e:
        return None


async def websocket_listener():
    headers = {
        "Authorization": f"Bearer {ACCESS_TOKEN}",
        "Accept": "application/json"
    }

    try:
        async with websockets.connect(WS_URL, additional_headers=headers, max_size=2 ** 24) as ws:
            logger.info("✅ WebSocket Connected to Upstox!")

            # Subscribe to instruments
            sub_msg = {
                "guid": f"test-{int(time.time())}",
                "method": "sub",
                "data": {
                    "mode": "full",
                    "instrumentKeys": [SPOT_KEY, FUT_KEY]
                }
            }
            await ws.send(json.dumps(sub_msg))
            logger.info(f"📡 Subscribed to: {SPOT_KEY} + {FUT_KEY}")
            logger.info("⏳ Waiting for ticks... (Press Ctrl+C to stop)")

            # Listen loop
            while True:
                message = await ws.recv()
                if isinstance(message, bytes):
                    parsed = parse_upstox_binary(message)
                    if parsed and parsed['ltp'] > 0:
                        tick = {
                            "instrument": "UNKNOWN",  # We'll update this logic later
                            "timestamp": datetime.now(timezone.utc).isoformat(),
                            **parsed
                        }
                        logger.info(f"📈 Tick Received: LTP={tick['ltp']}, Vol={tick['volume']}")
                        await save_tick_to_db(tick)

    except Exception as e:
        logger.error(f"WebSocket Error: {e}")


# ---------------------------------------------------------------------------
# Main Execution
# ---------------------------------------------------------------------------
async def main():
    logger.info("🚀 Starting Local TBT Test...")
    init_db()
    await websocket_listener()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n🛑 Stopped by user.")
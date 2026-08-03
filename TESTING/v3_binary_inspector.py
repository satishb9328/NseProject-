"""
Upstox V3 Binary Inspector
- Connects to V3 WS and prints raw hex of the binary packets.
"""
import asyncio
import websockets
import json
import requests
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-8s | %(message)s")
logger = logging.getLogger("Inspector")

ACCESS_TOKEN = "eyJ0eXAiOiJKV1QiLCJrZXlfaWQiOiJza192MS4wIiwiYWxnIjoiSFMyNTYifQ.eyJzdWIiOiI1NUFGR1ciLCJqdGkiOiI2YTY5NzIzNDBjNDllZDY5NTMxZjU3NzkiLCJpc011bHRpQ2xpZW50IjpmYWxzZSwiaXNQbHVzUGxhbiI6dHJ1ZSwiaWF0IjoxNzg1Mjk1NDEyLCJpc3MiOiJ1ZGFwaS1nYXRld2F5LXNlcnZpY2UiLCJleHAiOjE3ODUzNjI0MDB9.2hwIFSIJL7vuZroi2oS87Kgbifq9lrPq1E52_zN4Y5M"

WS_URL = "wss://api.upstox.com/v3/feed/market-data-feed"
SPOT_KEY = "NSE_INDEX|Nifty 50"


async def inspect_websocket():
    logger.info("Connecting to V3 WebSocket to inspect binary data...")
    headers = {
        "Authorization": f"Bearer {ACCESS_TOKEN}",
        "Accept": "application/json"
    }

    try:
        ws = await websockets.connect(WS_URL, additional_headers=headers, max_size=2 ** 24, open_timeout=10)
        logger.info("✅ Connected!")

        sub_msg = {
            "guid": "inspector-v3",
            "method": "sub",
            "data": {
                "mode": "full",
                "instrumentKeys": [SPOT_KEY]
            }
        }
        await ws.send(json.dumps(sub_msg))
        logger.info("📡 Subscribed. Waiting for binary packets...")

        # Receive 3 messages and inspect them
        for i in range(3):
            msg = await asyncio.wait_for(ws.recv(), timeout=10)
            if isinstance(msg, bytes):
                logger.info(f"\n--- PACKET {i + 1} ---")
                logger.info(f"Total Length: {len(msg)} bytes")
                # Print raw hex
                logger.info(f"Hex: {msg.hex()}")
                # Print raw bytes array
                logger.info(f"Bytes: {list(msg)}")
            else:
                logger.info(f"Received Text: {msg}")

        await ws.close()
        logger.info("🛑 Inspection complete.")

    except Exception as e:
        logger.error(f"❌ Error: {type(e).__name__} - {e}")


if __name__ == "__main__":
    asyncio.run(inspect_websocket())
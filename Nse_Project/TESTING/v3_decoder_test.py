"""
Upstox V3 Protobuf Decoder Test
- Decodes V3 binary data without needing the official SDK.
"""
import asyncio
import websockets
import json
import requests
import logging
import blackboxprotobuf

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-8s | %(message)s")
logger = logging.getLogger("Decoder")

ACCESS_TOKEN = "eyJ0eXAiOiJKV1QiLCJrZXlfaWQiOiJza192MS4wIiwiYWxnIjoiSFMyNTYifQ.eyJzdWIiOiI1NUFGR1ciLCJqdGkiOiI2YTY5NzIzNDBjNDllZDY5NTMxZjU3NzkiLCJpc011bHRpQ2xpZW50IjpmYWxzZSwiaXNQbHVzUGxhbiI6dHJ1ZSwiaWF0IjoxNzg1Mjk1NDEyLCJpc3MiOiJ1ZGFwaS1nYXRld2F5LXNlcnZpY2UiLCJleHAiOjE3ODUzNjI0MDB9.2hwIFSIJL7vuZroi2oS87Kgbifq9lrPq1E52_zN4Y5M"

WS_URL = "wss://api.upstox.com/v3/feed/market-data-feed"
SPOT_KEY = "NSE_INDEX|Nifty 50"


async def decode_websocket():
    logger.info("Connecting to V3 WebSocket to decode data...")
    headers = {
        "Authorization": f"Bearer {ACCESS_TOKEN}",
        "Accept": "application/json"
    }

    try:
        async with websockets.connect(WS_URL, additional_headers=headers, max_size=2 ** 24, open_timeout=10) as ws:
            logger.info("✅ Connected!")

            sub_msg = {
                "guid": "decoder-v3",
                "method": "sub",
                "data": {
                    "mode": "full",
                    "instrumentKeys": [SPOT_KEY]
                }
            }
            await ws.send(json.dumps(sub_msg))
            logger.info("📡 Subscribed. Waiting for packets...")

            # Receive 3 packets and decode them
            for i in range(3):
                msg = await asyncio.wait_for(ws.recv(), timeout=10)
                if isinstance(msg, bytes):
                    logger.info(f"\n--- PACKET {i + 1} ({len(msg)} bytes) ---")
                    try:
                        # Decode the protobuf blindly
                        decoded, typedef = blackboxprotobuf.decode_message(msg)
                        logger.info(f"Decoded Data: {json.dumps(decoded, indent=2, default=str)}")
                    except Exception as decode_err:
                        logger.error(f"Could not decode: {decode_err}")
                        logger.info(f"Raw Hex: {msg.hex()}")
                else:
                    logger.info(f"Received Text: {msg}")

    except Exception as e:
        logger.error(f"❌ Error: {type(e).__name__} - {e}")


if __name__ == "__main__":
    asyncio.run(decode_websocket())
"""
Upstox Connection Diagnostic v4
- Updated to V3 WebSocket Endpoint
"""
import asyncio
import websockets
import json
import requests
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-8s | %(message)s")
logger = logging.getLogger("Diagnostic")

ACCESS_TOKEN = "eyJ0eXAiOiJKV1QiLCJrZXlfaWQiOiJza192MS4wIiwiYWxnIjoiSFMyNTYifQ.eyJzdWIiOiI1NUFGR1ciLCJqdGkiOiI2YTY5NzIzNDBjNDllZDY5NTMxZjU3NzkiLCJpc011bHRpQ2xpZW50IjpmYWxzZSwiaXNQbHVzUGxhbiI6dHJ1ZSwiaWF0IjoxNzg1Mjk1NDEyLCJpc3MiOiJ1ZGFwaS1nYXRld2F5LXNlcnZpY2UiLCJleHAiOjE3ODUzNjI0MDB9.2hwIFSIJL7vuZroi2oS87Kgbifq9lrPq1E52_zN4Y5M"

# UPDATED TO V3 ENDPOINT
WS_URL = "wss://api.upstox.com/v3/feed/market-data-feed"
SPOT_KEY = "NSE_INDEX|Nifty 50"

async def test_websocket():
    logger.info(f"Attempting WebSocket connection to V3 endpoint...")
    headers = {
        "Authorization": f"Bearer {ACCESS_TOKEN}",
        "Accept": "application/json"
    }

    try:
        ws = await websockets.connect(WS_URL, additional_headers=headers, max_size=2**24, open_timeout=10)
        logger.info("✅ WebSocket Connection Successful!")

        # V3 Subscription Payload might be slightly different, but let's try the standard one first
        sub_msg = {
            "guid": "diag-test-v3",
            "method": "sub",
            "data": {
                "mode": "full",
                "instrumentKeys": [SPOT_KEY]
            }
        }
        await ws.send(json.dumps(sub_msg))
        logger.info("📡 Subscription sent. Waiting for message...")

        msg = await asyncio.wait_for(ws.recv(), timeout=5)
        if isinstance(msg, bytes):
            logger.info(f"✅ Received binary data (Length: {len(msg)} bytes)")
        else:
            logger.info(f"✅ Received text: {msg}")

        await ws.close()

    except websockets.exceptions.InvalidStatus as e:
        logger.error(f"❌ WebSocket Handshake Failed. HTTP Status: {e.response.status_code}")
        try:
            body = e.response.body.decode('utf-8') if e.response.body else "No body"
            logger.error(f"🚨 SERVER ERROR BODY: {body}")
        except Exception as inner_e:
            logger.error(f"Could not read body: {inner_e}")

    except Exception as e:
        logger.error(f"❌ Other WebSocket Error: {type(e).__name__} - {e}")

def test_rest_api():
    logger.info("Testing Access Token via REST API (Profile)...")
    url = "https://api.upstox.com/v2/user/profile"
    headers = {
        "Authorization": f"Bearer {ACCESS_TOKEN}",
        "Accept": "application/json"
    }

    try:
        response = requests.get(url, headers=headers, timeout=5)
        if response.status_code == 200:
            logger.info("✅ REST API Success! Token is VALID.")
        else:
            logger.error(f"❌ REST API Failed. Status: {response.status_code}")
    except Exception as e:
        logger.error(f"❌ REST API Request Error: {e}")

if __name__ == "__main__":
    test_rest_api()
    asyncio.run(test_websocket())
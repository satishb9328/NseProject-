import json
import asyncio
import os
from Nse_Project.Live_dashboard import upstox_live_feed as feed

print('JSON_PATH', feed.JSON_PATH)
print('BASE_DIR', feed.BASE_DIR)
print('EXISTS', os.path.exists(feed.JSON_PATH))

msg = {
    'type': 'live_feed',
    'feeds': {
        'NSE_FO|45259': {
            'fullFeed': {
                'marketFF': {
                    'ltpc': {'ltp': 0.15, 'ltt': '1787043034649', 'ltq': '65', 'cp': 0.25},
                    'marketLevel': {'bidAskQuote': [{'bidQ': '3250', 'bidP': 0.1, 'askQ': '520', 'askP': 0.15}]},
                    'optionGreeks': {'delta': 0.0007},
                    'marketOHLC': {'ohlc': [{'interval': '1d', 'open': 0.25, 'high': 0.3, 'low': 0.1, 'close': 0.15, 'vol': '50375', 'ts': '1786991400000'}]},
                    'atp': 0.19,
                    'vtt': '50375',
                    'oi': 43160.0
                }
            }
        }
    }
}

async def test():
    await feed.update_live_state_from_message(msg)

asyncio.run(test())

with open(feed.JSON_PATH, 'r', encoding='utf-8') as fh:
    print(json.dumps(json.load(fh), indent=2))

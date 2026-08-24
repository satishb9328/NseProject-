"""Standalone sanity check: loads the current Upstox token and fetches one quote."""
import json
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parent
TOKEN_FILE_PATHS = [
    ROOT / "Nse_Project" / "Brokers" / "upstox" / "upstox" / "upstox_token.json",
    ROOT / "Nse_Project" / "Brokers" / "upstox" / "upstox_v3_token.json",
    ROOT / "Nse_Project" / "Brokers" / "upstox" / "upstox_login_details.json",
    ROOT / "upstox" / "upstox_token.json",
]

candidates = []
for path in TOKEN_FILE_PATHS:
    if path.exists():
        data = json.loads(path.read_text(encoding="utf-8"))
        if data.get("access_token"):
            candidates.append((path.stat().st_mtime, path, data["access_token"]))

candidates.sort(reverse=True)
_, newest_path, token = candidates[0]
print(f"Using token from: {newest_path}")

headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
params = {"instrument_key": "NSE_EQ|INE434A01013"}  # AUBANK
resp = requests.get("https://api.upstox.com/v2/market-quote/quotes", headers=headers, params=params, timeout=15)

print(f"Status: {resp.status_code}")
print(resp.text)

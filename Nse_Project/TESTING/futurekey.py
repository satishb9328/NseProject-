import requests
from datetime import datetime

API_KEY = "0a02ae84-2ac8-4c54-974a-af1202562a43"
API_SECRET = "ca5k88sap1"
REDIRECT_URI = "https://eot8cga1pfpwz0t.m.pipedream.net"

print("1. Open this URL in your browser and log in:")
print(
    f"https://api.upstox.com/v2/login/authorization/dialog?response_type=code&client_id={API_KEY}&redirect_uri={REDIRECT_URI}")
print("\n2. After logging in, copy the 'code' from the redirected URL.")
code = input("3. Paste the code here and press Enter: ")

# 1. Get Access Token
print("\nFetching access token...")
url = "https://api.upstox.com/v2/login/authorization/token"
headers = {"Content-Type": "application/x-www-form-urlencoded", "Accept": "application/json"}
data = {"code": code, "client_id": API_KEY, "client_secret": API_SECRET, "grant_type": "authorization_code",
        "redirect_uri": REDIRECT_URI}
resp = requests.post(url, headers=headers, data=data)

if resp.status_code != 200:
    print("❌ Failed to get token. Check your code.")
    exit()

token = resp.json().get("access_token")

# 2. Fetch Instruments
print("Token received! Fetching active Nifty Future contract...")
url = "https://api.upstox.com/v2/instruments"
# CRITICAL FIX: No 'Accept' header here to prevent 400 Bad Request
headers = {"Authorization": f"Bearer {token}"}

response = requests.get(url, headers=headers, timeout=30)

if response.status_code == 200:
    data = response.json()
    today = datetime.now().strftime("%Y-%m-%d")

    nifty_futs = []
    for item in data:
        if (item.get('exchange') == 'NSE_FO' and
                item.get('name') == 'NIFTY' and
                item.get('segment') == 'FUT' and
                item.get('expiry') >= today):
            nifty_futs.append(item)

    nifty_futs.sort(key=lambda x: x['expiry'])

    if nifty_futs:
        active_fut = nifty_futs[0]
        print("\n" + "=" * 50)
        print("✅ FOUND ACTIVE NIFTY FUTURE CONTRACT:")
        print(f"Instrument Key : {active_fut['instrument_key']}")
        print(f"Expiry Date    : {active_fut['expiry']}")
        print(f"Trading Symbol : {active_fut['trading_symbol']}")
        print("=" * 50)
    else:
        print("❌ Could not find any active Nifty Future contract.")
else:
    print(f"❌ Error fetching data: {response.status_code}")
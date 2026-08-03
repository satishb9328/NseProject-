import requests

# Paste your code INSIDE these quotes
CODE = "NCZjM4"

API_KEY = "0a02ae84-2ac8-4c54-974a-af1202562a43"
API_SECRET = "ca5k88sap1"
REDIRECT_URI = "https://eot8cga1pfpwz0t.m.pipedream.net"

url = "https://api.upstox.com/v2/login/authorization/token"

headers = {
    "Content-Type": "application/x-www-form-urlencoded",
    "Accept": "application/json"
}

data = {
    "code": CODE,
    "client_id": API_KEY,
    "client_secret": API_SECRET,
    "grant_type": "authorization_code",
    "redirect_uri": REDIRECT_URI
}

print("Sending request to Upstox V2...")
response = requests.post(url, headers=headers, data=data)

print("\n--- RAW RESPONSE FROM UPSTOX ---")
print(response.text)
print("---------------------------------")
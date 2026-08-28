"""
Upstox Token Generator - Manual Code Entry
This script generates the login URL and lets you paste the authorization code manually
"""
import json
import time
import requests
import sys
import os
from datetime import datetime

# Load credentials (try multiple sources)
creds = {}
cred_paths = ['upstox_login_details.json', 'upstox/upstox_token.json', 'upstox_v3_token.json']
for path in cred_paths:
    if os.path.exists(path):
        try:
            with open(path, 'r') as f:
                data = json.load(f)
                if 'api_key' in data and 'api_secret' in data:
                    creds = data
                    break
        except Exception:
            pass

# Hardcoded fallback credentials (found in repo config)
if not creds or 'api_key' not in creds:
    creds = {
        'api_key': '0a02ae84-2ac8-4c54-974a-af1202562a43',
        'api_secret': 'ca5k88sap1'
    }
    print("⚠️  Using hardcoded credentials (credentials file not found or incomplete)")
else:
    print(f"✓ Loaded credentials from: {[p for p in cred_paths if os.path.exists(p) and 'api_key' in json.load(open(p))][0] if [p for p in cred_paths if os.path.exists(p) and 'api_key' in json.load(open(p))] else 'unknown'}")

API_KEY = creds['api_key']
API_SECRET = creds['api_secret']

# Common redirect URIs used by Upstox apps
REDIRECT_URI = "https://eot8cga1pfpwz0t.m.pipedream.net"  # User's specified redirect URL

def get_access_token(auth_code):
    """Exchange authorization code for access token (v3 endpoint with v2 fallback)"""
    # Try v3 first
    url_v3 = "https://api.upstox.com/v3/login/authorization/token"
    url_v2 = "https://api.upstox.com/v2/login/authorization/token"  # fallback

    headers = {
        "accept": "application/json",
        "Content-Type": "application/x-www-form-urlencoded"
    }

    data = {
        "code": auth_code,
        "client_id": API_KEY,
        "client_secret": API_SECRET,
        "redirect_uri": REDIRECT_URI,
        "grant_type": "authorization_code"
    }

    print(f"\n🔄 Requesting access token...")
    for url, version in [(url_v3, "v3"), (url_v2, "v2")]:
        try:
            response = requests.post(url, headers=headers, data=data, timeout=10)
            if response.status_code == 200:
                print(f"✅ Token obtained from {version} endpoint")
                return response.json()
            else:
                print(f"ℹ️  {version} endpoint returned {response.status_code}, trying next...")
        except Exception as e:
            print(f"ℹ️  {version} endpoint error: {e}, trying next...")
    
    print(f"❌ Failed to obtain token from both v3 and v2 endpoints")
    return None

def save_token(token_data):
    """Save token using centralized token manager (syncs to all files)"""
    access_token = token_data.get('access_token')
    expires_in = token_data.get('expires_in', 86400)  # Default 1 day

    # Use centralized token manager to sync to all locations
    try:
        from upstox_token_manager import save_v3_token
        success = save_v3_token(access_token, expires_in)
        
        if success:
            expiry_datetime = datetime.fromtimestamp(int(time.time()) + expires_in)
            print(f"\n✅ TOKEN SAVED AND SYNCED TO ALL LOCATIONS!")
            print(f"   Valid until: {expiry_datetime.strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"   Duration: {expires_in // 3600} hours ({expires_in // 86400} days)")
            return True
        else:
            print(f"\n❌ Failed to save token using centralized manager")
            return False
    except ImportError:
            # Fallback: manual write to the dynamic directory where this script lives
            print("⚠️  Could not import upstox_token_manager, using fallback save...")
            current_time = int(time.time())
            token_info = {
                "api_key": API_KEY,
                "api_secret": API_SECRET,
                "access_token": access_token,
                "fetched_at": current_time,
                "expires_in": expires_in,
                "expires_at": datetime.fromtimestamp(current_time + expires_in).isoformat(),
                "saved_at": datetime.now().isoformat()
            }
            
            # Dynamic path resolution (Resolves to current script's folder)
            current_dir = os.path.dirname(os.path.abspath(__file__))
            target_path = os.path.join(current_dir, "upstox_v3_token.json")
            
            try:
                os.makedirs(os.path.dirname(target_path), exist_ok=True)
                with open(target_path, 'w', encoding='utf-8') as f:
                    json.dump(token_info, f, indent=2)
                print(f"✓ Saved to: {target_path}")
                return True
            except Exception as e:
                print(f"❌ Failed to save to {target_path}: {e}")
                return False

def main():
    print("="*80)
    print("UPSTOX TOKEN GENERATOR - MANUAL METHOD")
    print("="*80)

    # Check if code is provided as argument
    if len(sys.argv) > 1:
        user_input = sys.argv[1]
        print(f"Using provided code: {user_input}")
    else:
        # Try v3 auth first, fallback to v2
        auth_url_v3 = (
            f"https://api.upstox.com/v3/login/authorization/dialog"
            f"?client_id={API_KEY}"
            f"&redirect_uri={REDIRECT_URI}"
            f"&response_type=code"
        )
        auth_url_v2 = (
            f"https://api.upstox.com/v2/login/authorization/dialog"
            f"?client_id={API_KEY}"
            f"&redirect_uri={REDIRECT_URI}"
            f"&response_type=code"
        )

        print("\n📋 STEP-BY-STEP INSTRUCTIONS:")
        print("="*80)
        print("\n1. Copy the URL below and paste it in your browser:")
        print(f"\n{auth_url_v3}\n")
        print("   (If the v3 URL fails, try the v2 fallback below)")
        print(f"\nV2 Fallback: {auth_url_v2}\n")
        print("-"*80)

        print("\n2. Login to your Upstox account")
        print("\n3. After authorization, you'll be redirected to a URL like:")
        print("   https://eot8cga1pfpwz0t.m.pipedream.net/?code=XXXXXX")
        print("   (The page may show an error - that's OK!)")

        print("\n4. Copy the ENTIRE URL from the browser address bar")
        print("   OR just copy the 'code' value after 'code='")

        print("\n" + "="*80)

        # Get authorization code from user
        user_input = input("\n📥 Paste the full redirect URL OR just the code here: ").strip()

    # Extract code if full URL was pasted
    if 'code=' in user_input:
        # Extract code from URL
        if '&' in user_input.split('code=')[1]:
            auth_code = user_input.split('code=')[1].split('&')[0]
        else:
            auth_code = user_input.split('code=')[1]
    else:
        auth_code = user_input

    print(f"\n✓ Authorization code received (length: {len(auth_code)})")

    # Exchange code for token
    token_data = get_access_token(auth_code)

    if token_data:
        # Save token
        print("\n💾 Saving token...")
        save_token(token_data)
        print("\n" + "="*80)
        print("✅ LOGIN COMPLETE - You can now use the API")
        print("="*80)
    else:
        print("\n❌ Failed to get access token")
        print("\n💡 TIP: Make sure you:")
        print("   - Copied the FULL authorization code")
        print("   - Used the code within 60 seconds (it expires quickly)")
        print("   - Used the correct redirect URI")

if __name__ == "__main__":
    main()
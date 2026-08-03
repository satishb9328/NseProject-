import os, time, json, datetime, sys, webbrowser
from urllib.parse import urlparse, parse_qs
from fyers_apiv3 import fyersModel

REQUIRED_CREDENTIAL_KEYS = {"api_key", "api_secret", "redirect_url"}

# --- Path Configurations ---
# Target path: MyProject2/config/fyers_config.json
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_DIR = os.path.join(BASE_DIR, "config")
CONFIG_FILE = os.path.join(CONFIG_DIR, "fyers_config.json")
TOKEN_DIR = os.path.join(BASE_DIR, "AccessToken")

os.makedirs(CONFIG_DIR, exist_ok=True)
os.makedirs(TOKEN_DIR, exist_ok=True)


def load_saved_credentials(filepath=CONFIG_FILE):
    if not os.path.exists(filepath):
        return None
    try:
        with open(filepath, "r") as f:
            credentials = json.load(f)
        if isinstance(credentials, dict) and REQUIRED_CREDENTIAL_KEYS.issubset(credentials.keys()):
            return credentials
        return None
    except Exception:
        return None


def prompt_for_credentials():
    print("---- Enter your Fyers Login Credentials ----")
    credentials = {
        "api_key": str(input("Enter API Key: ").strip()),
        "api_secret": str(input("Enter API Secret: ").strip()),
        "redirect_url": str(input("Enter Redirect URL: ").strip()),
        "access_token": ""
    }
    if input("Press Y to save login credentials: ").upper() == "Y":
        with open(CONFIG_FILE, "w") as f:
            json.dump(credentials, f, indent=4)
        print(f"✅ Saved credentials to '{CONFIG_FILE}'.")
    else:
        print("⚠️ Credentials not saved. Exiting.")
        sys.exit(1)
    return credentials


# 1. Load Credentials
login_credential = load_saved_credentials()
if login_credential is None:
    login_credential = prompt_for_credentials()

token_file = os.path.join(TOKEN_DIR, f"{datetime.datetime.now().date()}.json")

# 2. Check if today's access token already exists
if os.path.exists(token_file):
    with open(token_file, "r") as f:
        access_token = json.load(f)
    print("✅ Today's access token loaded from local cache.")
else:
    print("🔑 Generating new access token for today...")
    try:
        app_session = fyersModel.SessionModel(
            client_id=login_credential["api_key"],
            secret_key=login_credential["api_secret"],
            redirect_uri=login_credential["redirect_url"],
            response_type='code',
            grant_type="authorization_code"
        )
        auth_url = app_session.generate_authcode()
        print("Authorization URL:", auth_url)
        webbrowser.open(auth_url, new=2)

        auth_input = input("Paste the full redirected URL or auth code here: ").strip()
        if auth_input.lower().startswith("http"):
            parsed = urlparse(auth_input)
            params = parse_qs(parsed.query)
            auth_code = params.get("auth_code", [None])[0] or params.get("code", [None])[0]
        else:
            auth_code = auth_input

        app_session.set_token(auth_code)
        token_response = app_session.generate_token()

        if not token_response or "access_token" not in token_response:
            print(f"Error generating token: {token_response}")
            sys.exit(1)

        access_token = token_response["access_token"]

        # Cache daily token in AccessToken/ folder
        with open(token_file, "w") as f:
            json.dump(access_token, f)

    except Exception as e:
        print(f"Login Failed: {e}")
        sys.exit(1)

# 3. Always update config/fyers_config.json with the active access token
login_credential["access_token"] = access_token
with open(CONFIG_FILE, "w") as f:
    json.dump(login_credential, f, indent=4)

print(f"✅ Token synced to: {CONFIG_FILE}")
print(f"API Key: {login_credential['api_key']}")
print(f"Access Token: {access_token[:15]}... (Truncated for security)")
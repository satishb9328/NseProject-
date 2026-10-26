import requests
import json

print("=" * 60)
print("GAMMA ENGINE LIVE STATE CHECK")
print("=" * 60)

try:
    resp = requests.get("http://127.0.0.1:8080/gamma/status/", timeout=10)
    data = resp.json()
except Exception as e:
    print(f"FAILED to fetch /gamma/status/: {e}")
    exit(1)

status = data.get("status", data)

print(f"\n[1] Bot State:")
print(f"    running:            {status.get('running')}")
print(f"    processed_updates:  {status.get('processed_updates')}")
print(f"    last_error:         {status.get('last_error')}")
print(f"    redis_url:          {status.get('redis_url')}")
print(f"    data_key:           {status.get('data_key')}")
print(f"    channel:            {status.get('channel')}")

print(f"\n[2] Last Profile:")
lp = status.get("last_profile")
if lp is None:
    print("    >>> last_profile is None — engine has NOT processed any data")
elif isinstance(lp, dict):
    print(f"    timestamp:          {lp.get('timestamp')}")
    print(f"    spot:               {lp.get('spot')}")
    print(f"    expiry:             {lp.get('expiry')}")
    print(f"    scalp_enabled:      {lp.get('scalp_enabled')}")
    print(f"    scalp_signals:      {lp.get('scalp_signals')}")
    print(f"    weekly_gamma keys:  {list(lp.get('weekly_gamma', {}).keys())[:10]}")
    print(f"    day_gamma keys:     {list(lp.get('day_gamma', {}).keys())[:10]}")
else:
    print(f"    Unexpected type: {type(lp)}")

print(f"\n[3] Recent Alerts (last 5):")
alerts = status.get("gamma_alerts", [])
if not alerts:
    print("    (none)")
else:
    for a in alerts[-5:]:
        print(f"    {a.get('timestamp', '?')}: {a.get('message', '?')[:80]}")

print("\n" + "=" * 60)
print("DIAGNOSIS")
print("=" * 60)
running = status.get("running")
processed = status.get("processed_updates", 0)
last_error = status.get("last_error")
lp = status.get("last_profile")

if not running:
    print(">>> BOT IS NOT RUNNING — apps.py daemon thread failed to start it")
    print(">>> Check: Is GAMMA app in INSTALLED_APPS?")
    print(">>> Check: Did Django call GAMMAConfig.ready()?")
elif last_error:
    print(f">>> BOT HAS ERROR: {last_error}")
    print(">>> The engine is failing silently — check Django logs")
elif processed == 0:
    print(">>> BOT IS RUNNING BUT HAS PROCESSED 0 UPDATES")
    print(">>> Bot is alive but never received any Redis messages")
    print(">>> Check: Is bot subscribed to the right channel?")
    print(f">>> Expected channel: {status.get('channel')}")
    print(">>> Check: Is Redis pub/sub working?")
elif lp is None:
    print(">>> BOT HAS PROCESSED UPDATES BUT last_profile IS NONE")
    print(">>> This is a bug in the engine — should never happen")
elif lp.get("scalp_enabled") is False:
    print(">>> LAST PROFILE EXISTS BUT SCALP IS DISABLED")
    print(">>> GAMMA_SCALP_MODE=on env var didn't reach the engine")
elif lp.get("scalp_signals") is None:
    print(">>> SCALP ENGINE NOT INSTANTIATED")
    print(">>> Bot started before env var was set, OR env var didn't propagate")
else:
    print(">>> EVERYTHING LOOKS HEALTHY")
    print(">>> If dashboard still shows '--', the issue is in the frontend")

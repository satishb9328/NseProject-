import json
import sys

try:
    with open('gamma_status.json') as f:
        data = json.load(f)
except Exception as e:
    print(f"FAILED to read gamma_status.json: {e}")
    print("Did you run: curl -s http://127.0.0.1:8080/gamma/status/ > gamma_status.json")
    sys.exit(1)

status = data.get('status', data)

print("=" * 60)
print("GAMMA BOT STATE")
print("=" * 60)
print(f"running:            {status.get('running')}")
print(f"processed_updates:  {status.get('processed_updates')}")
print(f"last_error:         {status.get('last_error')}")
print(f"last_profile:       {'YES' if status.get('last_profile') else 'NONE'}")
print()

if status.get('last_profile'):
    lp = status['last_profile']
    print("=== LAST PROFILE ===")
    print(f"timestamp:          {lp.get('timestamp')}")
    print(f"spot:               {lp.get('spot')}")
    print(f"expiry:             {lp.get('expiry')}")
    print(f"scalp_enabled:      {lp.get('scalp_enabled')}")
    scalp = lp.get('scalp_signals')
    if scalp:
        print(f"scalp bias:         {scalp.get('bias')}")
        print(f"scalp score:        {scalp.get('bias_score')}")
        print(f"active_setup:       {scalp.get('active_setup')}")
    else:
        print("scalp_signals:      NONE")
else:
    print(">>> last_profile is None - bot has NOT processed any data yet")
    print(">>> This means: Gamma bot is NOT running")
    print()

print("=" * 60)
print("DIAGNOSIS")
print("=" * 60)
running = status.get('running')
processed = status.get('processed_updates', 0)
last_error = status.get('last_error')
lp = status.get('last_profile')

if not running:
    print(">>> BOT IS NOT RUNNING")
    print(">>> apps.py ready() did NOT start the bot under Uvicorn")
    print(">>> Apply the updated apps.py fix (bulletproof version)")
elif last_error:
    print(f">>> BOT HAS ERROR: {last_error}")
elif processed == 0:
    print(">>> BOT IS RUNNING BUT HAS PROCESSED 0 UPDATES")
    print(">>> Bot is alive but never received Redis messages")
elif lp is None:
    print(">>> BOT HAS PROCESSED UPDATES BUT last_profile IS NONE")
else:
    print(">>> EVERYTHING LOOKS HEALTHY")
    print(">>> If dashboard still shows '--', issue is in the frontend")

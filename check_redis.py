import redis
import time
import json

print("=" * 60)
print("REDIS LIVE DATA CHECK")
print("=" * 60)

r = redis.Redis(host='127.0.0.1', port=6379, db=0)

# Check 1: Is the live_options_data key populated?
data = r.get('live_options_data')
data_len = len(data) if data else 0
print(f"\n[1] live_options_data key length: {data_len}")
if data:
    print(f"    First 300 chars: {data[:300]}")
else:
    print("    >>> KEY IS EMPTY - feed producer has never published")

# Check 2: Are messages being published right now?
print("\n[2] Listening on channel 'gamma:live_options_updated' for 5 seconds...")
pubsub = r.pubsub()
pubsub.subscribe('gamma:live_options_updated')
timeout = time.time() + 5
msg_count = 0
while time.time() < timeout:
    msg = pubsub.get_message(timeout=1)
    if msg and msg.get('type') == 'message':
        msg_count += 1
        msg_data = msg.get('data', '')
        print(f"    Message {msg_count}: length={len(msg_data)}")
print(f"    Total messages in 5s: {msg_count}")
if msg_count == 0:
    print("    >>> NO MESSAGES - feed producer is NOT running")

# Check 3: List all Redis keys (to see what's actually in there)
print("\n[3] All Redis keys (first 30):")
keys = list(r.scan_iter('*', count=30))
for k in keys[:30]:
    ktype = r.type(k)
    ksize = r.dbsize() if ktype == b'none' else '?'
    print(f"    {k.decode() if isinstance(k, bytes) else k} ({ktype.decode() if isinstance(ktype, bytes) else ktype})")

print("\n" + "=" * 60)
print("DIAGNOSIS")
print("=" * 60)
if data_len == 0 and msg_count == 0:
    print(">>> REDIS HAS NO LIVE OPTIONS DATA")
    print(">>> FEED PRODUCER IS NOT RUNNING")
    print(">>> You need to start your Upstox live feed script")
elif data_len > 0 and msg_count == 0:
    print(">>> Redis has cached data but feed producer is OFFLINE")
    print(">>> Gamma engine should still have last_profile from cache")
elif data_len > 0 and msg_count > 0:
    print(">>> FEED IS ALIVE - data is flowing")
    print(">>> If Gamma cards still show '--', issue is elsewhere")
else:
    print(">>> Unclear state - check the outputs above")

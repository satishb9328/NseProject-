"""Regression tests for IV plumbing and strict NIFTY-50 symbol filtering.

Standalone (no pytest). Run with:

    .venv\\Scripts\\python.exe Nse_Project\\TESTING\\iv_symbol_filter_test.py

Covers:
  Findings 2 & 4  extract_live_values pulls `iv`; update_instrument_state stores it as
                  PERCENT so the engine's `iv / 100` yields the original fraction, and
                  _repair_iv_surface no longer falls back to a flat synthetic 15%.
  Finding 5       find_nifty_weekly_option_keys blocks NIFTYNXT50 / NIFTYFPI / etc.
"""
import asyncio
import math
import re
import sys
from pathlib import Path
from unittest import mock

ROOT_DIR = Path(__file__).resolve().parent.parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from Nse_Project.GAMMA import gamma_blast_engine as g  # noqa: E402
from Nse_Project.Live_dashboard import upstox_live_feed as feed  # noqa: E402

PASSED = 0
FAILED = 0


def check(label, actual, expected):
    global PASSED, FAILED
    ok = (
        math.isclose(actual, expected, rel_tol=1e-9)
        if isinstance(actual, float) and isinstance(expected, float)
        else actual == expected
    )
    if ok:
        PASSED += 1
        print(f"  [PASS] {label}: {actual!r}")
    else:
        FAILED += 1
        print(f"  [FAIL] {label}: expected {expected!r}, got {actual!r}")


# Verbatim shape of a live Upstox V3 option tick (iv is a FRACTION).
def option_tick(iv=0.19287109375, oi=13130.0):
    return {"fullFeed": {"marketFF": {
        "ltpc": {"ltp": 90.5, "ltt": "1787646291725", "ltq": "65", "cp": 88.0},
        "marketOHLC": {"ohlc": [{"interval": "1d", "open": 0.2, "vol": "29445"}]},
        "atp": 0.21, "vtt": "29445", "oi": oi, "iv": iv, "tbq": 2470.0, "tsq": 11570.0,
    }}, "requestMode": "full_d5"}


def test_extract_iv() -> None:
    print("\nF2a) extract_live_values() pulls iv off the tick")
    v = feed.extract_live_values(option_tick(), instrument_key="NSE_FO|1")
    check("iv extracted as the raw fraction", v["iv"], 0.19287109375)
    check("oi still extracted", v["oi"], 13130.0)
    check("iv absent -> None", feed.extract_live_values({"ltpc": {"ltp": 1.0}})["iv"], None)
    check("IV_FRACTION_TO_PERCENT", feed.IV_FRACTION_TO_PERCENT, 100.0)


def test_iv_stored_as_percent() -> None:
    print("\nF2b) update_instrument_state() stores iv as PERCENT")
    feed.volume_history.clear()
    feed.live_state["instrument_data"] = {}
    feed.INSTRUMENT_METADATA.clear()
    key = "NSE_FO|1"
    feed.INSTRUMENT_METADATA[key] = {
        "strike": 24300, "option_type": "CE", "tradingsymbol": "NIFTY26AUG24300CE"}
    feed.previous_oi_map.clear()
    feed.previous_oi_map[key] = 500.0 * feed.LOT_SIZE

    async def scenario(tick):
        with mock.patch.object(feed, "write_live_state", new=mock.AsyncMock()):
            values = feed.extract_live_values(tick, instrument_key=key)
            await feed.update_instrument_state(key, values)

    asyncio.run(scenario(option_tick(iv=0.19287109375)))
    entry = feed.live_state["instrument_data"][key]
    check("entry['iv'] is percent", entry["iv"], 19.287109375)
    # Round-trip through the engine's `iv / 100` must recover the original fraction.
    check("engine iv/100 recovers the fraction", entry["iv"] / 100, 0.19287109375)

    # A zero/absent IV must not be written (so _repair_iv_surface can interpolate).
    feed.live_state["instrument_data"] = {}
    asyncio.run(scenario(option_tick(iv=0.0)))
    check("iv=0 not stored", "iv" in feed.live_state["instrument_data"][key], False)


def test_engine_uses_real_iv() -> None:
    print("\nF2c) The Gamma engine now runs on real IV, not the flat 15% default")
    lot = feed.LOT_SIZE
    payload = {"expiry_date": "2026-09-01", "spot_price": 24334.55, "instrument_data": {
        "NSE_FO|1": {"strike": 24300, "option_type": "CE", "oi": 1000.0 * lot,
                     "coi": 200.0 * lot, "iv": 19.287109375, "ltp": 90.0, "volume": 5000},
        "NSE_FO|2": {"strike": 24300, "option_type": "PE", "oi": 900.0 * lot,
                     "coi": -150.0 * lot, "iv": 21.5, "ltp": 70.0, "volume": 4000},
    }}
    norm = g.normalize_live_payload(payload)
    rows = {r["strike_price"]: r for r in norm["data"]}
    check("call_iv carried through normalize", rows[24300.0]["call_iv"], 19.287109375)
    check("put_iv carried through normalize", rows[24300.0]["put_iv"], 21.5)

    profile = g.GammaProcessor("NIFTY").process_payload(norm)
    s = profile.strikes[0]
    check("call_iv NOT the 15.0 default", s.call_iv, 19.287109375)
    check("put_iv NOT the 15.0 default", s.put_iv, 21.5)
    check("sigma used is ~19.3%", round(s.call_iv / 100, 6), 0.192871)
    check("call/put vol differ (real skew)", s.call_iv != s.put_iv, True)

    # Without iv the surface still collapses to the flat default - proves the contrast.
    bare = {"expiry_date": "2026-09-01", "spot_price": 24334.55, "instrument_data": {
        "NSE_FO|1": {"strike": 24300, "option_type": "CE", "oi": 1000.0 * lot, "coi": 0.0},
        "NSE_FO|2": {"strike": 24300, "option_type": "PE", "oi": 900.0 * lot, "coi": 0.0},
    }}
    flat = g.GammaProcessor("NIFTY").process_payload(g.normalize_live_payload(bare))
    check("no-iv payload still falls back to 15.0", flat.strikes[0].call_iv, 15.0)


def test_symbol_regex() -> None:
    print("\nF5a) The strict NIFTY-50 symbol pattern")
    pat = re.compile(feed.NIFTY50_OPTION_SYMBOL_RE)
    accept = [
        "NIFTY26AUG24300CE", "NIFTY26AUG26650PE",   # monthly-style code
        "NIFTY2590224300CE", "NIFTY25O0724300PE",   # weekly-style codes
    ]
    reject = [
        "NIFTYNXT5026AUG68600CE", "NIFTYNXT5026AUG80800PE",
        "NIFTYFPIAUG261320CE", "NIFTYFPI26AUG1820PE",
        "BANKNIFTY26AUG52000CE", "MIDCPNIFTY26OCT15225PE",
        "FINNIFTY26AUG23000CE", "NIFTY26AUG24300XX",  # bad suffix
        "NIFTYAUG2624300CE",                          # letter right after NIFTY
    ]
    for s in accept:
        check(f"ACCEPT {s}", bool(pat.match(s)), True)
    for s in reject:
        check(f"REJECT {s}", bool(pat.match(s)), False)


def test_master_file_filter() -> None:
    print("\nF5b) find_nifty_weekly_option_keys() excludes sibling indices end-to-end")
    import gzip
    import io as _io

    import pandas as pd

    # Miniature instrument master mirroring the real column layout.
    rows = [
        # --- genuine NIFTY 50 (should survive) ---
        ("NSE_FO|1", "NIFTY26SEP24300CE", "NIFTY", "2026-09-01", 24300.0, "OPTIDX", "CE"),
        ("NSE_FO|2", "NIFTY26SEP24300PE", "NIFTY", "2026-09-01", 24300.0, "OPTIDX", "PE"),
        ("NSE_FO|3", "NIFTY26SEP24400CE", "NIFTY", "2026-09-01", 24400.0, "OPTIDX", "CE"),
        # --- contaminants (must be dropped) ---
        ("NSE_FO|4", "NIFTYNXT5026SEP68600CE", "NIFTYNXT50", "2026-09-01", 68600.0, "OPTIDX", "CE"),
        ("NSE_FO|5", "NIFTYNXT5026SEP80800PE", "NIFTYNXT50", "2026-09-01", 80800.0, "OPTIDX", "PE"),
        ("NSE_FO|6", "NIFTYFPI26SEP1320CE", "NIFTYFPI", "2026-09-01", 1320.0, "OPTIDX", "CE"),
        ("NSE_FO|7", "BANKNIFTY26SEP52000CE", "BANKNIFTY", "2026-09-01", 52000.0, "OPTIDX", "CE"),
        ("NSE_FO|8", "MIDCPNIFTY26SEP15225PE", "MIDCPNIFTY", "2026-09-01", 15225.0, "OPTIDX", "PE"),
        ("NSE_FO|9", "FINNIFTY26SEP23000CE", "FINNIFTY", "2026-09-01", 23000.0, "OPTIDX", "CE"),
    ]
    df = pd.DataFrame(rows, columns=[
        "instrument_key", "tradingsymbol", "name", "expiry", "strike",
        "instrument_type", "option_type"])
    buf = _io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb") as gz:
        gz.write(df.to_csv(index=False).encode("utf-8"))
    payload = buf.getvalue()

    class FakeResponse:
        content = payload

        def raise_for_status(self):
            return None

    # Freeze "now" well before the 2026-09-01 expiry so it is still tradable.
    from datetime import date as _date
    with mock.patch.object(feed.requests, "get", return_value=FakeResponse()), \
            mock.patch.object(feed, "earliest_tradable_expiry", lambda *a: _date(2026, 8, 26)):
        expiry, keys = feed.find_nifty_weekly_option_keys()

    check("resolved expiry", expiry, _date(2026, 9, 1))
    check("exactly the 3 NIFTY 50 keys survive", sorted(keys),
          ["NSE_FO|1", "NSE_FO|2", "NSE_FO|3"])
    syms = sorted(feed.INSTRUMENT_METADATA[k]["tradingsymbol"] for k in keys)
    check("metadata symbols are all NIFTY 50", syms,
          ["NIFTY26SEP24300CE", "NIFTY26SEP24300PE", "NIFTY26SEP24400CE"])
    for bad in ("NSE_FO|4", "NSE_FO|5", "NSE_FO|6", "NSE_FO|7", "NSE_FO|8", "NSE_FO|9"):
        check(f"{bad} excluded", bad in keys, False)
    check("no NIFTYNXT50 strike leaked",
          any(feed.INSTRUMENT_METADATA[k]["strike"] > 40000 for k in keys), False)


if __name__ == "__main__":
    test_extract_iv()
    test_iv_stored_as_percent()
    test_engine_uses_real_iv()
    test_symbol_regex()
    test_master_file_filter()

    print(f"\n{'=' * 60}")
    print(f"PASSED: {PASSED}   FAILED: {FAILED}")
    print(f"{'=' * 60}")
    sys.exit(1 if FAILED else 0)

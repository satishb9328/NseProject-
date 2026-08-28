"""Validation for the refactored POI / COI / Volume-Velocity logic.

Standalone (no pytest needed), matching the convention of the other scripts in
this folder. Run with:

    .venv\\Scripts\\python.exe Nse_Project\\TESTING\\poi_velocity_test.py

Covers:
  1. fetch_previous_oi_map() parses `market_data.prev_oi` from an option-chain payload.
  2. extract_live_values() returns poi=None on a real V3 `full_d5` tick (no `poi` field)
     and picks `poi` up automatically if Upstox ever adds it.
  3. compute_volume_velocity() produces correct shares/sec over the rolling window.
  4. update_instrument_state() writes coi/coi_pct/call_velocity/put_velocity.
"""
import asyncio
import sys
from datetime import date
from pathlib import Path
from unittest import mock

ROOT_DIR = Path(__file__).resolve().parent.parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from Nse_Project.Live_dashboard import upstox_live_feed as feed  # noqa: E402

PASSED = 0
FAILED = 0


def check(label: str, actual, expected) -> None:
    global PASSED, FAILED
    if actual == expected:
        PASSED += 1
        print(f"  [PASS] {label}: {actual!r}")
    else:
        FAILED += 1
        print(f"  [FAIL] {label}: expected {expected!r}, got {actual!r}")


# Exact shape of the documented Upstox /v2/option/chain response.
OPTION_CHAIN_PAYLOAD = {
    "status": "success",
    "data": [
        {
            "expiry": "2026-08-27",
            "strike_price": 26000,
            "call_options": {
                "instrument_key": "NSE_FO|11111",
                "market_data": {"ltp": 120.5, "volume": 900, "oi": 750, "prev_oi": 500},
            },
            "put_options": {
                "instrument_key": "NSE_FO|22222",
                "market_data": {"ltp": 80.25, "volume": 700, "oi": 640, "prev_oi": 800},
            },
        },
        {
            "expiry": "2026-08-27",
            "strike_price": 26100,
            # prev_oi missing on the call leg -> must be skipped, not defaulted to 0.
            "call_options": {
                "instrument_key": "NSE_FO|33333",
                "market_data": {"ltp": 60.0, "volume": 100, "oi": 300},
            },
            "put_options": {
                "instrument_key": "NSE_FO|44444",
                "market_data": {"ltp": 95.0, "volume": 200, "oi": 400, "prev_oi": 0},
            },
        },
    ],
}

# A real tick captured from live_options_data.json (requestMode full_d5).
REAL_V3_TICK = {
    "fullFeed": {
        "marketFF": {
            "ltpc": {"ltp": 0.1, "ltt": "1787646291725", "ltq": "65", "cp": 0.15},
            "marketOHLC": {"ohlc": [{"interval": "1d", "vol": "29445"}]},
            "atp": 0.21,
            "vtt": "29445",
            "oi": 13130.0,
            "iv": 2.8125,
            "tbq": 2470.0,
            "tsq": 11570.0,
        }
    },
    "requestMode": "full_d5",
}


def test_fetch_previous_oi_map() -> None:
    print("\n1) fetch_previous_oi_map() parses prev_oi from the option chain")
    response = mock.Mock(status_code=200)
    response.json.return_value = OPTION_CHAIN_PAYLOAD

    with mock.patch.object(feed, "read_access_token", return_value="dummy-token"), \
            mock.patch.object(feed.requests, "get", return_value=response) as mock_get:
        poi_map = feed.fetch_previous_oi_map(date(2026, 8, 27))

    lot = feed.LOT_SIZE
    check("single HTTP call for the whole chain", mock_get.call_count, 1)
    check("instruments with usable prev_oi", len(poi_map), 3)
    check("call prev_oi scaled by LOT_SIZE", poi_map["NSE_FO|11111"], 500.0 * lot)
    check("put prev_oi scaled by LOT_SIZE", poi_map["NSE_FO|22222"], 800.0 * lot)
    check("prev_oi of 0 is retained", poi_map["NSE_FO|44444"], 0.0)
    check("missing prev_oi is skipped", "NSE_FO|33333" in poi_map, False)


def test_extract_live_values_poi_sniff() -> None:
    print("\n2) extract_live_values() defensive poi sniff")
    values = feed.extract_live_values(REAL_V3_TICK, instrument_key="NSE_FO|65769")
    check("oi read from the real tick", values["oi"], 13130.0)
    check("poi absent in V3 full_d5 -> None", values["poi"], None)
    check("'poi' lookup did not alias onto 'oi'", values["poi"] is values["oi"], False)

    # Simulate Upstox adding previous OI back under an eFeedDetails wrapper.
    future_tick = {"fullFeed": {"marketFF": {"eFeedDetails": {"oi": 13130.0, "poi": 9000.0}}}}
    future = feed.extract_live_values(future_tick, instrument_key="NSE_FO|65769")
    check("poi auto-detected when present", future["poi"], 9000.0)


def test_compute_volume_velocity() -> None:
    print("\n3) compute_volume_velocity() rolling shares/sec")
    feed.volume_history.clear()
    key = "NSE_FO|VEL"
    clock = {"t": 1000.0}

    with mock.patch.object(feed.time, "monotonic", side_effect=lambda: clock["t"]):
        # First sample: no elapsed time yet, so no velocity.
        check("first tick yields None", feed.compute_volume_velocity(key, 1_000_000), None)

        # +2s and +100000 shares -> 50000 shares/sec.
        clock["t"] = 1002.0
        check("100k shares over 2s", feed.compute_volume_velocity(key, 1_100_000), 50_000.0)

        # +1s and +200000 shares. Window is 3s so the t=1000 sample is still the
        # oldest in range: 300000 shares over 3s = 100000/sec.
        clock["t"] = 1003.0
        check("300k shares over the 3s window", feed.compute_volume_velocity(key, 1_300_000), 100_000.0)

        # Flat volume across a fresh window -> zero velocity, never negative.
        clock["t"] = 1010.0
        feed.compute_volume_velocity(key, 1_300_000)
        clock["t"] = 1013.0
        check("flat volume -> 0 shares/sec", feed.compute_volume_velocity(key, 1_300_000), 0.0)

        # A vtt reset must not emit a huge negative velocity.
        clock["t"] = 1016.0
        check("vtt reset -> None (not negative)", feed.compute_volume_velocity(key, 5.0), None)


def test_update_instrument_state() -> None:
    print("\n4) update_instrument_state() writes COI + velocity")
    feed.volume_history.clear()
    feed.live_state["instrument_data"] = {}
    lot = feed.LOT_SIZE

    call_key, put_key = "NSE_FO|11111", "NSE_FO|22222"
    feed.INSTRUMENT_METADATA.clear()
    feed.INSTRUMENT_METADATA[call_key] = {
        "strike": 26000, "option_type": "CE", "tradingsymbol": "NIFTY26AUG26000CE",
    }
    feed.INSTRUMENT_METADATA[put_key] = {
        "strike": 26000, "option_type": "PE", "tradingsymbol": "NIFTY26AUG26000PE",
    }
    # POI map holds internal-scale values, exactly as fetch_previous_oi_map returns.
    feed.previous_oi_map.clear()
    feed.previous_oi_map.update({call_key: 500.0 * lot, put_key: 800.0 * lot})

    clock = {"t": 2000.0}

    async def scenario():
        with mock.patch.object(feed, "write_live_state", new=mock.AsyncMock()), \
                mock.patch.object(feed.time, "monotonic", side_effect=lambda: clock["t"]):
            # Call side: live OI 750 contracts vs previous 500 -> COI = +250 contracts.
            await feed.update_instrument_state(call_key, {
                "ltp": 120.5, "cp": 118.0, "oi": 750.0, "poi": None,
                "volume": 1_000_000, "raw": {},
            })
            # Put side: live OI 640 vs previous 800 -> COI = -160 contracts (unwinding).
            await feed.update_instrument_state(put_key, {
                "ltp": 80.25, "cp": 82.0, "oi": 640.0, "poi": None,
                "volume": 2_000_000, "raw": {},
            })
            clock["t"] = 2003.0
            await feed.update_instrument_state(call_key, {
                "ltp": 121.0, "cp": 118.0, "oi": 750.0, "poi": None,
                "volume": 1_240_000, "raw": {},
            })
            await feed.update_instrument_state(put_key, {
                "ltp": 80.0, "cp": 82.0, "oi": 640.0, "poi": None,
                "volume": 2_030_000, "raw": {},
            })

    asyncio.run(scenario())

    call = feed.live_state["instrument_data"][call_key]
    put = feed.live_state["instrument_data"][put_key]

    check("call oi (internal scale)", call["oi"], 750.0 * lot)
    check("call poi (internal scale)", call["poi"], 500.0 * lot)
    check("call coi = oi - poi", call["coi"], 250.0 * lot)
    check("call coi_pct", call["coi_pct"], 50.0)
    check("call poi_source", call["poi_source"], "option_chain")
    check("put coi is negative (unwind)", put["coi"], -160.0 * lot)
    check("put coi_pct", put["coi_pct"], -20.0)

    # 240k shares over 3s = 80000/sec (explosive, above the 50k dashboard threshold).
    check("call_velocity", call["call_velocity"], 80_000.0)
    check("call side has no put_velocity", "put_velocity" in call, False)
    # 30k shares over 3s = 10000/sec (calm).
    check("put_velocity", put["put_velocity"], 10_000.0)
    check("put side has no call_velocity", "call_velocity" in put, False)


def test_tick_poi_takes_precedence() -> None:
    print("\n5) A poi on the tick overrides the REST snapshot")
    feed.volume_history.clear()
    feed.live_state["instrument_data"] = {}
    lot = feed.LOT_SIZE
    key = "NSE_FO|11111"
    feed.INSTRUMENT_METADATA.clear()
    feed.INSTRUMENT_METADATA[key] = {
        "strike": 26000, "option_type": "CE", "tradingsymbol": "NIFTY26AUG26000CE",
    }
    feed.previous_oi_map.clear()
    feed.previous_oi_map[key] = 500.0 * lot  # REST says 500

    async def scenario():
        with mock.patch.object(feed, "write_live_state", new=mock.AsyncMock()):
            await feed.update_instrument_state(key, {
                "ltp": 120.5, "cp": 118.0, "oi": 750.0,
                "poi": 700.0,  # the tick says 700 -> must win
                "volume": 10, "raw": {},
            })

    asyncio.run(scenario())
    entry = feed.live_state["instrument_data"][key]
    check("poi taken from the tick", entry["poi"], 700.0 * lot)
    check("poi_source is 'tick'", entry["poi_source"], "tick")
    check("coi uses the tick poi", entry["coi"], 50.0 * lot)


def test_legacy_symbols_removed() -> None:
    print("\n6) Legacy baseline machinery is gone")
    for name in (
        "initialize_baseline", "fetch_yesterday_baseline", "fetch_yesterday_baseline_oi",
        "load_baseline_data", "save_baseline_data", "get_baseline_cache_path",
        "get_baseline_oi_for_instrument", "resolve_previous_trading_date",
        "_extract_candles", "_candle_date", "_candle_oi", "_historical_candle_exists",
        "load_offline_oi_from_participant_csv", "BASELINE_CACHE_DIR",
        "NSE_MARKET_HOLIDAYS", "FALLBACK_TO_LIVE_OI_BASELINE", "PARTICIPANT_OI_DIR",
        "get_yesterday_date",
    ):
        check(f"{name} removed", hasattr(feed, name), False)


if __name__ == "__main__":
    test_fetch_previous_oi_map()
    test_extract_live_values_poi_sniff()
    test_compute_volume_velocity()
    test_update_instrument_state()
    test_tick_poi_takes_precedence()
    test_legacy_symbols_removed()

    print(f"\n{'=' * 60}")
    print(f"PASSED: {PASSED}   FAILED: {FAILED}")
    print(f"{'=' * 60}")
    sys.exit(1 if FAILED else 0)

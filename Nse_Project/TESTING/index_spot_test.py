"""Regression tests for the live index SPOT pipeline.

Standalone (no pytest). Run with:

    .venv\\Scripts\\python.exe Nse_Project\\TESTING\\index_spot_test.py

The payloads below are VERBATIM captures from the live Upstox V3 feed. Note that a
real index tick carries marketOHLC.ohlc[1].open = 24260.05 (the current minute's
opening level) alongside the live ltpc.ltp = 24334.55. Picking up the former is
exactly what makes a dashboard look frozen, so it is asserted against explicitly.
"""
import asyncio
import sys
from pathlib import Path
from unittest import mock

ROOT_DIR = Path(__file__).resolve().parent.parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from Nse_Project.Live_dashboard import upstox_live_feed as feed  # noqa: E402

LIVE_LTP = 24334.55      # the real live index level
PREV_CLOSE = 24219.05    # ltpc.cp  -- must never be used as spot
MINUTE_OPEN = 24260.05   # marketOHLC I1 open -- the value that froze the dashboard
DAY_OPEN = 24175.75

OHLC = {"ohlc": [
    {"interval": "1d", "open": DAY_OPEN, "high": LIVE_LTP, "low": 24115.45,
     "close": LIVE_LTP, "ts": "1787596200000"},
    {"interval": "I1", "open": MINUTE_OPEN, "high": LIVE_LTP, "low": MINUTE_OPEN,
     "close": LIVE_LTP, "ts": "1787651880000"},
]}

FULL_INDEX_TICK = {"fullFeed": {"indexFF": {
    "ltpc": {"ltp": LIVE_LTP, "ltt": "1787651947000", "cp": PREV_CLOSE},
    "marketOHLC": OHLC}}, "requestMode": "full_d5"}

LTPC_INDEX_TICK = {"ltpc": {"ltp": LIVE_LTP, "ltt": "1787651947000", "cp": PREV_CLOSE}}

# Degenerate tick: protobuf drops ltp when it is the 0.0 default.
NO_LTP_TICK = {"fullFeed": {"indexFF": {
    "ltpc": {"ltt": "1787651947000", "cp": PREV_CLOSE},
    "marketOHLC": OHLC}}, "requestMode": "full_d5"}

ONLY_OHLC_TICK = {"fullFeed": {"indexFF": {"marketOHLC": OHLC}}}

PASSED = 0
FAILED = 0


def check(label, actual, expected):
    global PASSED, FAILED
    if actual == expected:
        PASSED += 1
        print(f"  [PASS] {label}: {actual!r}")
    else:
        FAILED += 1
        print(f"  [FAIL] {label}: expected {expected!r}, got {actual!r}")


def test_extract_index_spot() -> None:
    print("\n1) extract_index_spot() takes ltpc.ltp and nothing else")
    check("full mode -> live ltp", feed.extract_index_spot(FULL_INDEX_TICK), LIVE_LTP)
    check("ltpc mode -> live ltp", feed.extract_index_spot(LTPC_INDEX_TICK), LIVE_LTP)
    check("never returns the minute-candle open",
          feed.extract_index_spot(FULL_INDEX_TICK) == MINUTE_OPEN, False)
    check("never returns the previous close",
          feed.extract_index_spot(FULL_INDEX_TICK) == PREV_CLOSE, False)
    check("ltp missing -> None (not cp)", feed.extract_index_spot(NO_LTP_TICK), None)
    check("only marketOHLC -> None", feed.extract_index_spot(ONLY_OHLC_TICK), None)
    check("garbage -> None", feed.extract_index_spot("nope"), None)
    check("empty -> None", feed.extract_index_spot({}), None)


def test_is_index_key() -> None:
    print("\n2) _is_index_key() routing")
    check("exact key", feed._is_index_key("NSE_INDEX|Nifty 50"), True)
    check("whitespace tolerated", feed._is_index_key("  NSE_INDEX|Nifty 50  "), True)
    check("case tolerated", feed._is_index_key("nse_index|nifty 50"), True)
    check("other index still routed as index", feed._is_index_key("NSE_INDEX|India VIX"), True)
    check("option key is not an index", feed._is_index_key("NSE_FO|65769"), False)
    check("None is not an index", feed._is_index_key(None), False)


def test_subscription_includes_index() -> None:
    print("\n3) build_streamer() puts the index in the subscription list")
    captured = {}

    class FakeStreamer:
        Mode = {"LTPC": "ltpc", "FULL": "full_d5", "OPTION": "option_greeks", "D30": "full_d30"}
        Event = {"OPEN": "open", "MESSAGE": "message", "ERROR": "error", "CLOSE": "close"}

        def __init__(self, api_client=None, instrumentKeys=None, mode=None):
            captured["instrumentKeys"] = list(instrumentKeys or [])
            captured["mode"] = mode
            captured["subscribed"] = []

        def on(self, event, handler):
            captured.setdefault("handlers", {})[event] = handler

        def subscribe(self, keys, mode):
            captured["subscribed"].append((list(keys), mode))

        def auto_reconnect(self, *a, **k):
            pass

    option_keys = ["NSE_FO|111", "NSE_FO|222"]
    with mock.patch.object(feed, "MarketDataStreamerV3", FakeStreamer), \
            mock.patch.object(feed, "Configuration", mock.Mock()), \
            mock.patch.object(feed, "ApiClient", mock.Mock()):
        import datetime
        feed.build_streamer("tok", option_keys, datetime.date(2026, 8, 27), None)
        captured["handlers"]["open"]()   # simulate socket open / reconnect

    keys = captured["instrumentKeys"]
    check("index present in constructor keys", feed.INDEX_INSTRUMENT_KEY in keys, True)
    check("all option keys present", all(k in keys for k in option_keys), True)
    check("no duplicates", len(keys), len(set(keys)))
    check("index subscribed on open",
          any(feed.INDEX_INSTRUMENT_KEY in sub for sub, _ in captured["subscribed"]), True)


def test_update_index_spot() -> None:
    print("\n4) update_index_spot() writes root-level spot_price")
    for key in ("spot_price", "underlying_ltp", "underlying_spot", "index_ltp", "nifty_ltp"):
        feed.live_state[key] = None

    async def scenario(tick):
        with mock.patch.object(feed, "write_live_state", new=mock.AsyncMock()):
            values = feed.extract_live_values(tick, instrument_key=feed.INDEX_INSTRUMENT_KEY)
            await feed.update_index_spot(values)

    asyncio.run(scenario(FULL_INDEX_TICK))
    check("spot_price", feed.live_state["spot_price"], LIVE_LTP)
    check("underlying_ltp alias", feed.live_state["underlying_ltp"], LIVE_LTP)
    check("underlying_spot alias", feed.live_state["underlying_spot"], LIVE_LTP)
    check("index_ltp alias", feed.live_state["index_ltp"], LIVE_LTP)
    check("nifty_ltp alias", feed.live_state["nifty_ltp"], LIVE_LTP)
    check("spot_source", feed.live_state["spot_source"], "index_tick_ltp")
    check("spot_updated_at set", isinstance(feed.live_state["spot_updated_at"], str), True)
    check("raw index tick retained for diagnosis",
          feed.live_state["index_raw_sample"] is not None, True)
    check("index NOT added to instrument_data",
          feed.INDEX_INSTRUMENT_KEY in feed.live_state.get("instrument_data", {}), False)

    # A degenerate tick must not overwrite a good spot with cp / candle values.
    asyncio.run(scenario(NO_LTP_TICK))
    check("bad tick keeps the last good spot", feed.live_state["spot_price"], LIVE_LTP)


def test_routing_end_to_end() -> None:
    print("\n5) A feeds batch routes the index to spot, options to instrument_data")
    feed.live_state["instrument_data"] = {}
    feed.live_state["spot_price"] = None
    feed.previous_oi_map.clear()
    feed.volume_history.clear()
    feed.INSTRUMENT_METADATA.clear()
    feed.INSTRUMENT_METADATA["NSE_FO|111"] = {
        "strike": 24300, "option_type": "CE", "tradingsymbol": "NIFTY26AUG24300CE"}

    message = {"type": "live_feed", "feeds": {
        feed.INDEX_INSTRUMENT_KEY: FULL_INDEX_TICK,
        "NSE_FO|111": {"fullFeed": {"marketFF": {
            "ltpc": {"ltp": 55.5, "cp": 50.0}, "vtt": "12345", "oi": 900.0}}},
    }}

    async def scenario():
        with mock.patch.object(feed, "write_live_state", new=mock.AsyncMock()):
            await feed.update_live_state_from_message(message)

    asyncio.run(scenario())
    check("spot came from the index tick", feed.live_state["spot_price"], LIVE_LTP)
    check("option landed in instrument_data",
          "NSE_FO|111" in feed.live_state["instrument_data"], True)
    check("index did NOT land in instrument_data",
          feed.INDEX_INSTRUMENT_KEY in feed.live_state["instrument_data"], False)
    check("option ltp parsed", feed.live_state["instrument_data"]["NSE_FO|111"]["ltp"], 55.5)


def test_frontend_and_api_wiring() -> None:
    print("\n6) API + frontend expose and consume spot_price")
    import io
    views_src = io.open(ROOT_DIR / "Nse_Project" / "Live_dashboard" / "views.py",
                        encoding="utf-8").read()
    check('API returns "spot_price"', '"spot_price": _json_safe_value(spot_price)' in views_src, True)
    check('API returns "spot_updated_at"', '"spot_updated_at"' in views_src, True)

    for name in ("dashboard.html", "live_signals.html"):
        html = io.open(ROOT_DIR / "Nse_Project" / "Master_Temp" / name, encoding="utf-8").read()
        check(f"{name}: id=live-spot-price exists", 'id="live-spot-price"' in html, True)
        check(f"{name}: reads data.spot_price", "data.spot_price" in html, True)
        check(f"{name}: updateSpotPrice defined", "function updateSpotPrice(" in html, True)
        check(f"{name}: updateSpotPrice called in poll", "updateSpotPrice(data)" in html, True)
        check(f"{name}: gamma spot cannot clobber index spot", "haveIndexSpot" in html, True)


if __name__ == "__main__":
    test_extract_index_spot()
    test_is_index_key()
    test_subscription_includes_index()
    test_update_index_spot()
    test_routing_end_to_end()
    test_frontend_and_api_wiring()

    print(f"\n{'=' * 60}")
    print(f"PASSED: {PASSED}   FAILED: {FAILED}")
    print(f"{'=' * 60}")
    sys.exit(1 if FAILED else 0)

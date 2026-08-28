"""Regression tests for the cross-expiry / single-instance / stale-publish / POI-base fixes.

Standalone (no pytest). Run with:

    venv\\Scripts\\python.exe Nse_Project\\TESTING\\cross_expiry_guard_test.py

Covers:
  1. expiry_utils resolves weekly/monthly Upstox expiry codes and only ever reports a
     conflict when it can PROVE two instruments belong to different chains.
  2. build_option_chain_rows() / normalize_live_payload() key rows by
     (expiry_date, strike_price), so a payload holding two expiries can no longer
     overwrite or merge rows.
  3. acquire_single_instance_lock() lets exactly ONE feed process run.
  4. dump_live_state() refuses to force-publish a stale/disconnected snapshot to Redis.
  5. Only the active subscribed expiry is serialized, plus the optional
     `live_options_data:{expiry}` namespaced mirror key.
  6. The previous-OI baseline is persisted and re-hydrated, so COI never drops to
     None/0 across a feed restart.
"""
import asyncio
import contextlib
import io
import json
import os
import sys
import tempfile
from datetime import date
from pathlib import Path
from unittest import mock

ROOT_DIR = Path(__file__).resolve().parent.parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "Nse_Project.settings")

import django  # noqa: E402

django.setup()

from Nse_Project import expiry_utils as ex  # noqa: E402
from Nse_Project.GAMMA import gamma_blast_engine as g  # noqa: E402
from Nse_Project.Live_dashboard import upstox_live_feed as feed  # noqa: E402
from Nse_Project.Live_dashboard import views  # noqa: E402

PASSED = 0
FAILED = 0

ACTIVE = "2026-09-01"      # weekly code 26901
FOREIGN = "2026-09-08"     # weekly code 26908


def check(label, actual, expected):
    global PASSED, FAILED
    if actual == expected:
        PASSED += 1
        print(f"  [PASS] {label}: {actual!r}")
    else:
        FAILED += 1
        print(f"  [FAIL] {label}: expected {expected!r}, got {actual!r}")


def option(strike, side, expiry_code, **extra):
    """Build an instrument_data entry that carries its expiry ONLY in the symbol."""
    entry = {
        "strike": strike,
        "option_type": side,
        "tradingsymbol": f"NIFTY{expiry_code}{strike}{side}",
        "raw_sample": {},
    }
    entry.update(extra)
    return entry


def test_expiry_utils() -> None:
    print("\n1) expiry_utils parses and compares Upstox expiry codes")
    check("weekly code", ex.parse_symbol_expiry("NIFTY2690125750PE"), "2026-09-01")
    check("weekly Oct code", ex.parse_symbol_expiry("NIFTY25O0724300PE"), "2025-10-07")
    check("monthly code -> month scope", ex.parse_symbol_expiry("NIFTY26AUG24300CE"), "2026-08")
    check("sibling index is unknown", ex.parse_symbol_expiry("NIFTYNXT5026AUG68600CE"), None)
    check("date object", ex.normalize_expiry(date(2026, 9, 1)), "2026-09-01")
    check("timestamp string", ex.normalize_expiry("2026-09-01T15:30:00+05:30"), "2026-09-01")
    check("garbage string", ex.normalize_expiry("not-a-date"), None)

    check("different weeks conflict", ex.expiries_conflict(ACTIVE, FOREIGN), True)
    check("month scope matches its own month", ex.expiries_conflict("2026-09", ACTIVE), False)
    check("other month conflicts", ex.expiries_conflict("2026-08", ACTIVE), True)
    check("unknown never conflicts", ex.expiries_conflict(None, ACTIVE), False)
    check("identical never conflicts", ex.expiries_conflict(ACTIVE, ACTIVE), False)

    # Both legs of one strike must land in the SAME bucket, foreign ones must not.
    ce = option(26000, "CE", "26901")
    pe = option(26000, "PE", "26901")
    far = option(26000, "CE", "26908")
    check("CE/PE share a bucket", ex.expiry_group_key(ce, ACTIVE), ex.expiry_group_key(pe, ACTIVE))
    check("active bucket is the active expiry", ex.expiry_group_key(ce, ACTIVE), ACTIVE)
    check("foreign keeps its own bucket", ex.expiry_group_key(far, ACTIVE), FOREIGN)
    check("unstamped row inherits the active bucket",
          ex.expiry_group_key({"strike": 26000}, ACTIVE), ACTIVE)
    check("belongs_to_expiry: active", ex.belongs_to_expiry(ce, ACTIVE), True)
    check("belongs_to_expiry: foreign", ex.belongs_to_expiry(far, ACTIVE), False)
    check("belongs_to_expiry: unknown is kept", ex.belongs_to_expiry({"strike": 1}, ACTIVE), True)


def test_dashboard_rows_keyed_by_expiry() -> None:
    print("\n2a) build_option_chain_rows keys rows by (expiry_date, strike_price)")
    lot = views.LOT_SIZE
    # The exact corruption shape: SAME strike, one leg per expiry. With a strike-only key
    # the 2026-09-08 PE overwrote the 2026-09-01 PE and COI mixed two chains.
    instrument_data = {
        "NSE_FO|1": option(26000, "CE", "26901", ltp=120.5, oi=750.0 * lot, poi=500.0 * lot,
                           coi=250.0 * lot, coi_pct=50.0, volume=1_000),
        "NSE_FO|2": option(26000, "PE", "26901", ltp=80.25, oi=640.0 * lot, poi=800.0 * lot,
                           coi=-160.0 * lot, coi_pct=-20.0, volume=2_000),
        "NSE_FO|3": option(26000, "PE", "26908", ltp=999.0, oi=111.0 * lot, poi=1.0 * lot,
                           coi=110.0 * lot, coi_pct=11000.0, volume=3_000),
    }

    rows = views.build_option_chain_rows(
        instrument_data, target_symbol="NIFTY", strike_bounds=(25000.0, 27000.0),
        expiry_date=ACTIVE,
    )
    check("foreign expiry dropped -> 1 row", len(rows), 1)
    check("call leg from the active expiry", rows[0]["call_coi"], 250)
    check("put leg NOT overwritten by 2026-09-08", rows[0]["put_coi"], -160)
    check("put ltp is the active expiry's", rows[0]["put_ltp"], 80.25)
    # The row payload must stay exactly the 15 dashboard columns.
    check("row shape unchanged", "expiry" in rows[0], False)

    # Without an active expiry the two chains must still not merge: each keeps its own row.
    rows = views.build_option_chain_rows(
        instrument_data, target_symbol="NIFTY", strike_bounds=(25000.0, 27000.0),
    )
    check("no active expiry -> one row per expiry", len(rows), 2)
    check("both put legs survive independently",
          sorted(row["put_coi"] for row in rows), [-160, 110])
    check("call leg was not duplicated",
          [row["call_coi"] for row in rows].count(250), 1)


def test_engine_rows_keyed_by_expiry() -> None:
    print("\n2b) normalize_live_payload keys rows by (expiry_date, strike_price)")
    lot = feed.LOT_SIZE
    payload = {
        "expiry_date": ACTIVE,
        "spot_price": 26010.0,
        "instrument_data": {
            "NSE_FO|1": option(26000, "CE", "26901", oi=1000.0 * lot, coi=200.0 * lot,
                               iv=19.0, ltp=90.0, volume=5000),
            "NSE_FO|2": option(26000, "PE", "26901", oi=900.0 * lot, coi=-150.0 * lot,
                               iv=21.5, ltp=70.0, volume=4000),
            "NSE_FO|3": option(26000, "PE", "26908", oi=5.0 * lot, coi=999.0 * lot,
                               iv=80.0, ltp=1.0, volume=1),
        },
    }
    norm = g.normalize_live_payload(payload)
    check("only the active expiry survives", len(norm["data"]), 1)
    check("foreign rows counted", norm["foreign_expiry_rows"], 1)
    check("foreign expiries reported", norm["foreign_expiries"], [FOREIGN])
    row = norm["data"][0]
    check("strike_price stays a float", row["strike_price"], 26000.0)
    check("row carries its expiry", row["expiry"], ACTIVE)
    check("put_coi from the active expiry", row["put_coi"], -150.0 * lot)
    check("put_iv not polluted", row["put_iv"], 21.5)

    # Two expiries with no active expiry declared -> separate rows, never merged.
    payload_no_expiry = {k: v for k, v in payload.items() if k != "expiry_date"}
    norm = g.normalize_live_payload(payload_no_expiry)
    check("two expiries -> two rows", len(norm["data"]), 2)
    check("rows sorted deterministically",
          [r["expiry"] for r in norm["data"]], [ACTIVE, FOREIGN])


def test_single_instance_lock() -> None:
    print("\n3) Only ONE feed instance can hold the lock")
    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, "nested", "upstox_live_feed.lock")
        feed._instance_lock_handle = None

        check("first instance acquires", feed.acquire_single_instance_lock(path), True)
        check("lock file created", os.path.exists(path), True)
        check("our PID recorded", feed._read_lock_pid(path), str(os.getpid()))
        check("re-entrant call is a no-op success", feed.acquire_single_instance_lock(path), True)

        # Simulate a SECOND process: forget our handle (the OS lock stays held by it) and
        # try to acquire again through a brand new file handle.
        first_handle = feed._instance_lock_handle
        feed._instance_lock_handle = None
        check("second instance is refused", feed.acquire_single_instance_lock(path), False)
        check("holder PID still readable", feed._read_lock_pid(path), str(os.getpid()))

        # Release, then prove a restart can take the lock again (no stale-file deadlock).
        feed._instance_lock_handle = first_handle
        feed.release_single_instance_lock()
        check("release is idempotent", feed.release_single_instance_lock(), None)
        check("restart re-acquires", feed.acquire_single_instance_lock(path), True)
        feed.release_single_instance_lock()
        check("handle cleared after release", feed._instance_lock_handle, None)


def _run_one_dump_iteration():
    """Run exactly one iteration of dump_live_state(); return (disk_writes, publishes)."""
    async def scenario():
        calls = {"n": 0}

        async def fake_sleep(_seconds):
            calls["n"] += 1
            if calls["n"] > 1:
                raise asyncio.CancelledError

        with mock.patch.object(feed.asyncio, "sleep", fake_sleep), \
                mock.patch.object(feed, "atomic_write_json", mock.Mock()) as writer, \
                mock.patch.object(feed, "publish_live_state", new=mock.AsyncMock()) as publisher:
            with contextlib.suppress(asyncio.CancelledError):
                await feed.dump_live_state()
            return writer.call_count, publisher.await_count

    return asyncio.run(scenario())


def test_dump_does_not_publish_stale_state() -> None:
    print("\n4) dump_live_state() never force-publishes a stale/disconnected snapshot")
    feed.feed_health.update({
        "connected": True, "last_tick_monotonic": None, "last_tick_at": None,
        "stale_publish_skips": 0,
    })
    feed._last_redis_publish_ts = 123.0  # pretend a live snapshot was already published

    # a) Connected but no tick has ever arrived -> stale.
    check("no ticks -> not live", feed.is_feed_live(), False)
    writes, publishes = _run_one_dump_iteration()
    check("cold backup still written", writes, 1)
    check("no forced publish while stale", publishes, 0)
    check("live_state flagged stale", feed.live_state["stale"], True)
    check("feed_status explains why", feed.live_state["feed_status"], "stale_no_ticks")

    # b) A fresh tick -> live again, so the forced publish resumes.
    feed.mark_tick_received()
    check("fresh tick -> live", feed.is_feed_live(), True)
    writes, publishes = _run_one_dump_iteration()
    check("forced publish resumes", publishes, 1)
    check("live_state no longer stale", feed.live_state["stale"], False)
    check("feed_status live", feed.live_state["feed_status"], "live")

    # c) Socket closed -> stale immediately, even though the last tick is recent.
    feed.mark_socket_disconnected("1006")
    check("disconnected -> not live", feed.is_feed_live(), False)
    writes, publishes = _run_one_dump_iteration()
    check("no publish after disconnect", publishes, 0)
    check("feed_status carries the close code",
          feed.live_state["feed_status"], "disconnected:1006")

    # d) Tick older than STALE_FEED_SECONDS -> stale.
    feed.mark_socket_connected()
    feed.feed_health["last_tick_monotonic"] = feed.time.monotonic() - (feed.STALE_FEED_SECONDS + 5)
    check("aged tick -> not live", feed.is_feed_live(), False)
    _, publishes = _run_one_dump_iteration()
    check("no publish for an aged snapshot", publishes, 0)

    # e) Nothing published yet at all -> seed Redis once so consumers are not left empty.
    feed._last_redis_publish_ts = 0.0
    _, publishes = _run_one_dump_iteration()
    check("startup snapshot is seeded once", publishes, 1)
    feed._last_redis_publish_ts = 0.0


def test_publish_only_active_expiry() -> None:
    print("\n5) Only the active subscribed expiry is serialized to Redis")
    feed.live_state["expiry_date"] = ACTIVE
    feed.INSTRUMENT_METADATA.clear()
    feed.INSTRUMENT_METADATA["NSE_FO|1"] = {
        "strike": 26000, "option_type": "CE", "tradingsymbol": "NIFTY2690126000CE",
        "expiry": date(2026, 9, 1),
    }
    # A stale entry from the previous chain - exactly what a rollover or a restored
    # snapshot can leave behind; it carries no `expiry` stamp of its own.
    feed.live_state["instrument_data"] = {
        "NSE_FO|1": {"strike": 26000, "option_type": "CE", "tradingsymbol": "NIFTY2690126000CE", "oi": 10.0},
        "NSE_FO|9": {"strike": 26000, "option_type": "PE", "tradingsymbol": "NIFTY2690826000PE", "oi": 99.0},
    }

    check("active expiry resolved", feed.active_expiry_iso(), ACTIVE)
    kept, foreign = feed.sanitized_instrument_data(ACTIVE)
    check("only the active instrument kept", list(kept), ["NSE_FO|1"])
    check("foreign instrument counted", foreign, {FOREIGN: 1})

    payload, active = feed.snapshot_for_publish()
    check("payload expiry", active, ACTIVE)
    check("payload holds one instrument", payload["instrument_count"], 1)
    check("foreign count published", payload["foreign_expiry_instruments"], 1)
    check("live_state itself untouched", len(feed.live_state["instrument_data"]), 2)

    # The namespaced mirror key is opt-in, and the primary key must remain the LAST SET.
    async def publish_with(namespace: bool):
        fake = mock.Mock()
        feed._last_redis_publish_ts = 0.0
        with mock.patch.object(feed, "redis_client", fake), \
                mock.patch.object(feed, "REDIS_EXPIRY_NAMESPACE", namespace):
            await feed.publish_live_state(force=True)
        return [call.args[0] for call in fake.set.call_args_list], fake.publish.call_args[0]

    keys, published = asyncio.run(publish_with(False))
    check("default: exactly one SET", keys, [feed.REDIS_DATA_KEY])
    check("channel notified", published, (feed.REDIS_CHANNEL, "updated"))

    keys, _ = asyncio.run(publish_with(True))
    check("namespaced mirror written first", keys,
          [f"{feed.REDIS_DATA_KEY}:{ACTIVE}", feed.REDIS_DATA_KEY])
    feed._last_redis_publish_ts = 0.0


def test_poi_base_persistence() -> None:
    print("\n6) The POI baseline survives a feed restart")
    expiry = date(2026, 9, 1)
    lot = feed.LOT_SIZE
    baseline = {"NSE_FO|1": 500.0 * lot, "NSE_FO|2": 800.0 * lot}

    with tempfile.TemporaryDirectory() as tmp:
        with mock.patch.object(feed, "POI_SNAPSHOT_DIR", tmp):
            feed.persist_previous_oi(expiry, baseline)
            check("baseline file written", os.path.exists(feed.poi_snapshot_path(expiry)), True)
            check("round-trips exactly", feed.load_persisted_previous_oi(expiry), baseline)
            check("wrong expiry rejected", feed.load_persisted_previous_oi(date(2026, 9, 8)), {})

            # Restart: the map starts empty and must be re-hydrated from disk.
            feed.previous_oi_map.clear()
            check("re-hydrated count", feed.hydrate_previous_oi(expiry), 2)
            check("map populated before the first tick", feed.previous_oi_map, baseline)
            check("provenance recorded", feed.live_state["poi_source"], "persisted_baseline")

            # A failed REST refresh must fall back to the persisted baseline, not to None.
            feed.previous_oi_map.clear()
            with mock.patch.object(feed, "fetch_previous_oi_map", return_value={}):
                feed.refresh_previous_oi(expiry)
            check("failed refresh still yields a baseline", feed.previous_oi_map, baseline)

    # No baseline file at all: recover POI from the restored snapshot entries.
    with tempfile.TemporaryDirectory() as tmp:
        with mock.patch.object(feed, "POI_SNAPSHOT_DIR", tmp):
            feed.previous_oi_map.clear()
            feed.live_state["instrument_data"] = {
                "NSE_FO|1": {"strike": 26000, "option_type": "CE", "poi": 500.0 * lot},
            }
            check("recovered from the restored snapshot", feed.hydrate_previous_oi(expiry), 1)
            check("provenance recorded", feed.live_state["poi_source"], "restored_snapshot_poi")

    # A tick arriving while previous_oi_map is empty must RETAIN the known poi, so coi is
    # never published as None/0 during the restart window.
    feed.previous_oi_map.clear()
    feed.volume_history.clear()
    feed.INSTRUMENT_METADATA.clear()
    feed.INSTRUMENT_METADATA["NSE_FO|1"] = {
        "strike": 26000, "option_type": "CE", "tradingsymbol": "NIFTY2690126000CE",
        "expiry": expiry,
    }
    feed.live_state["instrument_data"] = {
        "NSE_FO|1": {"strike": 26000, "option_type": "CE",
                     "tradingsymbol": "NIFTY2690126000CE", "poi": 500.0 * lot},
    }

    async def tick():
        with mock.patch.object(feed, "write_live_state", new=mock.AsyncMock()):
            await feed.update_instrument_state("NSE_FO|1", {
                "ltp": 120.5, "cp": 118.0, "oi": 750.0, "poi": None, "volume": 10, "raw": {},
            })

    asyncio.run(tick())
    entry = feed.live_state["instrument_data"]["NSE_FO|1"]
    check("poi retained across the gap", entry["poi"], 500.0 * lot)
    check("poi_source flags the retention", entry["poi_source"], "retained")
    check("coi still computed (not None)", entry["coi"], 250.0 * lot)
    check("coi_pct still computed", entry["coi_pct"], 50.0)
    check("expiry stamped on the entry", entry["expiry"], ACTIVE)


def test_missing_expiry_is_unknown_not_foreign() -> None:
    print("\n7) NaN / NaT / blank expiries are UNKNOWN, never a foreign expiry")
    import numpy as np
    import pandas as pd

    # pandas' NaT is a datetime SUBCLASS, so a naive isinstance(value, datetime) branch
    # returned the string "NaT" - a truthy token that compared as a real expiry and made
    # every instrument carrying it look foreign (silently dropped, CE/PE rows split).
    check("pd.NaT -> unknown", ex.normalize_expiry(pd.NaT), None)
    check("np.datetime64 NaT -> unknown", ex.normalize_expiry(np.datetime64("NaT")), None)
    check("np.nan -> unknown", ex.normalize_expiry(np.nan), None)
    check("float nan -> unknown", ex.normalize_expiry(float("nan")), None)
    check("'NaT' string -> unknown", ex.normalize_expiry("NaT"), None)
    check("'nan' string -> unknown", ex.normalize_expiry("nan"), None)
    check("blank string -> unknown", ex.normalize_expiry("   "), None)
    check("real date still parsed", ex.normalize_expiry(date(2026, 9, 1)), ACTIVE)

    check("NaT never conflicts", ex.expiries_conflict(pd.NaT, ACTIVE), False)
    check("nan never conflicts", ex.expiries_conflict(np.nan, ACTIVE), False)
    check("NaT symbol -> unknown", ex.parse_symbol_expiry(np.nan), None)
    check("NaT row belongs to the active expiry",
          ex.belongs_to_expiry({"expiry": pd.NaT, "tradingsymbol": "NIFTY2690126000CE"}, ACTIVE), True)

    # The visible regression: a CE stamped NaT and a PE stamped None, same strike and same
    # real chain, must produce ONE merged row - not two half-rows, and not zero rows.
    lot = views.LOT_SIZE
    polluted = {
        "NSE_EQ|1": {"tradingsymbol": "RELIANCE", "strike": np.nan, "expiry": np.nan, "oi": 5.0},
        "NSE_FO|4": {"tradingsymbol": "MIDCPNIFTY2690115225PE", "strike": 15225.0,
                     "option_type": "PE", "expiry": np.nan, "oi": 1.0},
        "NSE_FO|1": {"tradingsymbol": "NIFTY2690126000CE", "strike": 26000, "option_type": "CE",
                     "expiry": pd.NaT, "oi": 750.0 * lot, "poi": 500.0 * lot, "volume": 10},
        "NSE_FO|2": {"tradingsymbol": "NIFTY2690126000PE", "strike": 26000, "option_type": "PE",
                     "expiry": None, "oi": 640.0 * lot, "poi": 800.0 * lot, "volume": 20},
        "bad-entry": "not-a-dict",
    }
    rows = views.build_option_chain_rows(
        polluted, "NIFTY", (25000.0, 27000.0), expiry_date=ACTIVE)
    check("CE(NaT) + PE(None) merge into ONE row", len(rows), 1)
    check("call leg kept", rows[0]["call_coi"], 250)
    check("put leg kept", rows[0]["put_coi"], -160)
    check("equity row ignored", rows[0]["strike"], "26000")
    rows = views.build_option_chain_rows(polluted, "NIFTY", (25000.0, 27000.0))
    check("still one row without an active expiry", len(rows), 1)
    check("a NaT expiry_date argument is treated as unknown",
          len(views.build_option_chain_rows(polluted, "NIFTY", (25000.0, 27000.0),
                                           expiry_date=pd.NaT)), 1)

    # The engine must behave identically.
    norm = g.normalize_live_payload({
        "expiry_date": ACTIVE, "spot_price": 26010.0, "instrument_data": polluted,
    })
    check("engine merges the NaT/None legs too", len(norm["data"]), 1)
    check("engine dropped nothing as foreign", norm["foreign_expiry_rows"], 0)
    check("engine rejected the leaked MIDCPNIFTY strike", norm["foreign_underlying_rows"], 1)
    check("engine kept both legs", (norm["data"][0]["call_oi"] > 0, norm["data"][0]["put_oi"] > 0),
          (True, True))
    # Equities and stock options must never reach the gamma aggregates either.
    leaked = g.normalize_live_payload({"expiry_date": ACTIVE, "spot_price": 26010.0,
                                      "instrument_data": {
        "NSE_FO|1": option(26000, "CE", "26901", oi=100.0, coi=1.0),
        "NSE_FO|8": {"tradingsymbol": "RELIANCE26SEP3000CE", "strike": 3000,
                     "option_type": "CE", "oi": 50.0, "coi": 1.0},
        "NSE_FO|6": {"tradingsymbol": "NIFTYNXT5026SEP68600CE", "strike": 68600,
                     "option_type": "CE", "oi": 50.0, "coi": 1.0},
        "NSE_FO|5": {"tradingsymbol": "BANKNIFTY2690152000CE", "strike": 52000,
                     "option_type": "CE", "oi": 50.0, "coi": 1.0},
    }})
    check("stock option + NIFTYNXT50 + BANKNIFTY all rejected",
          leaked["foreign_underlying_rows"], 3)
    check("only the NIFTY strike is aggregated",
          [row["strike_price"] for row in leaked["data"]], [26000.0])


# A master file shaped like the REAL one: equities/ETFs/bonds carry NO expiry, NO strike
# and NO option_type, so those cells arrive as NaN.
MASTER_ROWS = [
    ("NSE_EQ|INE002A01018", "RELIANCE", "RELIANCE INDUSTRIES", "", "", "EQ", "", ""),
    ("NSE_EQ|INE0ABC01011", "GOI2032", "7.26% GOI 2032", "", "", "SG", "", ""),
    ("NSE_EQ|INE0XYZ01019", "NIFTYBEES", "NIPPON ETF", "", "", "EQ", "", ""),
    ("NSE_FO|1", "NIFTY2690126000CE", "NIFTY", "2026-09-01", 26000.0, "OPTIDX", "CE", 65),
    ("NSE_FO|2", "NIFTY2690126000PE", "NIFTY", "2026-09-01", 26000.0, "OPTIDX", "PE", 65),
    ("NSE_FO|3", "NIFTY26SEP26000CE", "NIFTY", "2026-09-29", 26000.0, "OPTIDX", "CE", 65),
    ("NSE_FO|4", "MIDCPNIFTY2690115225PE", "MIDCPNIFTY", "2026-09-01", 15225.0, "OPTIDX", "PE", 120),
    ("NSE_FO|5", "BANKNIFTY2690152000CE", "BANKNIFTY", "2026-09-01", 52000.0, "OPTIDX", "CE", 35),
    ("NSE_FO|6", "NIFTYNXT5026SEP68600CE", "NIFTYNXT50", "2026-09-01", 68600.0, "OPTIDX", "CE", 25),
    ("NSE_FO|7", "NIFTY26SEPFUT", "NIFTY", "2026-09-29", "", "FUTIDX", "", 65),
    ("NSE_FO|8", "RELIANCE26SEP3000CE", "RELIANCE", "2026-09-29", 3000.0, "OPTSTK", "CE", 250),
    # Hostile NIFTY OPTIDX rows: unparseable expiry, and a missing strike.
    ("NSE_FO|9", "NIFTY2690226000CE", "NIFTY", "not-a-date", 26000.0, "OPTIDX", "CE", 65),
    ("NSE_FO|10", "NIFTY2690126050CE", "NIFTY", "2026-09-01", "", "OPTIDX", "CE", 65),
    # option_type missing but the symbol still says CE -> must NOT become the string "NAN".
    ("NSE_FO|11", "NIFTY2690126100CE", "NIFTY", "2026-09-01", 26100.0, "OPTIDX", "", 65),
]


def test_master_list_filtered_before_parsing() -> None:
    print("\n8) The master list is strictly filtered BEFORE any expiry/strike parsing")
    import gzip
    import io as _io

    import pandas as pd

    df = pd.DataFrame(MASTER_ROWS, columns=[
        "instrument_key", "tradingsymbol", "name", "expiry", "strike",
        "instrument_type", "option_type", "lot_size"])
    buf = _io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb") as gz:
        gz.write(df.to_csv(index=False).encode("utf-8"))
    payload = buf.getvalue()

    class FakeResponse:
        content = payload

        def raise_for_status(self):
            return None

    parsed_frames = []
    real_to_datetime = pd.to_datetime

    def spy_to_datetime(values, *args, **kwargs):
        # Record how many values ever reach the date parser: it must be the small NIFTY
        # OPTIDX subset, never the whole exchange (equities have a NaN expiry).
        try:
            parsed_frames.append(len(values))
        except TypeError:
            parsed_frames.append(1)
        return real_to_datetime(values, *args, **kwargs)

    with mock.patch.object(feed.requests, "get", return_value=FakeResponse()), \
            mock.patch.object(feed, "earliest_tradable_expiry", lambda *a: date(2026, 8, 26)), \
            mock.patch.object(feed.pd, "to_datetime", spy_to_datetime):
        expiry, keys = feed.find_nifty_weekly_option_keys()

    check("expiry resolved from the clean subset", expiry, date(2026, 9, 1))
    check("only valid NIFTY 50 keys survive", sorted(keys), ["NSE_FO|1", "NSE_FO|11", "NSE_FO|2"])
    check("expiry parsed on the 6-row NIFTY subset, not the whole file",
          parsed_frames, [6])
    check("equities / bonds / ETFs never reach the parser",
          all(size < len(MASTER_ROWS) for size in parsed_frames), True)

    for bad, why in (("NSE_EQ|INE002A01018", "equity"), ("NSE_EQ|INE0ABC01011", "bond"),
                     ("NSE_EQ|INE0XYZ01019", "ETF"), ("NSE_FO|4", "MIDCPNIFTY"),
                     ("NSE_FO|5", "BANKNIFTY"), ("NSE_FO|6", "NIFTYNXT50"),
                     ("NSE_FO|7", "index future"), ("NSE_FO|8", "stock option"),
                     ("NSE_FO|9", "unparseable expiry"), ("NSE_FO|10", "NaN strike")):
        check(f"{why} excluded", bad in keys, False)

    metas = [feed.INSTRUMENT_METADATA[key] for key in keys]
    check("no NaN/NaT survived into metadata",
          all(ex.normalize_expiry(meta["expiry"]) == ACTIVE for meta in metas), True)
    check("strikes are real numbers",
          sorted(meta["strike"] for meta in metas), [26000, 26000, 26100])
    check("option_type is never the string 'NAN'",
          sorted({meta["option_type"] for meta in metas}), ["CE", "PE"])
    check("missing option_type recovered from the symbol",
          feed.INSTRUMENT_METADATA["NSE_FO|11"]["option_type"], "CE")

    # An all-junk master file must degrade to (None, []) rather than raise.
    junk = pd.DataFrame([
        ("NSE_EQ|1", "RELIANCE", "RELIANCE INDUSTRIES", "", "", "EQ", "", ""),
    ], columns=["instrument_key", "tradingsymbol", "name", "expiry", "strike",
                "instrument_type", "option_type", "lot_size"])
    buf = _io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb") as gz:
        gz.write(junk.to_csv(index=False).encode("utf-8"))

    class JunkResponse:
        content = buf.getvalue()

        def raise_for_status(self):
            return None

    with mock.patch.object(feed.requests, "get", return_value=JunkResponse()):
        result = feed.find_nifty_weekly_option_keys()
    check("equities-only master file -> (None, []) with no exception", result, (None, []))


def test_api_never_returns_a_500() -> None:
    print("\n9) live_option_chain_api() degrades instead of raising a 500")
    from django.test import RequestFactory

    factory = RequestFactory()

    def call():
        response = views.live_option_chain_api(factory.get("/live-dashboard/api/live-options/"))
        return response.status_code, json.loads(response.content)

    status, payload = call()
    check("happy path status", status, 200)
    check("happy path has no error", payload["error"], None)
    happy_keys = sorted(payload)

    # Any failure inside the payload builder must be caught, logged and reported in-band:
    # dashboard.html turns a non-200 into a bare "Unable to load live option data.".
    with mock.patch.object(views, "read_live_options_data",
                           side_effect=RuntimeError("redis exploded")):
        status, payload = call()
    check("failure still answers 200", status, 200)
    check("error is surfaced", payload["error"], "RuntimeError: redis exploded")
    check("chain degrades to empty", payload["option_chain"], [])
    check("flagged stale", payload["stale"], True)
    check("feed_status marks the fault", payload["feed_status"], "api_error")
    check("error payload keeps every frontend key", sorted(payload), happy_keys)

    # A broken strike-bounds lookup must not take the chain down either.
    with mock.patch.object(views, "get_active_strike_bounds",
                           side_effect=ValueError("nse down")):
        status, payload = call()
    check("strike-bounds failure answers 200", status, 200)
    check("strike-bounds error surfaced", payload["error"], "ValueError: nse down")


def test_strike_bounds_negative_cache() -> None:
    print("\n10) NSE lookups are timed out and rate-limited on failure")
    source = io.open(ROOT_DIR / "Nse_Project" / "Live_dashboard" / "views.py",
                     encoding="utf-8").read()
    check("every NSE session.get passes a timeout",
          source.count("timeout=NSE_REQUEST_TIMEOUT"), 3)
    check("no un-timed session.get remains",
          "session.get(calls_url, headers=headers, cookies=cookies)" in source, False)
    check("timeout is configurable", views.NSE_REQUEST_TIMEOUT > 0, True)

    calls = []

    def failing():
        calls.append(1)
        return None

    views._ACTIVE_STRIKE_BOUNDS = None
    views._ACTIVE_STRIKE_BOUNDS_FETCHED_AT = None
    views._ACTIVE_STRIKE_BOUNDS_ATTEMPTED_AT = None
    with mock.patch.object(views, "most_active_index_contracts", failing):
        results = [views.get_active_strike_bounds() for _ in range(5)]
    check("5 polls -> ONE NSE attempt", len(calls), 1)
    check("bounds stay None (caller uses its safe band)", results, [None] * 5)

    # force_refresh must still bypass the negative cache.
    with mock.patch.object(views, "most_active_index_contracts", failing):
        views.get_active_strike_bounds(force_refresh=True)
    check("force_refresh bypasses the negative cache", len(calls), 2)

    # A success repopulates the cache and is served without further NSE traffic.
    import pandas as pd

    df = pd.DataFrame({"Strike Price": [24000.0, 26000.0]})
    with mock.patch.object(views, "most_active_index_contracts",
                           lambda: (df, "01-Sep-2026")) as _:
        first = views.get_active_strike_bounds(force_refresh=True)
    with mock.patch.object(views, "most_active_index_contracts", failing):
        second = views.get_active_strike_bounds()
    check("bounds cached from the successful fetch", first, (24000.0, 26000.0))
    check("cached value reused with no new attempt", (second, len(calls)), ((24000.0, 26000.0), 2))


if __name__ == "__main__":
    test_expiry_utils()
    test_dashboard_rows_keyed_by_expiry()
    test_engine_rows_keyed_by_expiry()
    test_single_instance_lock()
    test_dump_does_not_publish_stale_state()
    test_publish_only_active_expiry()
    test_poi_base_persistence()
    test_missing_expiry_is_unknown_not_foreign()
    test_master_list_filtered_before_parsing()
    test_api_never_returns_a_500()
    test_strike_bounds_negative_cache()

    print(f"\n{'=' * 60}")
    print(f"PASSED: {PASSED}   FAILED: {FAILED}")
    print(f"{'=' * 60}")
    sys.exit(1 if FAILED else 0)

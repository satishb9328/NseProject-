"""Regression tests for the DTE singularity floor and the 15:30 expiry rollover.

Standalone (no pytest). Run with:

    .venv\\Scripts\\python.exe Nse_Project\\TESTING\\dte_rollover_test.py

Covers:
  Obs1  MIN_DTE_YEARS floors time-to-expiry so greeks stay finite through the close,
        and charm is quoted per DAY rather than per year.
  Obs2  earliest_tradable_expiry() drops the expiring chain the moment 15:30 IST
        passes, and apply_expiry_rollover() swaps the live subscription safely.
"""
import asyncio
import math
import sys
from datetime import date, datetime, timedelta
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


def test_dte_floor() -> None:
    print("\nObs1a) _fractional_dte is floored at one hour")
    check("MIN_DTE_YEARS is one hour", g.MIN_DTE_YEARS, 1.0 / (365.0 * 24.0))
    check("MIN_DTE_YEARS value", round(g.MIN_DTE_YEARS, 8), round(1.1415525e-4, 8))
    check("old 1e-6 floor is gone", g.MIN_DTE_YEARS > 1e-6, True)

    proc = g.GammaProcessor("NIFTY")
    # An expiry that closed days ago must clamp to the floor, not go negative/tiny.
    long_past = (date.today() - timedelta(days=10)).isoformat()
    check("expired chain clamps to the floor", proc._fractional_dte(long_past), g.MIN_DTE_YEARS)
    # Today's expiry after the close also clamps.
    check("today's expiry post-close clamps",
          proc._fractional_dte(date.today().isoformat()) >= g.MIN_DTE_YEARS, True)
    # Malformed input clamps rather than returning 1e-6.
    check("garbage expiry clamps", proc._fractional_dte("not-a-date"), g.MIN_DTE_YEARS)
    # A genuinely distant expiry is untouched by the floor.
    future = (date.today() + timedelta(days=30)).isoformat()
    check("30d expiry well above the floor", proc._fractional_dte(future) > 0.07, True)


def test_greeks_finite_at_floor() -> None:
    print("\nObs1b) Greeks stay finite and non-degenerate at the floor")
    spot, strike, rate, vol = 24334.55, 24300.0, 0.065, 0.12
    gamma, call_charm, put_charm, vanna = g.bs_greeks(spot, strike, g.MIN_DTE_YEARS, rate, vol)

    check("gamma is finite", math.isfinite(gamma), True)
    check("gamma is non-zero (was 1e-117)", gamma > 1e-12, True)
    check("gamma is not astronomical", gamma < 1.0, True)
    check("charm is finite", math.isfinite(call_charm), True)
    check("vanna is finite", math.isfinite(vanna), True)
    check("call/put charm are opposites", call_charm, -put_charm)

    # The old 1e-6 dte underflowed phi(d1) to zero; confirm that is what we escaped.
    old_gamma, old_charm, _, _ = g.bs_greeks(spot, strike, 1e-6, rate, vol)
    check("old 1e-6 dte produced ~zero gamma", old_gamma < 1e-20, True)
    check("new floor is vastly better", gamma > old_gamma, True)


def test_charm_is_per_day() -> None:
    print("\nObs1c) Charm is scaled to per-DAY")
    spot, strike, dte, rate, vol = 24334.55, 24300.0, 0.02, 0.065, 0.20
    _, call_charm, put_charm, _ = g.bs_greeks(spot, strike, dte, rate, vol)

    # Recompute the raw per-YEAR closed form and scale it down by the calendar year.
    sqrt_dte = math.sqrt(dte)
    d1 = (math.log(spot / strike) + (rate + 0.5 * vol**2) * dte) / (vol * sqrt_dte)
    d2 = d1 - vol * sqrt_dte
    pdf = math.exp(-0.5 * d1**2) / math.sqrt(2 * math.pi)
    charm_year = pdf * (2 * rate * dte - d2 * vol * sqrt_dte) / (2 * dte * vol * sqrt_dte)

    check("call_charm == -per_year/365", call_charm, -charm_year / g.DAYS_PER_YEAR)
    check("put_charm == +per_year/365", put_charm, charm_year / g.DAYS_PER_YEAR)
    check("DAYS_PER_YEAR", g.DAYS_PER_YEAR, 365.0)
    check("exactly 365x smaller than the per-year form",
          math.isclose(abs(call_charm) * 365.0, abs(charm_year), rel_tol=1e-9), True)


def test_expired_chain_now_produces_real_exposures() -> None:
    print("\nObs1d) An expired chain no longer collapses every exposure to ~0")
    expired = (date.today() - timedelta(days=1)).isoformat()
    payload = {"expiry_date": expired, "spot_price": 24334.55, "instrument_data": {
        "NSE_FO|1": {"strike": 24300, "option_type": "CE", "oi": 1_000_000.0,
                     "coi": 200_000.0, "iv": 12.0, "ltp": 90.0, "volume": 5000},
        "NSE_FO|2": {"strike": 24300, "option_type": "PE", "oi": 900_000.0,
                     "coi": -150_000.0, "iv": 12.5, "ltp": 70.0, "volume": 4000},
    }}
    profile = g.GammaProcessor("NIFTY").process_payload(g.normalize_live_payload(payload))
    check("dte clamped to the floor", profile.dte, g.MIN_DTE_YEARS)
    check("weekly net_gex is non-zero (was 1.5e-117)", abs(profile.weekly_gamma.net_gex) > 1.0, True)
    check("day net_gex is non-zero", abs(profile.day_gamma.net_gex) > 1.0, True)
    check("day gross_gex is non-zero", profile.day_gamma.gross_gex > 1.0, True)
    check("weekly net_charm is finite", math.isfinite(profile.weekly_gamma.net_charm), True)
    check("net_charm magnitude is sane (<1e15)", abs(profile.weekly_gamma.net_charm) < 1e15, True)
    print(f"    weekly net_gex={profile.weekly_gamma.net_gex:,.0f} "
          f"net_charm={profile.weekly_gamma.net_charm:,.0f}")
    print(f"    day    net_gex={profile.day_gamma.net_gex:,.0f} "
          f"ratio={profile.day_gamma.net_gex_ratio}")


def test_earliest_tradable_expiry() -> None:
    print("\nObs2a) earliest_tradable_expiry() rolls over at 15:30 IST")
    day = date(2026, 8, 27)

    def at(h, m, s=0):
        return datetime(day.year, day.month, day.day, h, m, s, tzinfo=feed.IST)

    check("09:15 -> today", feed.earliest_tradable_expiry(at(9, 15)), day)
    check("15:29:59 -> today", feed.earliest_tradable_expiry(at(15, 29, 59)), day)
    # Spec: STRICTLY greater than 15:30 rolls over, so 15:30:00 exactly still counts.
    check("15:30:00 -> today (not strictly past)", feed.earliest_tradable_expiry(at(15, 30, 0)), day)
    check("15:30:01 -> tomorrow", feed.earliest_tradable_expiry(at(15, 30, 1)),
          day + timedelta(days=1))
    check("16:00 -> tomorrow", feed.earliest_tradable_expiry(at(16, 0)), day + timedelta(days=1))
    check("23:59 -> tomorrow (no midnight wait)", feed.earliest_tradable_expiry(at(23, 59)),
          day + timedelta(days=1))
    check("market_close_at", feed.market_close_at(at(9, 15)), at(15, 30))
    check("IST offset", feed.IST.utcoffset(None), timedelta(hours=5, minutes=30))


def test_apply_expiry_rollover() -> None:
    print("\nObs2b) apply_expiry_rollover() swaps the subscription safely")
    calls = []

    class FakeStreamer:
        Mode = {"LTPC": "ltpc", "FULL": "full_d5", "OPTION": "option_greeks", "D30": "full_d30"}

        def unsubscribe(self, keys):
            calls.append(("unsubscribe", list(keys)))

        def subscribe(self, keys, mode):
            calls.append(("subscribe", list(keys), mode))

    old_keys = ["NSE_FO|OLD1", "NSE_FO|OLD2"]
    new_keys = ["NSE_FO|NEW1", "NSE_FO|NEW2", "NSE_FO|NEW3"]
    instrument_keys = list(old_keys)
    active = {"expiry": date(2026, 8, 25)}
    new_expiry = date(2026, 8, 27)

    feed.live_state["instrument_data"] = {"NSE_FO|OLD1": {"strike": 1}}
    feed.live_state["restored_from_disk"] = True
    feed.live_state["restored_instruments"] = 277
    feed.volume_history["NSE_FO|OLD1"] = "stale"
    feed.previous_oi_map["NSE_FO|OLD1"] = 123.0

    refreshed = []

    async def scenario():
        loop = asyncio.get_running_loop()
        with mock.patch.object(feed, "refresh_previous_oi", lambda e: refreshed.append(e)), \
                mock.patch.object(feed, "publish_live_state", new=mock.AsyncMock()), \
                mock.patch.object(feed, "MarketDataStreamerV3", FakeStreamer):
            await feed.apply_expiry_rollover(
                FakeStreamer(), active, instrument_keys, new_expiry, new_keys, loop
            )

    asyncio.run(scenario())

    check("old chain unsubscribed FIRST", calls[0][0], "unsubscribe")
    check("unsubscribed exactly the old keys", calls[0][1], old_keys)
    check("new chain subscribed second", calls[1][0], "subscribe")
    check("new subscription includes the index", feed.INDEX_INSTRUMENT_KEY in calls[1][1], True)
    check("new subscription has all new keys", all(k in calls[1][1] for k in new_keys), True)
    check("no old key leaked in", any(k in calls[1][1] for k in old_keys), False)
    check("subscribed on full mode", calls[1][2], "full_d5")

    check("instrument_keys mutated IN PLACE", instrument_keys, new_keys)
    check("active expiry advanced", active["expiry"], new_expiry)
    check("live_state expiry_date advanced", feed.live_state["expiry_date"], "2026-08-27")
    check("stale chain cleared", feed.live_state["instrument_data"], {})
    check("restore flags reset", feed.live_state["restored_from_disk"], False)
    check("restored_instruments reset", feed.live_state["restored_instruments"], 0)
    check("velocity history cleared", len(feed.volume_history), 0)
    check("previous OI map cleared", len(feed.previous_oi_map), 0)
    check("POI refetched for the new expiry", refreshed, [new_expiry])


if __name__ == "__main__":
    test_dte_floor()
    test_greeks_finite_at_floor()
    test_charm_is_per_day()
    test_expired_chain_now_produces_real_exposures()
    test_earliest_tradable_expiry()
    test_apply_expiry_rollover()

    print(f"\n{'=' * 60}")
    print(f"PASSED: {PASSED}   FAILED: {FAILED}")
    print(f"{'=' * 60}")
    sys.exit(1 if FAILED else 0)

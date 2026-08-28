"""Regression tests for the four Day-Gamma fixes.

Standalone (no pytest). Run with:

    .venv\\Scripts\\python.exe Nse_Project\\TESTING\\day_gamma_fixes_test.py

Covers:
  2.1  Day PCR uses magnitudes; the day regime signal is Net-GEX-driven, not PCR-gated.
  1.1  coi=None is preserved (not 0.0) and disqualifies the strike from Day Gamma only.
  2.2  GEX no longer applies lot_size a second time (scale = shares * spot).
  3.1  The Redis publish is throttled, with a force override.
"""
import asyncio
import math
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


RED = "🔴 Negative Gamma: Market Weak / BUY PE"
GREEN = "🟢 Positive Gamma: Stable Market / Buy Dips"
AMBER = "🟡 Range-bound: Choppy / Transition Zone"


def test_day_signal_decoupled_from_pcr() -> None:
    print("\n2.1a) day_gamma_regime_signal() is Net-GEX driven")
    band = g.DAY_GEX_NEUTRAL_FRACTION
    check("neutral band default", band, 0.15)
    # Strongly negative conviction -> bearish
    check("ratio -0.50 -> red", g.day_gamma_regime_signal(-50.0, 100.0), RED)
    # Strongly positive conviction -> bullish
    check("ratio +0.50 -> green", g.day_gamma_regime_signal(50.0, 100.0), GREEN)
    # THE REGRESSION: net GEX is negative but tiny relative to gross activity. The old
    # code fired RED here purely because a negative PCR satisfied `pcr < 0.7`.
    check("small negative ratio -> amber, not red", g.day_gamma_regime_signal(-1.0, 100.0), AMBER)
    check("small positive ratio -> amber", g.day_gamma_regime_signal(1.0, 100.0), AMBER)
    check("exactly at -band -> red", g.day_gamma_regime_signal(-15.0, 100.0), RED)
    check("exactly at +band -> green", g.day_gamma_regime_signal(15.0, 100.0), GREEN)
    check("zero gross -> amber", g.day_gamma_regime_signal(-1e9, 0.0), AMBER)
    check("nan net -> amber", g.day_gamma_regime_signal(float("nan"), 100.0), AMBER)
    check("inf gross -> amber", g.day_gamma_regime_signal(-5.0, float("inf")), AMBER)
    # The day signal must not accept a pcr argument any more.
    import inspect
    params = list(inspect.signature(g.day_gamma_regime_signal).parameters)
    check("signature is (net_gex, gross_gex)", params, ["net_gex", "gross_gex"])


def test_absolute_pcr() -> None:
    print("\n2.1b) _metrics_for(absolute_pcr=True) never returns a negative PCR")
    # call COI negative (unwinding), put COI positive (building) - the live case.
    strikes = [
        g.OptionStrike(strike=24300, call_coi=-4_000_000.0, put_coi=800_000.0, has_day_coi=True),
        g.OptionStrike(strike=24350, call_coi=-358_264.0, put_coi=51_396.0, has_day_coi=True),
    ]
    day = g.GammaProcessor._metrics_for(
        strikes, "call_coi", "put_coi", "day_net_gex", "day_net_charm", "day_net_vanna",
        24334.55, absolute_pcr=True,
    )
    check("pcr is positive", day.pcr > 0, True)
    check("pcr magnitude", day.pcr, round(851_396.0 / 4_358_264.0, 3))
    # Signed flows are still exposed so the direction is not lost.
    check("call_flow stays signed/negative", day.call_flow, -4_358_264.0)
    check("put_flow stays signed/positive", day.put_flow, 851_396.0)
    check("strike_count", day.strike_count, 2)

    # Weekly path (default absolute_pcr=False) must be byte-for-byte unchanged.
    oi_strikes = [
        g.OptionStrike(strike=24300, call_oi=1000.0, put_oi=2000.0),
        g.OptionStrike(strike=24350, call_oi=3000.0, put_oi=1500.0),
    ]
    weekly = g.GammaProcessor._metrics_for(
        oi_strikes, "call_oi", "put_oi", "net_gex", "net_charm", "net_vanna", 24334.55,
    )
    check("weekly pcr unchanged", weekly.pcr, round(3500.0 / 4000.0, 3))


# A payload with three strikes: two fully-known COI, one with coi=None on the put leg.
def _instrument_data():
    return {
        "NSE_FO|1": {"strike": 24300, "option_type": "CE", "tradingsymbol": "NIFTY24300CE",
                     "oi": 1_000_000.0, "coi": 200_000.0, "iv": 12.0, "ltp": 90.0, "volume": 5000},
        "NSE_FO|2": {"strike": 24300, "option_type": "PE", "tradingsymbol": "NIFTY24300PE",
                     "oi": 900_000.0, "coi": -150_000.0, "iv": 12.5, "ltp": 70.0, "volume": 4000},
        "NSE_FO|3": {"strike": 24400, "option_type": "CE", "tradingsymbol": "NIFTY24400CE",
                     "oi": 800_000.0, "coi": 0.0, "iv": 11.0, "ltp": 40.0, "volume": 3000},
        "NSE_FO|4": {"strike": 24400, "option_type": "PE", "tradingsymbol": "NIFTY24400PE",
                     "oi": 700_000.0, "coi": 50_000.0, "iv": 11.5, "ltp": 110.0, "volume": 2000},
        # ---- unknown COI: must be excluded from Day Gamma, kept for Weekly ----
        "NSE_FO|5": {"strike": 24500, "option_type": "CE", "tradingsymbol": "NIFTY24500CE",
                     "oi": 600_000.0, "coi": 123_456.0, "iv": 10.5, "ltp": 20.0, "volume": 1000},
        "NSE_FO|6": {"strike": 24500, "option_type": "PE", "tradingsymbol": "NIFTY24500PE",
                     "oi": 500_000.0, "coi": None, "iv": 10.0, "ltp": 150.0, "volume": 900},
    }


def _payload():
    return {"expiry_date": "2026-08-27", "spot_price": 24334.55,
            "instrument_data": _instrument_data()}


def test_none_coi_not_coerced() -> None:
    print("\n1.1) coi=None is preserved and disqualifies the strike from Day Gamma")
    norm = g.normalize_live_payload(_payload())
    rows = {r["strike_price"]: r for r in norm["data"]}

    check("unknown put COI stays None", rows[24500.0]["put_coi"], None)
    check("known call COI on same strike", rows[24500.0]["call_coi"], 123_456.0)
    # A genuine zero must NOT be confused with unknown.
    check("genuine 0.0 COI preserved as 0.0", rows[24400.0]["call_coi"], 0.0)
    check("genuine 0.0 is not None", rows[24400.0]["call_coi"] is None, False)
    # OI is unaffected -> Weekly Gamma still sees all three strikes.
    check("unknown-COI strike keeps its OI", rows[24500.0]["put_oi"], 500_000.0)

    s_known = g.GammaProcessor._to_strike(rows[24300.0])
    s_zero = g.GammaProcessor._to_strike(rows[24400.0])
    s_unknown = g.GammaProcessor._to_strike(rows[24500.0])
    check("both legs known -> has_day_coi", s_known.has_day_coi, True)
    check("genuine zero -> has_day_coi", s_zero.has_day_coi, True)
    check("one leg None -> not has_day_coi", s_unknown.has_day_coi, False)

    profile = g.GammaProcessor("NIFTY").process_payload(norm)
    check("weekly still sees all 3 strikes", profile.weekly_gamma.strike_count, 3)
    check("day sees only the 2 known strikes", profile.day_gamma.strike_count, 2)
    check("excluded strike contributes no day GEX",
          next(s for s in profile.strikes if s.strike == 24500.0).day_net_gex, 0.0)
    # Day flows must exclude the unknown strike's 123,456 call COI entirely.
    check("day call_flow excludes unknown strike", profile.day_gamma.call_flow, 200_000.0)
    check("day put_flow excludes unknown strike", profile.day_gamma.put_flow, -100_000.0)


def test_no_double_lot_scaling() -> None:
    print("\n2.2) GEX scale is shares * spot (no second lot_size factor)")
    norm = g.normalize_live_payload(_payload())
    proc = g.GammaProcessor("NIFTY")
    profile = proc.process_payload(norm)
    spot = profile.spot
    dte = profile.dte

    target = next(s for s in profile.strikes if s.strike == 24300.0)
    call_gamma, _, _, _ = g.bs_greeks(spot, 24300.0, dte, proc.risk_free_rate, target.call_iv / 100)
    put_gamma, _, _, _ = g.bs_greeks(spot, 24300.0, dte, proc.risk_free_rate, target.put_iv / 100)

    expected_call = -target.call_coi * call_gamma * spot
    expected_put = target.put_coi * put_gamma * spot
    check("day_call_gex == -coi*gamma*spot", target.day_call_gex, expected_call)
    check("day_put_gex == coi*gamma*spot", target.day_put_gex, expected_put)
    check("day_net_gex is the sum", target.day_net_gex, expected_call + expected_put)

    # And it is exactly 1/lot_size of the old (double-scaled) formula.
    old_formula = expected_call * g.CONFIG["lot_size"]
    check("exactly 65x smaller than before",
          math.isclose(target.day_call_gex, old_formula / g.CONFIG["lot_size"], rel_tol=1e-9), True)

    # gross_gex / net_gex_ratio wiring
    day_strikes = [s for s in profile.strikes if s.has_day_coi]
    check("gross_gex", profile.day_gamma.gross_gex,
          sum(abs(s.day_call_gex) + abs(s.day_put_gex) for s in day_strikes))
    check("net_gex_ratio within [-1,1]", abs(profile.day_gamma.net_gex_ratio) <= 1.0, True)
    check("signal matches the ratio-based classifier", profile.day_gamma.signal,
          g.day_gamma_regime_signal(profile.day_gamma.net_gex, profile.day_gamma.gross_gex))


def test_all_coi_unknown_degrades_gracefully() -> None:
    print("\n1.1b) All-unknown COI must not crash, and must not fake a regime")
    payload = {"expiry_date": "2026-08-27", "spot_price": 24334.55, "instrument_data": {
        "A": {"strike": 24300, "option_type": "CE", "oi": 1000.0, "coi": None,
              "iv": 12.0, "ltp": 90.0, "volume": 1},
        "B": {"strike": 24300, "option_type": "PE", "oi": 900.0, "coi": None,
              "iv": 12.0, "ltp": 70.0, "volume": 1},
    }}
    profile = g.GammaProcessor("NIFTY").process_payload(g.normalize_live_payload(payload))
    d = profile.day_gamma
    check("day strike_count is 0", d.strike_count, 0)
    check("day net_gex is 0", d.net_gex, 0)
    check("day gross_gex is 0", d.gross_gex, 0)
    check("day ratio is 0", d.net_gex_ratio, 0.0)
    check("day pcr is 0 (no divide-by-zero)", d.pcr, 0.0)
    check("gamma flip falls back to spot", d.gamma_flip_level, 24334.55)
    check("no fake walls", (d.call_wall, d.put_wall), (None, None))
    check("signal is neutral, not bearish", d.signal, AMBER)
    # Weekly Gamma must be completely unaffected by the day exclusion.
    check("weekly unaffected", profile.weekly_gamma.strike_count, 1)
    check("weekly net_gex still computed", profile.weekly_gamma.net_gex != 0, True)


def test_charm_vanna_scale_aligned() -> None:
    print("\nFlag2) Charm/Vanna carry NO spot factor; Vanna is per vol-point")
    norm = g.normalize_live_payload(_payload())
    proc = g.GammaProcessor("NIFTY")
    profile = proc.process_payload(norm)
    spot = profile.spot
    lot = g.CONFIG["lot_size"]

    target = next(s for s in profile.strikes if s.strike == 24300.0)
    # Charm/Vanna are 1/time and 1/vol - they have no 1/price term, so NO spot factor.
    check("net_charm == plain OI-weighted charm", target.net_charm,
          target.call_oi * target.call_charm - target.put_oi * target.put_charm)
    check("net_vanna == plain OI-weighted vanna", target.net_vanna,
          target.call_oi * target.call_vanna - target.put_oi * target.put_vanna)
    check("day_net_charm == plain COI-weighted charm", target.day_net_charm,
          target.call_coi * target.call_charm - target.put_coi * target.put_charm)
    check("day_net_vanna == plain COI-weighted vanna", target.day_net_vanna,
          target.call_coi * target.call_vanna - target.put_coi * target.put_vanna)
    # GEX keeps its spot factor (gamma's 1/price cancels against it).
    call_gamma, _, _, _ = g.bs_greeks(spot, 24300.0, profile.dte,
                                      proc.risk_free_rate, target.call_iv / 100)
    check("GEX still scaled by spot", target.day_call_gex,
          -target.call_coi * call_gamma * spot)
    # No lot_size anywhere in the exposure math.
    check("charm is not lot-scaled",
          math.isclose(target.net_charm * lot, target.net_charm, rel_tol=1e-9), False)

    # Vanna vol-step normalization: the greek itself is per 1% vol.
    sigma = target.call_iv / 100
    sqrt_dte = math.sqrt(profile.dte)
    d1 = (math.log(spot / 24300.0) + (proc.risk_free_rate + 0.5 * sigma**2) * profile.dte) / (sigma * sqrt_dte)
    d2 = d1 - sigma * sqrt_dte
    pdf = math.exp(-0.5 * d1**2) / math.sqrt(2 * math.pi)
    check("VOL_POINTS_PER_UNIT", g.VOL_POINTS_PER_UNIT, 100.0)
    check("vanna == raw/100 (per vol point)", target.call_vanna,
          (-pdf * d2 / sigma) / 100.0)

    # An unknown-COI strike must contribute nothing to day charm/vanna either.
    unknown = next(s for s in profile.strikes if s.strike == 24500.0)
    check("unknown strike day_net_charm is 0", unknown.day_net_charm, 0.0)
    check("unknown strike day_net_vanna is 0", unknown.day_net_vanna, 0.0)
    # ...but it still contributes to the WEEKLY charm/vanna.
    check("unknown strike still has weekly charm", unknown.net_charm != 0, True)

    # Magnitudes must now be in a readable range, not hundreds of billions.
    check("weekly net_charm < 1e9", abs(profile.weekly_gamma.net_charm) < 1e9, True)
    check("weekly net_vanna < 1e9", abs(profile.weekly_gamma.net_vanna) < 1e9, True)


def test_startup_restores_snapshot(tmp_marker="__restore_test__") -> None:
    print("\nFlag1) Startup reuses the on-disk snapshot instead of wiping it")
    import datetime
    import json
    import tempfile

    expiry = datetime.date(2026, 8, 27)
    snapshot = {
        "updated_at": "2026-08-25T15:30:00+05:30",
        "expiry_date": expiry.isoformat(),
        "spot_price": 24334.55,
        "spot_updated_at": "2026-08-25T15:30:00+05:30",
        "instrument_data": {
            "NSE_FO|1": {"strike": 24300, "option_type": "CE", "oi": 1000.0, "coi": 100.0},
            "NSE_FO|2": {"strike": 24300, "option_type": "PE", "oi": 900.0, "coi": -50.0},
            "bad-entry": "not-a-dict",
        },
    }

    def run_with(file_contents, expiry_date=expiry):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "live_options_data.json"
            if file_contents is not None:
                path.write_text(file_contents, encoding="utf-8")
            feed.live_state["instrument_data"] = {}
            feed.live_state["restored_from_disk"] = False
            with mock.patch.object(feed, "JSON_PATH", str(path)):
                return feed.load_persisted_live_state(expiry_date)

    # Happy path
    count = run_with(json.dumps(snapshot))
    check("restored the 2 well-formed entries", count, 2)
    check("instrument_data populated", len(feed.live_state["instrument_data"]), 2)
    check("malformed entry dropped", "bad-entry" in feed.live_state["instrument_data"], False)
    check("restored_from_disk flag", feed.live_state["restored_from_disk"], True)
    check("restored_instruments", feed.live_state["restored_instruments"], 2)
    check("snapshot provenance kept", feed.live_state["restored_snapshot_updated_at"],
          "2026-08-25T15:30:00+05:30")
    check("spot carried forward", feed.live_state["spot_price"], 24334.55)
    check("spot labelled as restored", feed.live_state["spot_source"], "restored_from_disk")

    # Expiry guard: a snapshot from a different expiry must be rejected outright.
    check("wrong-expiry snapshot rejected", run_with(json.dumps(snapshot), datetime.date(2026, 9, 3)), 0)

    # Missing file / corrupt JSON / wrong shape / empty chain -> 0, no exception.
    check("missing file -> 0", run_with(None), 0)
    check("corrupt json -> 0", run_with("{not json"), 0)
    check("non-object json -> 0", run_with("[1,2,3]"), 0)
    check("empty instrument_data -> 0",
          run_with(json.dumps({"expiry_date": expiry.isoformat(), "instrument_data": {}})), 0)
    check("missing instrument_data -> 0",
          run_with(json.dumps({"expiry_date": expiry.isoformat()})), 0)
    # A snapshot with no expiry_date at all is accepted (nothing to contradict).
    check("no expiry_date -> still restored",
          run_with(json.dumps({"instrument_data": snapshot["instrument_data"]})), 2)


def test_redis_publish_throttle() -> None:
    print("\n3.1) Redis publish is throttled, with a force override")
    check("interval default", feed.REDIS_PUBLISH_MIN_INTERVAL, 1.0)

    clock = {"t": 5000.0}
    fake_redis = mock.Mock()

    async def scenario():
        feed._last_redis_publish_ts = 0.0
        with mock.patch.object(feed, "redis_client", fake_redis), \
                mock.patch.object(feed.time, "monotonic", side_effect=lambda: clock["t"]):
            # 50 ticks inside one interval -> exactly ONE publish.
            for _ in range(50):
                clock["t"] += 0.01
                await feed.publish_live_state()
            first_publishes = fake_redis.publish.call_count
            first_sets = fake_redis.set.call_count

            # Cross the interval boundary -> one more.
            clock["t"] += 1.2
            await feed.publish_live_state()
            second_publishes = fake_redis.publish.call_count

            # force=True must bypass the throttle immediately.
            clock["t"] += 0.01
            await feed.publish_live_state(force=True)
            forced_publishes = fake_redis.publish.call_count
            return first_publishes, first_sets, second_publishes, forced_publishes

    first_pub, first_set, second_pub, forced_pub = asyncio.run(scenario())
    check("50 rapid ticks -> 1 publish", first_pub, 1)
    check("SET throttled with PUBLISH", first_set, 1)
    check("after the interval -> 2 publishes", second_pub, 2)
    check("force bypasses the throttle -> 3", forced_pub, 3)
    check("channel used", fake_redis.publish.call_args[0][0], feed.REDIS_CHANNEL)
    check("data key used", fake_redis.set.call_args[0][0], feed.REDIS_DATA_KEY)

    # A Redis outage must not raise out of publish_live_state.
    async def failing():
        feed._last_redis_publish_ts = 0.0
        broken = mock.Mock()
        broken.set.side_effect = feed.redis.RedisError("down")
        with mock.patch.object(feed, "redis_client", broken):
            await feed.publish_live_state(force=True)
        return True

    check("RedisError is swallowed", asyncio.run(failing()), True)

    # write_live_state must forward force_publish through.
    async def forwards():
        feed._last_redis_publish_ts = 0.0
        with mock.patch.object(feed, "publish_live_state", new=mock.AsyncMock()) as pub, \
                mock.patch.object(feed, "atomic_write_json", mock.Mock()):
            await feed.write_live_state(force_publish=True)
            return pub.await_args.kwargs.get("force")

    check("write_live_state forwards force_publish", asyncio.run(forwards()), True)


def test_live_payload_if_available() -> None:
    print("\n4) Sanity-check against the real live_options_data.json (if present)")
    import json
    path = ROOT_DIR / "live_options_data.json"
    if not path.exists():
        print("  [SKIP] live_options_data.json not found")
        return
    raw = json.load(open(path, encoding="utf-8"))
    norm = g.normalize_live_payload(raw)
    if not norm:
        print("  [SKIP] payload not normalizable")
        return
    profile = g.GammaProcessor("NIFTY").process_payload(norm)
    if profile is None:
        print("  [SKIP] profile could not be built")
        return
    d = profile.day_gamma
    print(f"  day strike_count = {d.strike_count} (was polluted with ~266 fake-zero strikes)")
    print(f"  day pcr          = {d.pcr}")
    print(f"  day net_gex      = {d.net_gex:,.0f}")
    print(f"  day gross_gex    = {d.gross_gex:,.0f}")
    print(f"  day net_gex_ratio= {d.net_gex_ratio}")
    print(f"  day call_flow    = {d.call_flow:,.0f}  put_flow = {d.put_flow:,.0f}")
    print(f"  day signal       = {d.signal}")
    check("live day pcr is non-negative", d.pcr >= 0, True)
    check("live day strike_count > 0", d.strike_count > 0, True)
    check("every counted strike had real COI",
          all(s.has_day_coi for s in profile.strikes if s.day_net_gex != 0), True)


if __name__ == "__main__":
    test_day_signal_decoupled_from_pcr()
    test_absolute_pcr()
    test_none_coi_not_coerced()
    test_all_coi_unknown_degrades_gracefully()
    test_no_double_lot_scaling()
    test_charm_vanna_scale_aligned()
    test_startup_restores_snapshot()
    test_redis_publish_throttle()
    test_live_payload_if_available()

    print(f"\n{'=' * 60}")
    print(f"PASSED: {PASSED}   FAILED: {FAILED}")
    print(f"{'=' * 60}")
    sys.exit(1 if FAILED else 0)

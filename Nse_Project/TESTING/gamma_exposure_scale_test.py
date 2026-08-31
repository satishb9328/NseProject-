"""Regression tests for the Gamma exposure-scale + display-format fix.

Standalone (no pytest). Run with:

    venv\\Scripts\\python.exe Nse_Project\\TESTING\\gamma_exposure_scale_test.py

Covers:
  A) resolve_oi_divisor() - which lot factor gets divided out, and when it must not be.
  B) The feed publishes OI/COI in SHARES (contracts * LOT_SIZE); the engine now divides
     that factor out BEFORE the Gamma/Charm/Vanna formulas, so NET GEX / NET CHARM /
     NET VANNA are exactly LOT_SIZE (65x) smaller than the inflated numbers that used to
     reach the dashboard.
  C) Scale-invariant outputs (PCR, net_gex_ratio, gamma flip, regime signals, alerts)
     are untouched by the conversion - only the magnitudes change.
  D) format_exposure() renders compact Cr/L/K (and international K/M/B/T) strings, and
     every GammaMetrics / last_profile payload carries those *_display strings.
"""
import json
import math
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

# Console hardening: the regime signals carry emoji, which raise UnicodeEncodeError on a
# cp1252 console when PYTHONIOENCODING is unset (same guard as Swing_stocks uses).
for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

from Nse_Project.GAMMA import gamma_blast_engine as g  # noqa: E402
from Nse_Project.Live_dashboard import upstox_live_feed as feed  # noqa: E402

PASSED = 0
FAILED = 0

LOT = g.CONFIG["lot_size"]
SPOT = 24334.55
EXPIRY = "2026-08-27"


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


# Contract (lot) counts a human would recognise from the option chain.
CHAIN_IN_CONTRACTS = {
    24300: {"call_oi": 15_000, "put_oi": 21_000, "call_coi": 3_200, "put_coi": -1_800,
            "call_iv": 12.0, "put_iv": 12.5, "call_ltp": 90.0, "put_ltp": 70.0},
    24400: {"call_oi": 12_500, "put_oi": 9_400, "call_coi": -900, "put_coi": 2_400,
            "call_iv": 11.0, "put_iv": 11.5, "call_ltp": 40.0, "put_ltp": 110.0},
    24500: {"call_oi": 9_100, "put_oi": 6_300, "call_coi": 1_100, "put_coi": 640,
            "call_iv": 10.5, "put_iv": 10.0, "call_ltp": 20.0, "put_ltp": 150.0},
}


def _payload(multiplier: float, extra: dict | None = None) -> dict:
    """Build a live-style payload with OI/COI multiplied by ``multiplier``.

    ``multiplier=LOT`` reproduces what upstox_live_feed actually publishes
    (``oi_shares = contracts * LOT_SIZE``); ``multiplier=1`` is the same chain already
    expressed in contracts.
    """
    instrument_data = {}
    for index, (strike, row) in enumerate(sorted(CHAIN_IN_CONTRACTS.items())):
        instrument_data[f"NSE_FO|{index * 2 + 1}"] = {
            "strike": strike, "option_type": "CE", "tradingsymbol": f"NIFTY26AUG{strike}CE",
            "oi": row["call_oi"] * multiplier, "coi": row["call_coi"] * multiplier,
            "iv": row["call_iv"], "ltp": row["call_ltp"], "volume": 5000,
        }
        instrument_data[f"NSE_FO|{index * 2 + 2}"] = {
            "strike": strike, "option_type": "PE", "tradingsymbol": f"NIFTY26AUG{strike}PE",
            "oi": row["put_oi"] * multiplier, "coi": row["put_coi"] * multiplier,
            "iv": row["put_iv"], "ltp": row["put_ltp"], "volume": 4000,
        }
    payload = {"expiry_date": EXPIRY, "spot_price": SPOT, "instrument_data": instrument_data}
    payload.update(extra or {})
    return payload


def _profile(multiplier: float, extra: dict | None = None):
    norm = g.normalize_live_payload(_payload(multiplier, extra))
    return g.GammaProcessor("NIFTY").process_payload(norm)


def test_feed_really_publishes_shares() -> None:
    print("\nA0) The producer's contract: live_state OI/COI is contracts * LOT_SIZE")
    check("engine lot size matches the feed's", LOT, feed.LOT_SIZE)
    check("_scale_oi_to_internal multiplies by the lot size",
          feed._scale_oi_to_internal(500.0), 500.0 * feed.LOT_SIZE)


def test_resolve_oi_divisor() -> None:
    print("\nA) resolve_oi_divisor() picks the lot factor to divide out")
    check("no payload -> configured lot size", g.resolve_oi_divisor(), float(LOT))
    check("empty payload -> configured lot size", g.resolve_oi_divisor({}), float(LOT))
    check("payload lot_size wins", g.resolve_oi_divisor({"lot_size": 50}), 50.0)
    check("camelCase lotSize honoured", g.resolve_oi_divisor({"lotSize": 25}), 25.0)
    check("garbage lot_size -> fallback", g.resolve_oi_divisor({"lot_size": "abc"}), float(LOT))
    check("zero lot_size -> fallback", g.resolve_oi_divisor({"lot_size": 0}), float(LOT))
    # A producer that already publishes lots must NOT be divided a second time.
    check("payload scale=contracts -> 1.0", g.resolve_oi_divisor({"scale": "contracts"}), 1.0)
    check("payload scale=lots -> 1.0", g.resolve_oi_divisor({"scale": "LOTS"}), 1.0)
    check("the feed's own scale tag still means shares",
          g.resolve_oi_divisor({"scale": "internal_shares"}), float(LOT))

    original = g.CONFIG["oi_input_scale"]
    try:
        g.CONFIG["oi_input_scale"] = "contracts"
        check("GAMMA_OI_INPUT_SCALE=contracts disables the division",
              g.resolve_oi_divisor({"lot_size": 65}), 1.0)
    finally:
        g.CONFIG["oi_input_scale"] = original
    check("config restored", g.CONFIG["oi_input_scale"], original)


def test_oi_coi_converted_to_contracts() -> None:
    print("\nB1) _to_strike() converts the published shares back into contracts")
    rows = {r["strike_price"]: r for r in g.normalize_live_payload(_payload(LOT))["data"]}
    check("the normalized row still carries raw shares",
          rows[24300.0]["call_oi"], 15_000.0 * LOT)

    strike = g.GammaProcessor._to_strike(rows[24300.0], float(LOT))
    check("call_oi in contracts", strike.call_oi, 15_000.0)
    check("put_oi in contracts", strike.put_oi, 21_000.0)
    check("call_coi in contracts", strike.call_coi, 3_200.0)
    check("put_coi in contracts (sign kept)", strike.put_coi, -1_800.0)
    check("prices are NOT rescaled", strike.call_ltp, 90.0)
    check("volume is NOT rescaled", strike.call_vol, 5000.0)
    check("iv is NOT rescaled", strike.call_iv, 12.0)

    # Default divisor == configured lot size, so a direct caller sees the same scale.
    check("default divisor matches the explicit one",
          g.GammaProcessor._to_strike(rows[24300.0]).call_oi, 15_000.0)
    # An unknown COI must survive the conversion as "unknown", not as a real 0.0.
    unknown = dict(rows[24300.0])
    unknown["put_coi"] = None
    check("coi=None still disqualifies the strike",
          g.GammaProcessor._to_strike(unknown, float(LOT)).has_day_coi, False)
    check("an explicit divisor of 1 leaves the shares alone",
          g.GammaProcessor._to_strike(rows[24300.0], 1.0).call_oi, 15_000.0 * LOT)


def test_exposures_are_lot_size_smaller() -> None:
    print("\nB2) NET GEX / CHARM / VANNA are exactly LOT_SIZE smaller than before")
    profile = _profile(LOT)
    weekly, day = profile.weekly_gamma, profile.day_gamma
    target = next(s for s in profile.strikes if s.strike == 24300.0)

    call_gamma, _, _, _ = g.bs_greeks(profile.spot, 24300.0, profile.dte,
                                      g.CONFIG["risk_free_rate"], target.call_iv / 100)
    # The formula is unchanged - it is simply fed contracts instead of shares.
    check("call_gex == -contracts * gamma * spot", target.call_gex,
          -15_000.0 * call_gamma * profile.spot)
    # ...which is exactly 1/LOT_SIZE of what the shares-fed formula produced.
    shares_formula = -(15_000.0 * LOT) * call_gamma * profile.spot
    check("exactly LOT_SIZE smaller than the shares formula",
          math.isclose(target.call_gex, shares_formula / LOT, rel_tol=1e-9), True)

    # The whole profile now matches a payload that was already published in contracts.
    contracts = _profile(1, {"scale": "contracts"})
    check("weekly net_gex matches the contracts payload", math.isclose(
        weekly.net_gex, contracts.weekly_gamma.net_gex, rel_tol=1e-9), True)
    check("weekly net_charm matches", math.isclose(
        weekly.net_charm, contracts.weekly_gamma.net_charm, rel_tol=1e-9), True)
    check("weekly net_vanna matches", math.isclose(
        weekly.net_vanna, contracts.weekly_gamma.net_vanna, rel_tol=1e-9), True)
    check("day net_gex matches", math.isclose(
        day.net_gex, contracts.day_gamma.net_gex, rel_tol=1e-9), True)
    check("day gross_gex matches", math.isclose(
        day.gross_gex, contracts.day_gamma.gross_gex, rel_tol=1e-9), True)

    # COI flows and OI totals are in lots too, so the dashboard cannot mix units.
    check("day call_flow in contracts", day.call_flow, 3_200.0 - 900.0 + 1_100.0)
    check("day put_flow in contracts", day.put_flow, -1_800.0 + 2_400.0 + 640.0)
    check("weekly call_flow in contracts", weekly.call_flow, 15_000.0 + 12_500.0 + 9_100.0)
    check("profile records the divisor", profile.contract_divisor, float(LOT))
    check("profile records the unit", profile.oi_unit, "contracts")

    # The headline regression: the old shares-fed pipeline was LOT_SIZE times bigger.
    inflated = _profile(LOT, {"scale": "contracts"})  # shares consumed as if they were lots
    check("the old (inflated) net_gex was LOT_SIZE bigger", math.isclose(
        inflated.weekly_gamma.net_gex, weekly.net_gex * LOT, rel_tol=1e-9), True)
    check("the old (inflated) net_charm was LOT_SIZE bigger", math.isclose(
        inflated.weekly_gamma.net_charm, weekly.net_charm * LOT, rel_tol=1e-9), True)
    check("the old (inflated) net_vanna was LOT_SIZE bigger", math.isclose(
        inflated.weekly_gamma.net_vanna, weekly.net_vanna * LOT, rel_tol=1e-9), True)
    print(f"    inflated net_gex  = {inflated.weekly_gamma.net_gex:>18,.0f}")
    print(f"    corrected net_gex = {weekly.net_gex:>18,.0f}  ({weekly.net_gex_display})")


def test_signals_are_scale_invariant() -> None:
    print("\nC) Ratios, flips, PCR and the regime signals do not move with the scale")
    shares = _profile(LOT)
    inflated = _profile(LOT, {"scale": "contracts"})

    check("weekly pcr unchanged", shares.weekly_gamma.pcr, inflated.weekly_gamma.pcr)
    check("day pcr unchanged", shares.day_gamma.pcr, inflated.day_gamma.pcr)
    check("day net_gex_ratio unchanged",
          shares.day_gamma.net_gex_ratio, inflated.day_gamma.net_gex_ratio)
    check("weekly gamma flip unchanged",
          shares.weekly_gamma.gamma_flip_level, inflated.weekly_gamma.gamma_flip_level)
    check("day gamma flip unchanged",
          shares.day_gamma.gamma_flip_level, inflated.day_gamma.gamma_flip_level)
    check("weekly signal unchanged", shares.weekly_gamma.signal, inflated.weekly_gamma.signal)
    check("day signal unchanged", shares.day_gamma.signal, inflated.day_gamma.signal)
    check("call wall unchanged", shares.call_wall, inflated.call_wall)
    check("put wall unchanged", shares.put_wall, inflated.put_wall)
    check("skew unchanged", shares.skew, inflated.skew)
    check("gamma flip is None when no valid crossing exists",
          (shares.gamma_flip_level, inflated.gamma_flip_level), (None, None))

    # Zero-crossing alerts are sign-driven, so they still fire after the rescale.
    engine = g.SignalEngine()
    engine.evaluate_gamma_alerts(shares)          # the first payload only primes the state
    engine._previous_weekly_gex = abs(shares.weekly_gamma.net_gex) + 1.0
    engine._previous_day_gex = abs(shares.day_gamma.net_gex) + 1.0
    alerts = engine.evaluate_gamma_alerts(shares)
    expected_flips = ((shares.weekly_gamma.net_gex < 0) + (shares.day_gamma.net_gex < 0))
    check("small contract-scale exposures still trip the flip alerts",
          sum("flipped Negative" in message for message in alerts), expected_flips)


def test_format_exposure() -> None:
    print("\nD1) format_exposure() renders compact, readable magnitudes")
    check("crore", g.format_exposure(25_000_000), "2.50 Cr")
    check("the 14-digit number from the bug report",
          g.format_exposure(1072130050043), "107,213.01 Cr")
    check("negative lakh", g.format_exposure(-1_500_000), "-15.00 L")
    check("thousand", g.format_exposure(15_000), "15.00 K")
    check("plain integer below 1K", g.format_exposure(999), "999")
    check("sub-unit keeps decimals", g.format_exposure(0.25), "0.25")
    check("exact zero", g.format_exposure(0), "0")
    check("string input is coerced", g.format_exposure("25000000"), "2.50 Cr")
    check("nan -> --", g.format_exposure(float("nan")), "--")
    check("inf -> --", g.format_exposure(float("inf")), "--")
    check("None -> --", g.format_exposure(None), "--")
    check("international billions",
          g.format_exposure(2_500_000_000, units="international"), "2.50 B")
    check("international trillions", g.format_exposure(1e12, units="international"), "1.00 T")
    check("international millions",
          g.format_exposure(-7_250_000, units="international"), "-7.25 M")
    check("precision override", g.format_exposure(25_000_000, precision=0), "2 Cr")

    original = g.CONFIG["exposure_units"]
    try:
        g.CONFIG["exposure_units"] = "international"
        check("CONFIG switches the default unit table",
              g.format_exposure(2_500_000_000), "2.50 B")
    finally:
        g.CONFIG["exposure_units"] = original
    check("units config restored", g.CONFIG["exposure_units"], original)


def test_metrics_carry_display_strings() -> None:
    print("\nD2) Every metric block ships pre-formatted display strings")
    profile = _profile(LOT)
    for name, metrics in (("weekly", profile.weekly_gamma), ("day", profile.day_gamma)):
        check(f"{name} net_gex_display", metrics.net_gex_display,
              g.format_exposure(metrics.net_gex))
        check(f"{name} net_charm_display", metrics.net_charm_display,
              g.format_exposure(metrics.net_charm))
        check(f"{name} net_vanna_display", metrics.net_vanna_display,
              g.format_exposure(metrics.net_vanna))
        check(f"{name} gross_gex_display", metrics.gross_gex_display,
              g.format_exposure(metrics.gross_gex))
        check(f"{name} call_flow_display", metrics.call_flow_display,
              g.format_exposure(metrics.call_flow))
        check(f"{name} put_flow_display", metrics.put_flow_display,
              g.format_exposure(metrics.put_flow))
        check(f"{name} exposure_unit", metrics.exposure_unit, "contracts")
        check(f"{name} display is short enough for the UI",
              len(metrics.net_gex_display) <= 16, True)
    check("profile net_gex_display", profile.net_gex_display,
          g.format_exposure(profile.net_gex))
    # gross_gex is filled in after _metrics_for(), so its display must be refreshed.
    check("day gross_gex_display was refreshed after gross_gex was set",
          profile.day_gamma.gross_gex > 0 and profile.day_gamma.gross_gex_display != "0", True)


def test_last_profile_publishes_scale_and_display() -> None:
    print("\nD3) /gamma/status/ -> last_profile carries the scale + display strings")
    bot = g.RedisGammaBot()
    bot._process_message(json.dumps(_payload(LOT)))
    check("payload processed without error", bot.last_error, None)
    published = bot.last_profile or {}
    check("processed_updates incremented", bot.processed_updates, 1)
    check("lot factor published", published.get("lot_size"), float(LOT))
    check("oi unit published", published.get("oi_unit"), "contracts")
    check("exposure units published", published.get("exposure_units"),
          g.CONFIG["exposure_units"])
    check("net_gex_display published", published.get("net_gex_display"),
          g.format_exposure(published.get("net_gex")))
    check("weekly charm display forwarded",
          published["weekly_gamma"]["net_charm_display"],
          g.format_exposure(published["weekly_gamma"]["net_charm"]))
    check("day vanna display forwarded",
          published["day_gamma"]["net_vanna_display"],
          g.format_exposure(published["day_gamma"]["net_vanna"]))
    check("raw floats are still published for maths",
          isinstance(published["weekly_gamma"]["net_gex"], float), True)
    check("the payload is JSON-serialisable for the API",
          isinstance(json.dumps(published), str), True)


def test_live_payload_if_available() -> None:
    print("\nE) Sanity-check against the real live_options_data.json (if present)")
    path = ROOT_DIR / "live_options_data.json"
    if not path.exists():
        print("  [SKIP] live_options_data.json not found")
        return
    raw = json.loads(path.read_text(encoding="utf-8"))
    norm = g.normalize_live_payload(raw)
    profile = g.GammaProcessor("NIFTY").process_payload(norm) if norm else None
    if profile is None:
        print("  [SKIP] payload could not be turned into a profile")
        return
    weekly, day = profile.weekly_gamma, profile.day_gamma
    print(f"    weekly net_gex   = {weekly.net_gex:>20,.0f}  -> {weekly.net_gex_display}")
    print(f"    weekly net_charm = {weekly.net_charm:>20,.0f}  -> {weekly.net_charm_display}")
    print(f"    weekly net_vanna = {weekly.net_vanna:>20,.0f}  -> {weekly.net_vanna_display}")
    print(f"    day    net_gex   = {day.net_gex:>20,.0f}  -> {day.net_gex_display}")
    print(f"    day    net_charm = {day.net_charm:>20,.0f}  -> {day.net_charm_display}")
    print(f"    day    net_vanna = {day.net_vanna:>20,.0f}  -> {day.net_vanna_display}")
    print(f"    weekly signal    = {weekly.signal}")
    print(f"    day    signal    = {day.signal}")
    check("net_gex is finite", math.isfinite(weekly.net_gex), True)
    # A per-contract NIFTY chain lands in the crore range, never in the 14-digit range
    # that prompted this fix.
    check("weekly net_gex is no longer 13+ digits", abs(weekly.net_gex) < 1e13, True)
    check("weekly net_charm stays readable", abs(weekly.net_charm) < 1e13, True)
    check("weekly net_vanna stays readable", abs(weekly.net_vanna) < 1e13, True)
    check("display strings are at most 16 chars",
          max(len(weekly.net_gex_display), len(weekly.net_charm_display),
              len(weekly.net_vanna_display)) <= 16, True)


if __name__ == "__main__":
    test_feed_really_publishes_shares()
    test_resolve_oi_divisor()
    test_oi_coi_converted_to_contracts()
    test_exposures_are_lot_size_smaller()
    test_signals_are_scale_invariant()
    test_format_exposure()
    test_metrics_carry_display_strings()
    test_last_profile_publishes_scale_and_display()
    test_live_payload_if_available()

    print(f"\n{'=' * 60}")
    print(f"PASSED: {PASSED}   FAILED: {FAILED}")
    print(f"{'=' * 60}")
    sys.exit(1 if FAILED else 0)

"""Frontend/API integration checks for the CALL VEL / PUT VEL dashboard columns.

Standalone (no pytest needed). Run with:

    .venv\\Scripts\\python.exe Nse_Project\\TESTING\\dashboard_velocity_test.py

Verifies:
  1. build_option_chain_rows() maps the feed's velocity onto call_velocity/put_velocity.
  2. The HTML <th> count, the JS cells[] array and every colspan agree (15 columns).
  3. The JS column order matches the <th> order exactly, so no cell is off-by-one.
  4. The glow threshold is 50000 and the STRIKE index used by the ATM highlighter is 7.
"""
import io
import os
import re
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "Nse_Project.settings")

import django  # noqa: E402

django.setup()

from Nse_Project.Live_dashboard import views  # noqa: E402

DASHBOARD = ROOT_DIR / "Nse_Project" / "Master_Temp" / "dashboard.html"
LIVE_SIGNALS = ROOT_DIR / "Nse_Project" / "Master_Temp" / "live_signals.html"

EXPECTED_HEADERS = [
    "CALL OI%", "CALL OI", "CALL VOL", "CALL VEL", "CALL LTP", "CALL COI", "CALL COI%",
    "STRIKE",
    "PUT COI%", "PUT COI", "PUT LTP", "PUT VEL", "PUT VOL", "PUT OI", "PUT OI%",
]
# The JS cells[] array, expressed as the row key each column reads.
EXPECTED_JS_KEYS = [
    "call_oi_pct", "call_oi", "call_volume", "call_velocity", "call_ltp", "call_coi",
    "call_coi_pct", "strike", "put_coi_pct", "put_coi", "put_ltp", "put_velocity",
    "put_volume", "put_oi", "put_oi_pct",
]

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


def test_build_option_chain_rows() -> None:
    print("\n1) build_option_chain_rows() exposes call_velocity / put_velocity")
    lot = views.LOT_SIZE
    # Shaped exactly like live_options_data.json -> instrument_data after the refactor.
    instrument_data = {
        "NSE_FO|11111": {
            "strike": 26000, "option_type": "CE", "tradingsymbol": "NIFTY26AUG26000CE",
            "ltp": 120.5, "oi": 750.0 * lot, "poi": 500.0 * lot,
            "coi": 250.0 * lot, "coi_pct": 50.0,
            "volume": 1_240_000, "volume_velocity": 80_000.0, "call_velocity": 80_000.0,
            "raw_sample": {},
        },
        "NSE_FO|22222": {
            "strike": 26000, "option_type": "PE", "tradingsymbol": "NIFTY26AUG26000PE",
            "ltp": 80.25, "oi": 640.0 * lot, "poi": 800.0 * lot,
            "coi": -160.0 * lot, "coi_pct": -20.0,
            "volume": 2_030_000, "volume_velocity": 10_000.0, "put_velocity": 10_000.0,
            "raw_sample": {},
        },
    }
    rows = views.build_option_chain_rows(
        instrument_data, target_symbol="NIFTY", strike_bounds=(25000.0, 27000.0)
    )
    check("one merged strike row", len(rows), 1)
    row = rows[0]
    check("strike", row["strike"], "26000")
    check("call_velocity passed through", row["call_velocity"], 80_000.0)
    check("put_velocity passed through", row["put_velocity"], 10_000.0)
    check("call_coi converted to lots", row["call_coi"], 250)
    check("put_coi converted to lots", row["put_coi"], -160)
    # COI% must remain the feed's "Change in OI %" (COI / previous OI * 100) and must
    # NOT be overwritten by any call-vs-put COI ratio inside build_option_chain_rows().
    check("call_coi_pct preserved as change-in-OI %", row["call_coi_pct"], 50.0)
    check("put_coi_pct preserved as change-in-OI %", row["put_coi_pct"], -20.0)
    # Velocity is shares/sec and must NOT be divided by the lot size.
    check("velocity not scaled to lots", row["call_velocity"] == 80_000.0 / lot, False)
    # New relative COI dominance per strike (absolute-value math): 250 vs 160 lots.
    check("call_coi_rel_pct is call's share of |COI|", row["call_coi_rel_pct"], 61.0)
    check("put_coi_rel_pct is put's share of |COI|", row["put_coi_rel_pct"], 39.0)
    check("every expected key present",
          sorted(row.keys()),
          sorted(EXPECTED_JS_KEYS + ["call_coi_rel_pct", "put_coi_rel_pct"]))


def _headers(html: str) -> list:
    seg = html[html.index("option-chain-table"):]
    head = seg[seg.index("<thead"):seg.index("</thead>")]
    return re.findall(r">([A-Z][A-Z% ]*)</th>", head)


def test_dashboard_html() -> None:
    print("\n2) dashboard.html column wiring")
    html = io.open(DASHBOARD, encoding="utf-8").read()
    check("header count", len(_headers(html)), 15)
    check("header order", _headers(html), EXPECTED_HEADERS)

    start = html.index("const cells = [")
    cells_block = html[start:html.index("];", start)]
    keys = re.findall(r"row\.([a-z_]+)", cells_block)
    check("JS cells[] count", len(keys), 15)
    check("JS cells[] order matches headers", keys, EXPECTED_JS_KEYS)

    check("colspan=15 in tbody placeholder", 'colspan="15"' in html, True)
    check("no stale colspan=13", 'colspan="13"' in html, False)
    check("renderEmptyState colSpan = 15", "td.colSpan = 15;" in html, True)
    check("ATM highlighter reads children[7]", "tr.children[7]" in html, True)
    check("glow threshold is 50000", "const VELOCITY_GLOW_THRESHOLD = 50000;" in html, True)
    check("call glow class defined", ".option-chain-table td.vel-hot-call" in html, True)
    check("put glow class defined", ".option-chain-table td.vel-hot-put" in html, True)
    check("CALL_VEL_INDEX is 3", "const CALL_VEL_INDEX = 3;" in html, True)
    check("PUT_VEL_INDEX is 11", "const PUT_VEL_INDEX = 11;" in html, True)


def test_live_signals_html() -> None:
    print("\n3) live_signals.html compact option chain wiring")
    html = io.open(LIVE_SIGNALS, encoding="utf-8").read()

    # The legacy 15-column full option-chain table was removed from the Live
    # Signals dashboard; only the central Compact Options Chain remains.
    check("legacy full table removed", 'id="option-chain-table"' in html, False)
    check("legacy full tbody removed", 'id="live-options-tbody"' in html, False)
    check("legacy 15-col colspan removed", 'colspan="15"' in html, False)
    check("legacy renderRows removed", "function renderRows" in html, False)
    check("legacy buildRow removed", "function buildRow" in html, False)

    check("compact table present", 'id="compact-options-table"' in html, True)
    check("compact table class present", 'class="compact-chain-table"' in html, True)

    # The compact chain keeps all 7 columns; only the COI REL% SVG sparkline was removed.
    for header in (">CALL COI%</th>", ">CALL LTP</th>", ">CALL COI REL%</th>",
                   ">STRIKE</th>", ">PUT COI REL%</th>", ">PUT LTP</th>",
                   ">PUT COI%</th>"):
        check(f"compact header {header!r}", header in html, True)

    # The SVG sparkline graph is gone, but the COI REL% text renderer remains.
    check("sparkline CSS removed", ".coi-sparkline" in html, False)
    check("sparkline path math removed", "coiSparkLinePath" in html, False)
    check("SVG namespace removed", "COI_SVG_NS" in html, False)
    check("text renderer present", "function renderCoiRelCell" in html, True)
    check("value formatter present", "fmtCoiRel" in html, True)
    check("main value text kept", ".coi-main" in html, True)
    check("peak tag text kept", ".coi-pk-tag" in html, True)
    check("drawdown text kept", ".coi-drawdown" in html, True)

    # renderCompactChain reads the 7 compact fields (5 original + 2 relative COI).
    check("compact renderer present", "function renderCompactChain" in html, True)
    for key in ("call_coi_pct", "call_coi_rel_pct", "call_ltp", "strike",
                "put_ltp", "put_coi_rel_pct", "put_coi_pct"):
        check(f"compact renderer reads row.{key}", f"row.{key}" in html, True)


def test_api_contract() -> None:
    print("\n4) live_option_chain_api() advertises the new POI provenance")
    src = io.open(ROOT_DIR / "Nse_Project" / "Live_dashboard" / "views.py", encoding="utf-8").read()
    check('"poi_source" exposed', '"poi_source"' in src, True)
    check('"poi_updated_at" exposed', '"poi_updated_at"' in src, True)
    check("legacy baseline_date removed", '"baseline_date"' in src, False)
    check("legacy baseline_source removed", '"baseline_source"' in src, False)


if __name__ == "__main__":
    test_build_option_chain_rows()
    test_dashboard_html()
    test_live_signals_html()
    test_api_contract()

    print(f"\n{'=' * 60}")
    print(f"PASSED: {PASSED}   FAILED: {FAILED}")
    print(f"{'=' * 60}")
    sys.exit(1 if FAILED else 0)

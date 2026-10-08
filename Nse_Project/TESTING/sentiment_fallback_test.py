"""Regression tests for the neutral-fallback sentiment fixes.

Standalone (no pytest). Run with:

    .venv\\Scripts\\python.exe Nse_Project\\TESTING\\sentiment_fallback_test.py

Covers:
  1. upstox_live_feed._compute_live_sentiment() returns the neutral baseline (50)
     for an empty OR half-initialised option chain, while real directional data
     still maps to bearish/bullish scores.
  2. views._build_preopen_context() strips option/trap data and returns only the
     auction MAX/MIN/Final IEP plus the projected open versus yesterday's close.
"""
import math
import os
import sys
from pathlib import Path
from unittest import mock

ROOT_DIR = Path(__file__).resolve().parent.parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "Nse_Project.settings")

import django  # noqa: E402

django.setup()

from Nse_Project.Live_dashboard import upstox_live_feed as feed  # noqa: E402
from Nse_Project.Live_dashboard import views  # noqa: E402

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


def test_compute_live_sentiment() -> None:
    """Lock the worker's neutral fallback for empty / one-sided / initializing chains."""
    print("\n1) _compute_live_sentiment() neutral fallback")

    # Empty chain (explicit {}).
    bias, momentum, score = feed._compute_live_sentiment({})
    check("empty -> bias", bias, 0.0)
    check("empty -> momentum", momentum, 0.0)
    check("empty -> score", score, 50.0)

    # None falls back to the (empty) live_state instrument_data.
    bias, momentum, score = feed._compute_live_sentiment(None)
    check("None -> score", score, 50.0)

    # Half-initialised: only CE OI present (no PE yet).
    bias, momentum, score = feed._compute_live_sentiment({
        "ce1": {"option_type": "CE", "oi": 1000, "coi": 0},
    })
    check("CE-only -> score", score, 50.0)

    # Half-initialised: only PE OI present (no CE yet).
    bias, momentum, score = feed._compute_live_sentiment({
        "pe1": {"option_type": "PE", "oi": 1000, "coi": 0},
    })
    check("PE-only -> score", score, 50.0)

    # Non-dict entries and missing oi/coi are ignored, not treated as a signal.
    bias, momentum, score = feed._compute_live_sentiment({
        "junk": 123,
        "ce1": {"option_type": "CE"},  # no oi/coi -> treated as 0
    })
    check("missing oi/coi -> score", score, 50.0)

    # Balanced chain -> still neutral.
    bias, momentum, score = feed._compute_live_sentiment({
        "ce1": {"option_type": "CE", "oi": 1000, "coi": 0},
        "pe1": {"option_type": "PE", "oi": 1000, "coi": 0},
    })
    check("balanced -> score", score, 50.0)

    print("\n2) _compute_live_sentiment() directional data is preserved")

    # Bearish: call OI >> put OI (heavy call writing) must produce a sub-40 score.
    bias, momentum, score = feed._compute_live_sentiment({
        "ce1": {"option_type": "CE", "oi": 2000, "coi": 0},
        "pe1": {"option_type": "PE", "oi": 1000, "coi": 0},
    })
    check("bearish bias", bias, -100 / 3)
    check("bearish momentum", momentum, 0.0)
    check("bearish score", score, 100 / 3)

    # Bullish: put OI >> call OI must produce a super-60 score (not neutral).
    bias, momentum, score = feed._compute_live_sentiment({
        "ce1": {"option_type": "CE", "oi": 1000, "coi": 0},
        "pe1": {"option_type": "PE", "oi": 2000, "coi": 0},
    })
    check("bullish bias", bias, 100 / 3)
    check("bullish momentum", momentum, 0.0)
    check("bullish score", score, 200 / 3)


def _build_context(trap_data, previous_close=23000.0) -> dict:
    """Call _build_preopen_context() with the TBT fetch + previous close mocked."""
    with mock.patch.object(views, "fetch_real_preopen_trap_data", return_value=trap_data), \
            mock.patch.object(views, "_read_previous_close", return_value=previous_close):
        return views._build_preopen_context()


def test_build_preopen_context() -> None:
    """Lock the stripped pre-open card contract (MAX/MIN/Final IEP + projected open)."""
    print("\n3) _build_preopen_context() WAITING state")
    ctx = _build_context(None)
    check("waiting preopen_max", ctx["preopen_max"], None)
    check("waiting projected_open_pct", ctx["projected_open_pct"], None)
    check("waiting projected_open_display", ctx["projected_open_display"], "—")
    check("waiting projected_open_tone", ctx["projected_open_tone"], "flat")
    check("no call_wall key", "call_wall" in ctx, False)
    check("no trap_signal key", "trap_signal" in ctx, False)

    print("\n4) _build_preopen_context() projected open vs previous close")
    flat = {
        "Open_IEP": 23000.0,
        "Max_IEP": 23050.0,
        "Min_IEP": 22950.0,
        "Final_IEP": 23000.0,
    }
    ctx = _build_context(flat, previous_close=23000.0)
    check("flat projected_open_pct", ctx["projected_open_pct"], 0.0)
    check("flat projected_open_display", ctx["projected_open_display"], "0.00%")
    check("flat projected_open_tone", ctx["projected_open_tone"], "flat")

    up = {
        "Open_IEP": 23000.0,
        "Max_IEP": 23200.0,
        "Min_IEP": 23000.0,
        "Final_IEP": 23200.0,
    }
    ctx = _build_context(up, previous_close=23000.0)
    up_pct = round((23200.0 - 23000.0) / 23000.0 * 100.0, 2)
    check("up projected_open_pct", ctx["projected_open_pct"], up_pct)
    check("up projected_open_tone", ctx["projected_open_tone"], "up")
    check("up projected_open_display", ctx["projected_open_display"], f"+{up_pct:.2f}%")

    down = {
        "Open_IEP": 23000.0,
        "Max_IEP": 23000.0,
        "Min_IEP": 22800.0,
        "Final_IEP": 22800.0,
    }
    ctx = _build_context(down, previous_close=23000.0)
    down_pct = round((22800.0 - 23000.0) / 23000.0 * 100.0, 2)
    check("down projected_open_pct", ctx["projected_open_pct"], down_pct)
    check("down projected_open_tone", ctx["projected_open_tone"], "down")
    check("down projected_open_display", ctx["projected_open_display"], f"{down_pct:.2f}%")


if __name__ == "__main__":
    test_compute_live_sentiment()
    test_build_preopen_context()

    print(f"\n{'=' * 60}")
    print(f"PASSED: {PASSED}   FAILED: {FAILED}")
    print(f"{'=' * 60}")
    sys.exit(1 if FAILED else 0)
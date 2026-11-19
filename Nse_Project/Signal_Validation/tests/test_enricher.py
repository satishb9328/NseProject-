"""Deterministic unit tests for logger.state.RingBuffer and logger.enricher.Enricher."""

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from logger.state import RingBuffer
from logger.enricher import Enricher


def _row(spot=24800.0, atm_strike=24800.0, expiry="2026-11-26", updated_at="t"):
    return {
        "row_type": "capture", "ts": "2026-11-18T09:30:00+05:30", "updated_at": updated_at,
        "captured_at": "2026-11-18T09:30:00+05:30", "seq": 1, "session_date": "2026-11-18",
        "spot": spot, "active_expiry": expiry, "atm_strike": atm_strike,
        "bias": 1.0, "momentum": 1.0, "gamma_boost": 0.5, "score": 55.0, "iv_skew": 1.0,
        "atm_ce_ltp": 200.0, "atm_pe_ltp": 200.0,
    }


class TestEnricher(unittest.TestCase):
    def test_complete_enrichment(self):
        buf = RingBuffer(horizons=(15, 30, 60, 120), max_seconds=120.0, max_gap_seconds=10.0)
        row0 = _row()
        buf.add(row0, mono=1000.0, chain={24800.0: {"ce": 200.0, "pe": 200.0}})
        for i in range(1, 131):
            r = _row(spot=24800.0 + i * 0.1, updated_at=f"t{i}")
            buf.add(r, mono=1000.0 + i, chain={24800.0: {"ce": 200.0 + i * 0.01, "pe": 200.0 - i * 0.01}})
        enr = Enricher(horizons=(15, 30, 60, 120))
        enriched = enr.enrich(row0, capture_mono=1000.0, buffer=buf)
        self.assertTrue(enriched["complete"])
        self.assertIsNone(enriched["reason"])
        self.assertAlmostEqual(enriched["spot_15s"], 24800.0 + 15 * 0.1)
        self.assertAlmostEqual(enriched["spot_120s"], 24800.0 + 120 * 0.1)
        self.assertAlmostEqual(enriched["ce_30s"], 200.0 + 30 * 0.01)

    def test_gap_invalidation(self):
        buf = RingBuffer(max_seconds=120.0, max_gap_seconds=10.0)
        row0 = _row()
        buf.add(row0, mono=1000.0, chain={24800.0: {"ce": 200.0, "pe": 200.0}})
        for i in range(30, 131):  # gap of 30s > 10s
            r = _row(spot=24800.0 + i * 0.1, updated_at=f"g{i}")
            buf.add(r, mono=1000.0 + i, chain={24800.0: {"ce": 200.0, "pe": 200.0}})
        enr = Enricher()
        enriched = enr.enrich(row0, capture_mono=1000.0, buffer=buf)
        self.assertFalse(enriched["complete"])
        self.assertEqual(enriched["reason"], "feed_gap")
        self.assertIsNone(enriched["spot_15s"])
        self.assertIsNotNone(enriched["spot_120s"])

    def test_strike_unavailable_on_atm_shift(self):
        buf = RingBuffer(max_seconds=120.0)
        row0 = _row(atm_strike=24800.0)
        buf.add(row0, mono=1000.0, chain={24800.0: {"ce": 200.0, "pe": 200.0}})
        # future sample: ATM shifted to 24900, 24800 no longer in chain
        r = _row(spot=24950.0, atm_strike=24900.0, updated_at="f1")
        buf.add(r, mono=1015.0, chain={24900.0: {"ce": 160.0, "pe": 170.0}})
        enr = Enricher()
        enriched = enr.enrich(row0, capture_mono=1000.0, buffer=buf)
        self.assertIsNone(enriched["ce_15s"])   # pinned 24800 CE not available
        self.assertIsNotNone(enriched["spot_15s"])  # spot still resolves

    def test_dedup_and_seq(self):
        buf = RingBuffer()
        self.assertEqual(buf.next_seq(), 1)
        buf.add(_row(updated_at="A"), mono=1000.0)
        self.assertTrue(buf.is_duplicate("A"))
        self.assertFalse(buf.is_duplicate("B"))
        self.assertFalse(buf.is_duplicate(None))


if __name__ == "__main__":
    unittest.main()

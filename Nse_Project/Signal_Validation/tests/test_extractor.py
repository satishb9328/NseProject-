"""Deterministic unit tests for logger.extractor.Extractor."""

import sys
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from logger.extractor import Extractor

IST = timezone(timedelta(hours=5, minutes=30))


def _snapshot(spot, updated_at, instruments):
    return {
        "spot_price": spot,
        "expiry_date": "2026-11-26",
        "updated_at": updated_at,
        "data_source": "redis",
        "feed_connected": True,
        "stale": False,
        "seconds_since_last_tick": 0.4,
        "instrument_data": instruments,
    }


class TestExtractor(unittest.TestCase):
    def setUp(self):
        self.ex = Extractor()

    def test_extract_full_row(self):
        snap = _snapshot(24800.0, "2026-11-18T09:30:00+05:30", {
            "ce": {"strike_price": 24800.0, "option_type": "CE", "ltp": 212.4},
            "pe": {"strike_price": 24800.0, "option_type": "PE", "ltp": 198.6},
            "ce2": {"strike_price": 24900.0, "option_type": "CE", "ltp": 150.0},
            "pe2": {"strike_price": 24900.0, "option_type": "PE", "ltp": 160.0},
        })
        ms = {"bias": 3.2, "momentum": 1.8, "score": 58.3, "gamma_boost_val": 0.7, "iv_skew_est": 1.1}
        row = self.ex.extract(snap, ms, seq=1, captured_at=datetime(2026, 11, 18, 9, 30, 0, tzinfo=IST))
        self.assertIsNotNone(row)
        self.assertEqual(row["row_type"], "capture")
        self.assertEqual(row["atm_strike"], 24800.0)
        self.assertEqual(row["atm_ce_ltp"], 212.4)
        self.assertEqual(row["atm_pe_ltp"], 198.6)
        self.assertEqual(row["score"], 58.3)
        self.assertEqual(row["session_date"], "2026-11-18")
        self.assertEqual(row["seq"], 1)
        self.assertTrue(row["feed_connected"])
        self.assertFalse(row["stale"])

    def test_build_chain(self):
        snap = _snapshot(24950.0, None, {
            "c1": {"strike_price": 24800.0, "option_type": "CE", "ltp": 220.0},
            "p1": {"strike_price": 24800.0, "option_type": "PE", "ltp": 190.0},
            "c2": {"strike_price": 24900.0, "option_type": "CE", "ltp": 160.0},
            "p2": {"strike_price": 24900.0, "option_type": "PE", "ltp": 170.0},
        })
        chain = self.ex.build_chain(snap)
        self.assertEqual(chain[24800.0]["ce"], 220.0)
        self.assertEqual(chain[24800.0]["pe"], 190.0)
        self.assertEqual(chain[24900.0]["ce"], 160.0)

    def test_nearest_strike_picks_closest_to_spot(self):
        snap = _snapshot(24950.0, None, {
            "c1": {"strike_price": 24800.0, "option_type": "CE", "ltp": 220.0},
            "p1": {"strike_price": 24800.0, "option_type": "PE", "ltp": 190.0},
            "c2": {"strike_price": 24900.0, "option_type": "CE", "ltp": 160.0},
            "p2": {"strike_price": 24900.0, "option_type": "PE", "ltp": 170.0},
        })
        row = self.ex.extract(snap, {}, seq=1, captured_at=datetime(2026, 11, 18, 9, 30, 0, tzinfo=IST))
        self.assertEqual(row["atm_strike"], 24900.0)

    def test_drop_missing_spot(self):
        snap = {"updated_at": "2026-11-18T09:30:00+05:30", "instrument_data": {}}
        self.assertIsNone(self.ex.extract(snap, {}, seq=1))

    def test_missing_signal_defaults_to_neutral(self):
        snap = _snapshot(24800.0, "2026-11-18T09:30:00+05:30", {
            "ce": {"strike_price": 24800.0, "option_type": "CE", "ltp": 212.4},
            "pe": {"strike_price": 24800.0, "option_type": "PE", "ltp": 198.6},
        })
        row = self.ex.extract(snap, {}, seq=1, captured_at=datetime(2026, 11, 18, 9, 30, 0, tzinfo=IST))
        self.assertEqual(row["bias"], 0.0)
        self.assertEqual(row["score"], 50.0)


if __name__ == "__main__":
    unittest.main()

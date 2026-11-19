"""Deterministic unit tests for evaluator.loader.Loader."""

import json
import os
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from evaluator.loader import Loader  # noqa: E402


class TestLoader(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="sigval_ld_")
        os.makedirs(os.path.join(self.tmp, "processed"))
        os.makedirs(os.path.join(self.tmp, "raw"))
        self.cfg = {
            "paths": {"processed_dir": os.path.join(self.tmp, "processed"),
                      "raw_dir": os.path.join(self.tmp, "raw")},
            "horizons": [15, 30, 60, 120],
        }
        self.date = "2026-11-18"

    def _proc_row(self, i, missing_forward=False, stale=False, connected=True, source="redis"):
        r = {"row_type": "enriched", "ts": f"2026-11-18T09:30:{i % 60:02d}+05:30",
             "session_date": self.date, "spot": 24800.0 + i, "active_expiry": "2026-11-26",
             "atm_strike": 24800.0, "bias": 1.0, "momentum": 1.0, "gamma_boost": 0.5,
             "score": 55.0, "iv_skew": 1.0, "atm_ce_ltp": 200.0, "atm_pe_ltp": 200.0,
             "data_source": source, "feed_connected": connected, "stale": stale,
             "seconds_since_last_tick": 0.4}
        for h in (15, 30, 60, 120):
            r[f"spot_{h}s"] = None if missing_forward else 24800.0 + i + 1
            r[f"ce_{h}s"] = None if missing_forward else 200.0
            r[f"pe_{h}s"] = None if missing_forward else 200.0
        r["complete"] = not missing_forward
        return r

    def _write(self, subdir, rows):
        name = f"signal_{self.date}.enriched.jsonl" if subdir == "processed" else f"signal_{self.date}.jsonl"
        with open(os.path.join(self.tmp, subdir, name), "w", encoding="utf-8") as fh:
            for r in rows:
                fh.write(json.dumps(r) + "\n")

    def test_filters_quality_and_recovers_missing(self):
        rows = [self._proc_row(i) for i in range(10)]
        rows[2]["stale"] = True
        rows[3]["feed_connected"] = False
        rows[4]["data_source"] = "cold_json"
        rows[5] = self._proc_row(5, missing_forward=True)
        self._write("processed", rows)

        raw = []
        for i in range(40):  # enough rows so ts+15s exists for recovery
            raw.append({"row_type": "capture", "ts": f"2026-11-18T09:30:{i % 60:02d}+05:30",
                        "session_date": self.date, "spot": 24800.0 + i,
                        "active_expiry": "2026-11-26", "atm_strike": 24800.0,
                        "atm_ce_ltp": 200.0, "atm_pe_ltp": 200.0})
        self._write("raw", raw)

        loader = Loader(self.cfg)
        df = loader.load([self.date])
        self.assertEqual(len(df), 7)  # 10 - 3 quality-filtered
        self.assertEqual(df["spot_15s"].isna().sum(), 0)  # row 5 recovered from raw

    def test_discover_dates(self):
        self._write("processed", [self._proc_row(0)])
        loader = Loader(self.cfg)
        self.assertIn(self.date, loader.discover_dates())


if __name__ == "__main__":
    unittest.main()

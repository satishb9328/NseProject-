"""Deterministic unit tests for evaluator.metrics."""

import sys
import unittest
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from evaluator import metrics
from evaluator import labels


def _make_df(n=200):
    rows = []
    for i in range(n):
        bull = (i % 2 == 0)
        rows.append({
            "ts": f"2026-11-18T09:30:{i % 60:02d}+05:30",
            "spot": 24800.0, "atm_strike": 24800.0, "atm_ce_ltp": 200.0, "atm_pe_ltp": 200.0,
            "score": 60.0 if bull else 40.0, "bias": 1.0 if bull else -1.0,
            "momentum": 0.5 if bull else -0.5, "gamma_boost": 0.3 if bull else -0.3,
            "iv_skew": 1.0 if bull else -1.0,
            "spot_15s": 24803.0 if bull else 24797.0,
            "spot_30s": 24803.0 if bull else 24797.0,
            "ce_15s": 203.0 if bull else 197.0,
            "pe_15s": 197.0 if bull else 203.0,
        })
    return pd.DataFrame(rows)


def _config():
    return {
        "horizons": [15, 30],
        "labels": {"cost": {"spot_points": 2.0, "option_pct": 1.0}},
        "signals": {"score": {"bull_threshold": 55.0, "bear_threshold": 45.0}},
    }


class TestMetrics(unittest.TestCase):
    def test_edge_delta_is_signal_minus_always_long(self):
        df = labels.build(_make_df(200), _config())
        rows = metrics.compute_metric_rows(df, _config())
        bull = [r for r in rows if r["signal"] == "score" and r["bucket"] == "bull"
                and r["horizon"] == 15 and r["outcome"] == "spot"][0]
        self.assertAlmostEqual(bull["hit_rate"], 1.0)        # bull rows all rise
        self.assertAlmostEqual(bull["edge_delta"], 0.5, places=6)  # 1.0 - 0.5 always-long
        self.assertIn("max_drawdown_cooldown", bull)
        self.assertIn("n_cooldown_trades", bull)
        ce = [r for r in rows if r["signal"] == "score" and r["bucket"] == "bull"
              and r["horizon"] == 15 and r["outcome"] == "ce"][0]
        self.assertIsNone(ce["edge_delta"])  # edge delta is spot-only

    def test_cooldown_sample_spacing(self):
        ts = np.array([1000.0, 1010.0, 1020.0, 1035.0])
        vals = np.array([0.0, 1.0, 2.0, 3.0])
        sampled = metrics._cooldown_sample(ts, vals, horizon_seconds=15.0)
        self.assertEqual(sampled.tolist(), [0.0, 2.0, 3.0])

    def test_benchmarks(self):
        df = labels.build(_make_df(200), _config())
        bench = metrics.compute_benchmarks(df, _config())
        self.assertIn("15", bench)
        self.assertAlmostEqual(bench["15"]["always_long_hit_rate"], 0.5)
        self.assertAlmostEqual(bench["15"]["always_short_hit_rate"], 0.5)


if __name__ == "__main__":
    unittest.main()

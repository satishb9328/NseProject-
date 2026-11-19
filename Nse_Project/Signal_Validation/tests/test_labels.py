"""Deterministic unit tests for evaluator.labels."""

import sys
import unittest
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from evaluator import labels


def _df():
    n = 6
    return pd.DataFrame({
        "spot": [24800.0] * n,
        "atm_ce_ltp": [200.0] * n,
        "atm_pe_ltp": [200.0] * n,
        "score": [60.0, 40.0, 50.0, 60.0, 40.0, 50.0],
        "bias": [1.0, -1.0, 0.0, 1.0, -1.0, 0.0],
        "momentum": [0.5, -0.5, 0.0, 0.5, -0.5, 0.0],
        "gamma_boost": [0.3, -0.3, 0.0, 0.3, -0.3, 0.0],
        "iv_skew": [1.0, -1.0, 0.0, 1.0, -1.0, 0.0],
        "spot_15s": [24803.0, 24797.0, 24800.0, 24801.0, 24799.0, 24800.0],
        "ce_15s": [203.0, 197.0, 200.0, 200.0, 200.0, 200.0],
        "pe_15s": [197.0, 203.0, 200.0, 200.0, 200.0, 200.0],
    })


class TestLabels(unittest.TestCase):
    def test_build_returns(self):
        df = labels.build_returns(_df(), [15])
        self.assertIn("spot_move_15_pts", df)
        self.assertIn("spot_ret_15_bp", df)
        self.assertIn("ce_ret_15_pct", df)
        self.assertAlmostEqual(df.loc[0, "spot_move_15_pts"], 3.0)
        self.assertAlmostEqual(df.loc[0, "ce_ret_15_pct"], 1.5)  # (203-200)/200*100

    def test_build_directions(self):
        cfg = {"signals": {"score": {"bull_threshold": 55.0, "bear_threshold": 45.0}}}
        df = labels.build_directions(_df(), cfg)
        self.assertEqual(list(df["dir_score"]), [1, -1, 0, 1, -1, 0])
        self.assertEqual(list(df["dir_bias"]), [1.0, -1.0, 0.0, 1.0, -1.0, 0.0])
        self.assertEqual(df.loc[1, "dir_iv_skew"], 1.0)  # falling skew -> bullish

    def test_classify(self):
        direction = np.array([1, 1, -1, -1, 0])
        returns = np.array([3.0, -3.0, -3.0, 3.0, 5.0])
        self.assertEqual(list(labels.classify(direction, returns, 2.0)),
                         ["success", "failure", "success", "failure", "neutral"])


if __name__ == "__main__":
    unittest.main()

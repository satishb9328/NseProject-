"""Deterministic unit tests for logger.writer.JsonlWriter."""

import json
import os
import sys
import tempfile
import time
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from logger.writer import JsonlWriter


def _row(date="2026-11-18", seq=1):
    return {"row_type": "capture", "ts": "2026-11-18T09:30:00+05:30",
            "session_date": date, "seq": seq, "spot": 24800.0}


class TestWriter(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="sigval_tw_")
        self.w = JsonlWriter(data_dir=self.tmp, flush_interval_seconds=5.0, flush_max_rows=100)

    def tearDown(self):
        self.w.close()

    def test_write_and_flush_creates_dirs(self):
        self.w.write_raw(_row())
        self.w.write_processed({"row_type": "enriched", "ts": "2026-11-18T09:30:00+05:30",
                                "session_date": "2026-11-18", "seq": 1, "complete": True})
        self.w.flush()
        raw = Path(self.tmp) / "raw" / "signal_2026-11-18.jsonl"
        proc = Path(self.tmp) / "processed" / "signal_2026-11-18.enriched.jsonl"
        self.assertTrue(raw.exists())
        self.assertTrue(proc.exists())
        lines = raw.read_text(encoding="utf-8").strip().splitlines()
        self.assertEqual(len(lines), 1)
        self.assertEqual(json.loads(lines[0])["seq"], 1)

    def test_rotation_by_date(self):
        self.w.write_raw(_row("2026-11-18"))
        self.w.write_raw(_row("2026-11-19", seq=2))
        self.w.flush()
        self.assertTrue((Path(self.tmp) / "raw" / "signal_2026-11-18.jsonl").exists())
        self.assertTrue((Path(self.tmp) / "raw" / "signal_2026-11-19.jsonl").exists())

    def test_truncate_partial_line_recovery(self):
        path = Path(self.tmp) / "raw" / "signal_2026-11-18.jsonl"
        os.makedirs(path.parent, exist_ok=True)
        path.write_text(json.dumps(_row()) + "\n" + json.dumps(_row(seq=2))[:50], encoding="utf-8")
        w2 = JsonlWriter(data_dir=self.tmp)
        w2.write_raw(_row(seq=3))
        w2.flush()
        w2.close()
        lines = path.read_text(encoding="utf-8").strip().splitlines()
        self.assertEqual(len(lines), 2)  # partial line truncated, new row appended
        self.assertEqual(json.loads(lines[-1])["seq"], 3)

    def test_retention_sweep(self):
        old = Path(self.tmp) / "raw" / "signal_2000-01-01.jsonl"
        os.makedirs(old.parent, exist_ok=True)
        old.write_text("{}\n", encoding="utf-8")
        os.utime(old, (0, 0))
        self.w.sweep_retention(now_epoch=time.time())
        self.assertFalse(old.exists())


if __name__ == "__main__":
    unittest.main()

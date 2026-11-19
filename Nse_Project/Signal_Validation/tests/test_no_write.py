"""Prove the read-only Redis wrapper never mutates live keys or publishes."""

import json
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from logger.collector import Collector, ReadOnlyRedis, RedisReadOnlyError, _WRITE_METHODS


class _SpyRedis:
    def __init__(self, store=None):
        self.store = store or {}
        self.calls = []

    def get(self, key):
        self.calls.append("get")
        return self.store.get(key)

    def pubsub(self, **kwargs):
        self.calls.append("pubsub")
        return _FakePubSub()


class _FakePubSub:
    def subscribe(self, *channels):
        pass

    def get_message(self, timeout=None):
        return None

    def close(self):
        pass


class TestNoWrite(unittest.TestCase):
    def test_every_write_command_is_blocked(self):
        for name in _WRITE_METHODS:
            wrapped = ReadOnlyRedis(_SpyRedis())
            with self.assertRaises(RedisReadOnlyError, msg=f"{name} should be blocked"):
                getattr(wrapped, name)

    def test_critical_commands_in_denylist(self):
        for cmd in ("set", "publish", "delete", "hset", "lpush", "pipeline", "multi", "eval", "flushdb", "shutdown"):
            self.assertIn(cmd, _WRITE_METHODS)

    def test_collector_read_path_only_calls_read_methods(self):
        spy = _SpyRedis({
            "live_options_data": json.dumps({"spot_price": 123.0}),
            "market_state:2026-11-19": json.dumps({"score": 55.0}),
        })
        col = Collector()
        col._client = ReadOnlyRedis(spy)
        self.assertEqual(col.get_snapshot(), {"spot_price": 123.0})
        self.assertEqual(col.get_market_state("2026-11-19"), {"score": 55.0})
        self.assertEqual(set(spy.calls), {"get"})  # only reads, never set/publish/...


if __name__ == "__main__":
    unittest.main()

"""Read-only Redis collector (Phase 3).

Subscribes to the publish channel and GETs the two snapshot keys, with
exponential-backoff reconnect. The Redis client is wrapped so every write
command raises ``RedisReadOnlyError`` — this validator can only ever READ the
live platform's Redis, never mutate it.
"""

from __future__ import annotations

import json
import logging
import time
from datetime import datetime
from typing import Callable, Optional

import redis

from . import IST

logger = logging.getLogger("sigval.collector")

# Redis write commands forbidden in this read-only collector. Read methods
# (get, pubsub, ping, ...) pass through unchanged.
_WRITE_METHODS = frozenset({
    "append", "bgrewriteaof", "bgsave", "blpop", "brpop", "brpoplpush",
    "client_kill", "client_setname", "config_set", "decr", "decrby", "delete",
    "dump", "eval", "evalsha", "expire", "expireat", "flushall", "flushdb",
    "getset", "hdel", "hincrby", "hincrbyfloat", "hmset", "hset", "hsetnx",
    "incr", "incrby", "incrbyfloat", "linsert", "lock", "lpop", "lpush",
    "lpushx", "lrem", "lset", "ltrim", "move", "mset", "msetnx", "multi",
    "persist", "pexpire", "pexpireat", "pipeline", "publish", "rename",
    "renamenx", "restore", "rpop", "rpoplpush", "rpush", "rpushx", "sadd",
    "save", "script_flush", "script_kill", "script_load", "sdiffstore",
    "set", "setex", "setnx", "setrange", "shutdown", "sinterstore", "smove",
    "spop", "srem", "sunionstore", "swapdb", "transaction", "unlink", "zadd",
    "zincrby", "zinterstore", "zrem", "zremrangebylex", "zremrangebyrank",
    "zremrangebyscore", "zunionstore",
})


class RedisReadOnlyError(RuntimeError):
    """Raised when a write command is attempted on the read-only client."""


class ReadOnlyRedis:
    """Wraps a redis client and forbids every write command via ``__getattr__``."""

    def __init__(self, client) -> None:
        self._client = client

    def __getattr__(self, name: str):
        if name in _WRITE_METHODS:
            raise RedisReadOnlyError(f"write command '{name}' is forbidden in read-only mode")
        return getattr(self._client, name)

    def __repr__(self) -> str:
        return f"ReadOnlyRedis({self._client!r})"


class Collector:
    """Read-only Redis pub/sub listener + snapshot/metrics fetcher."""

    def __init__(
        self,
        url: str = "redis://127.0.0.1:6379/0",
        channel: str = "gamma:live_options_updated",
        data_key: str = "live_options_data",
        market_state_key_prefix: str = "market_state",
        *,
        socket_timeout: float = 5.0,
        socket_connect_timeout: float = 5.0,
        backoff_start: float = 1.0,
        backoff_max: float = 30.0,
    ) -> None:
        self.url = url
        self.channel = channel
        self.data_key = data_key
        self.market_state_key_prefix = market_state_key_prefix
        self.socket_timeout = float(socket_timeout)
        self.socket_connect_timeout = float(socket_connect_timeout)
        self.backoff_start = float(backoff_start)
        self.backoff_max = float(backoff_max)
        self._client: Optional[ReadOnlyRedis] = None

    def _get_client(self) -> ReadOnlyRedis:
        """Lazily build (and cache) the wrapped client; no network at init."""
        if self._client is None:
            raw = redis.Redis.from_url(
                self.url,
                decode_responses=True,
                socket_timeout=self.socket_timeout,
                socket_connect_timeout=self.socket_connect_timeout,
            )
            self._client = ReadOnlyRedis(raw)
        return self._client

    @staticmethod
    def ist_date() -> str:
        return datetime.now(IST).date().isoformat()


    def get_snapshot(self) -> Optional[dict]:
        """GET ``live_options_data`` and JSON-decode it; None when absent/unparseable."""
        client = self._get_client()
        raw = client.get(self.data_key)
        return self._parse_json(raw)

    def get_market_state(self, date_iso: Optional[str] = None) -> Optional[dict]:
        """GET ``market_state:{IST_date}`` and JSON-decode it."""
        client = self._get_client()
        date = date_iso or self.ist_date()
        raw = client.get(f"{self.market_state_key_prefix}:{date}")
        return self._parse_json(raw)

    @staticmethod
    def _parse_json(raw) -> Optional[dict]:
        if raw is None:
            return None
        try:
            data = json.loads(raw)
        except (TypeError, ValueError):
            logger.warning("Ignoring unparseable Redis payload")
            return None
        return data if isinstance(data, dict) else None

    def run_listener(
        self,
        on_update: Callable[[dict, dict], None],
        stop_event,
        on_status: Optional[Callable[[str], None]] = None,
    ) -> None:
        """Blocking pub/sub loop with exponential backoff (1s..30s).

        ``on_update(snapshot, market_state)`` is called for each publish.
        ``on_status(state)`` receives ``"connected"`` / ``"reconnecting"`` /
        ``"stopped"``. Runs until ``stop_event`` is set.
        """
        backoff = self.backoff_start
        while not stop_event.is_set():
            pubsub = None
            try:
                client = self._get_client()
                pubsub = client.pubsub(ignore_subscribe_messages=True)
                pubsub.subscribe(self.channel)
                backoff = self.backoff_start
                if on_status:
                    on_status("connected")
                while not stop_event.is_set():
                    message = pubsub.get_message(timeout=1.0)
                    if message is None or message.get("type") != "message":
                        continue
                    snapshot = self.get_snapshot()
                    market_state = self.get_market_state() or {}
                    if snapshot is not None:
                        on_update(snapshot, market_state)
            except redis.exceptions.RedisError as exc:
                logger.warning("Redis error in listener (reconnecting): %s", exc)
                if on_status:
                    on_status("reconnecting")
            except Exception as exc:  # noqa: BLE001 — keep the worker alive
                logger.exception("Unexpected listener error: %s", exc)
                if on_status:
                    on_status("reconnecting")
            finally:
                if pubsub is not None:
                    try:
                        pubsub.close()
                    except Exception:
                        pass
            if stop_event.is_set():
                break
            time.sleep(backoff)
            backoff = min(backoff * 2.0, self.backoff_max)
        if on_status:
            on_status("stopped")

    def close(self) -> None:
        if self._client is not None:
            try:
                self._client.close()
            except Exception:
                pass
            self._client = None


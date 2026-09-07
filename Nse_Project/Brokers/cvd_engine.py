"""
Cumulative Volume Delta (CVD) engine for live tick pipelines.

What it does
------------
For every live tick it:
  1. derives the *per-tick* volume = current cumulative day volume - previous
     cumulative day volume (brokers send TOTAL traded volume, not tick volume);
  2. classifies the tick delta (aggressive buy / sell) against Best Bid / Best Ask:
         LTP >= Best Ask   ->  +tick_volume
         LTP <= Best Bid   ->  -tick_volume
         otherwise         ->   0
  3. groups ticks into 2-minute OHLCV candles using Pandas;
  4. keeps a running Cumulative Volume Delta (CVD);
  5. computes ``cvd_trend_4m`` = CVD(now) - CVD(2 periods ago)  [2 x 2m = 4m];
  6. publishes each finished candle to Redis Pub/Sub.

Threading model (never blocks the broker ``on_message`` loop)
-------------------------------------------------------------
``ingest()`` / ``ingest_fyers_message()`` / ``ingest_upstox_message()`` are called
from the WebSocket thread and ONLY enqueue into a bounded ``queue.Queue``.
A single daemon worker thread drains the queue, runs the (heavy-ish) Pandas
resampling and publishes to Redis.

Persistence / reset
-------------------
CVD state is held in memory for the whole trading day and resets once per session.
The first tick of a new calendar day is the 09:15 IST open, so a calendar rollover
is the 09:15 reset. Call ``CVDEngine.reset`` from a 09:15 scheduler for an explicit
hard reset.

Integration examples
--------------------
Fyers ``on_message`` (raw dict)::

    cvd = CVDEngine(redis_url="redis://127.0.0.1:6379/0")
    cvd.start()
    # inside FyersSocketManager._on_message(msg):
    cvd.ingest_fyers_message(msg)

Upstox V3 ``on_message`` (decoded envelope)::

    cvd.ingest_upstox_message(message)

Or, from an existing Fyers ``Tick`` object (the consumer thread)::

    cvd.ingest(CVDTick.from_fyers_tick(tick))
"""

from __future__ import annotations

import json
import logging
import queue
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from datetime import time as time_of_day
from typing import Any, Callable, Dict, List, Optional, Tuple

import pandas as pd

try:
    import redis
except ImportError:  # keeps the engine importable even if redis-py is missing
    redis = None

logger = logging.getLogger(__name__)

# India market timezone + session open. CVD resets at the 09:15 IST open.
IST = timezone(timedelta(hours=5, minutes=30))
MARKET_OPEN = time_of_day(9, 15)

# ── Redis defaults (override with env vars in your deployment) ──────────────────
DEFAULT_REDIS_URL = "redis://127.0.0.1:6379/0"
DEFAULT_REDIS_DATA_KEY = "cvd_2m_snapshot"
DEFAULT_REDIS_CHANNEL = "cvd:2m_candle_closed"


def _to_float(value: Any) -> Optional[float]:
    """Coerce a value to float, returning None for missing/NaN/unparseable."""
    if value is None:
        return None
    try:
        f = float(value)
    except (TypeError, ValueError):
        return None
    return f if f == f else None


def _to_int(value: Any) -> int:
    """Coerce a value to int, returning 0 for missing/unparseable (handles '325')."""
    if value is None:
        return 0
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return 0


def _coerce_timestamp(value: Any) -> datetime:
    """Normalise a timestamp (epoch s/ms, ISO string, datetime) to tz-aware datetime."""
    if value is None:
        return datetime.now(IST)
    if isinstance(value, datetime):
        return value
    if isinstance(value, (int, float)):
        if value > 1e12:  # milliseconds -> seconds
            value /= 1000.0
        return datetime.fromtimestamp(value, tz=IST)
    if isinstance(value, str):
        s = value.strip()
        if s.isdigit() or (s.startswith("-") and s[1:].isdigit()):
            v = float(s)
            if v > 1e12:
                v /= 1000.0
            return datetime.fromtimestamp(v, tz=IST)
        try:
            return pd.Timestamp(s).to_pydatetime()
        except Exception:
            return datetime.now(IST)
    return datetime.now(IST)


def _find_value_by_key(node: Any, key: str) -> Any:
    """Depth-first search for the first value under an exact (case-insensitive) key."""
    if isinstance(node, dict):
        for k, v in node.items():
            if isinstance(k, str) and k.lower() == key.lower():
                return v
        for v in node.values():
            found = _find_value_by_key(v, key)
            if found is not None:
                return found
    elif isinstance(node, list):
        for item in node:
            found = _find_value_by_key(item, key)
            if found is not None:
                return found
    return None


def _extract_fyers_bid_ask(msg: dict) -> Tuple[Optional[float], Optional[float]]:
    """Best bid / best ask from a Fyers tick payload (handles depth arrays)."""
    bid = _to_float(msg.get("bid"))
    ask = _to_float(msg.get("ask"))
    if bid is None:
        bids = msg.get("bids")
        if isinstance(bids, list) and bids and isinstance(bids[0], (list, tuple)):
            bid = _to_float(bids[0][0])
    if ask is None:
        asks = msg.get("asks")
        if isinstance(asks, list) and asks and isinstance(asks[0], (list, tuple)):
            ask = _to_float(asks[0][0])
    if bid is None:
        bid = _to_float(msg.get("bp1", msg.get("bidP")))
    if ask is None:
        ask = _to_float(msg.get("sp1", msg.get("ap1", msg.get("askP"))))
    return bid, ask


@dataclass
class CVDTick:
    """Broker-agnostic tick normalised for CVD computation."""

    symbol: str
    ltp: float
    bid: Optional[float]
    ask: Optional[float]
    total_volume: int  # cumulative day volume, NOT tick volume
    timestamp: datetime = field(default_factory=lambda: datetime.now(IST))

    @classmethod
    def from_fyers_message(cls, msg: dict) -> "CVDTick":
        bid, ask = _extract_fyers_bid_ask(msg)
        return cls(
            symbol=msg.get("symbol", ""),
            ltp=_to_float(msg.get("ltp")) or 0.0,
            bid=bid,
            ask=ask,
            total_volume=_to_int(msg.get("vol_traded", msg.get("vol", 0))),
            timestamp=_coerce_timestamp(
                msg.get("timestamp") or msg.get("ltt") or msg.get("ts")
            ),
        )

    @classmethod
    def from_fyers_tick(cls, tick) -> "CVDTick":
        """Convert an existing Fyers ``Tick`` dataclass into a ``CVDTick``."""
        return cls(
            symbol=tick.symbol,
            ltp=tick.ltp,
            bid=getattr(tick, "bid", None),
            ask=getattr(tick, "ask", None),
            total_volume=getattr(tick, "volume", 0),
            timestamp=tick.timestamp,
        )

    @classmethod
    def from_upstox_feed(
        cls, instrument_key: str, feed_payload: dict
    ) -> Optional["CVDTick"]:
        return upstox_feed_to_tick(instrument_key, feed_payload)


def upstox_feed_to_tick(instrument_key: str, feed_payload: dict) -> Optional["CVDTick"]:
    """Extract one ``CVDTick`` from a single Upstox V3 ``feeds`` entry.

    Verified against the live V3 payload:
        fullFeed.marketFF.ltpc.ltp                       -> LTP
        fullFeed.marketFF.ltpc.ltt                       -> epoch ms
        fullFeed.marketFF.vtt                            -> cumulative day volume
        fullFeed.marketFF.marketLevel.bidAskQuote[0]     -> {bidP, askP}
    """
    if not isinstance(feed_payload, dict):
        return None
    ff = feed_payload.get("fullFeed") or feed_payload
    if not isinstance(ff, dict):
        return None
    market_ff = ff.get("marketFF") or ff
    if not isinstance(market_ff, dict):
        return None

    ltpc = market_ff.get("ltpc") or {}
    ltp = _to_float(ltpc.get("ltp"))
    if ltp is None:
        return None

    # Volume Traded Today ("vtt") is cumulative day volume, NOT tick volume.
    total_volume = 0
    vtt = _find_value_by_key(market_ff, "vtt")
    if vtt is None:
        vtt = _find_value_by_key(market_ff, "volume")
    if vtt is not None:
        total_volume = _to_int(vtt)

    bid = ask = None
    market_level = market_ff.get("marketLevel") if isinstance(market_ff, dict) else None
    bid_ask = market_level.get("bidAskQuote") if isinstance(market_level, dict) else None
    if isinstance(bid_ask, list) and bid_ask and isinstance(bid_ask[0], dict):
        bid = _to_float(bid_ask[0].get("bidP"))
        ask = _to_float(bid_ask[0].get("askP"))

    return CVDTick(
        symbol=instrument_key,
        ltp=ltp,
        bid=bid,
        ask=ask,
        total_volume=total_volume,
        timestamp=_coerce_timestamp(ltpc.get("ltt")),
    )


def upstox_message_to_ticks(msg: Any) -> List["CVDTick"]:
    """Convert an Upstox V3 message envelope into a list of ``CVDTick``."""
    ticks: List["CVDTick"] = []
    if not isinstance(msg, dict):
        return ticks

    feeds = None
    if isinstance(msg.get("data"), dict) and isinstance(msg["data"].get("feeds"), dict):
        feeds = msg["data"]["feeds"]
    elif isinstance(msg.get("feeds"), dict):
        feeds = msg["feeds"]

    if feeds:
        for instrument_key, feed_payload in feeds.items():
            tick = upstox_feed_to_tick(instrument_key, feed_payload)
            if tick is not None:
                ticks.append(tick)
        return ticks

    tick = upstox_feed_to_tick(msg.get("instrument_key", ""), msg)
    if tick is not None:
        ticks.append(tick)
    return ticks


class CVDEngine:
    """Cumulative Volume Delta aggregator (2-minute bars) with Redis Pub/Sub.

    ``ingest()`` (called from the broker WebSocket / ``on_message`` thread) only
    enqueues ticks into a bounded ``queue.Queue`` — it never touches Pandas or
    Redis. A single daemon worker drains the queue, computes per-tick volume
    deltas, groups ticks into 2-minute candles with Pandas, maintains the running
    CVD and publishes finished candles to Redis.

    CVD resets once per trading day (the 09:15 IST open, which in practice is the
    first tick of a new calendar day).
    """

    def __init__(
        self,
        candle_seconds: int = 120,
        trend_periods: int = 2,
        redis_url: Optional[str] = DEFAULT_REDIS_URL,
        redis_data_key: str = DEFAULT_REDIS_DATA_KEY,
        redis_channel: str = DEFAULT_REDIS_CHANNEL,
        tick_queue_maxsize: int = 100_000,
        flush_interval: float = 0.5,
        on_candle: Optional[Callable[[dict], None]] = None,
        clock: Optional[Callable[[], datetime]] = None,
        publish: bool = True,
    ):
        self.candle_seconds = int(candle_seconds)
        self.trend_periods = int(trend_periods)
        self.timeframe_label = (
            f"{self.candle_seconds // 60}m"
            if self.candle_seconds % 60 == 0
            else f"{self.candle_seconds}s"
        )
        self._freq = f"{self.candle_seconds}s"

        self.redis_url = redis_url
        self.redis_data_key = redis_data_key
        self.redis_channel = redis_channel
        self.publish_enabled = bool(publish) and redis_url is not None
        self.on_candle = on_candle
        self._clock = clock or (lambda: datetime.now(IST))
        self._flush_interval = float(flush_interval)

        self._tick_queue: "queue.Queue[CVDTick]" = queue.Queue(maxsize=tick_queue_maxsize)
        self._state_lock = threading.Lock()
        self._stop_event = threading.Event()
        self._worker: Optional[threading.Thread] = None
        self._redis_client = None
        self._redis_down = False
        self._session_date: Optional[date] = None
        self._states: Dict[str, dict] = {}

        self.logger = logging.getLogger(f"{__name__}.CVDEngine")

    # ── Public API ─────────────────────────────────────────────────────────────

    def start(self) -> None:
        """Start the background worker thread (idempotent)."""
        if self._worker is not None and self._worker.is_alive():
            return
        self._stop_event.clear()
        self._worker = threading.Thread(
            target=self._worker_loop, name="cvd-engine", daemon=True
        )
        self._worker.start()
        self.logger.info("CVD engine worker started (daemon thread).")

    def stop(self) -> None:
        """Signal the worker to stop and wait briefly for it to finish."""
        self._stop_event.set()
        if self._worker is not None:
            self._worker.join(timeout=2.0)

    def ingest(self, tick: "CVDTick") -> bool:
        """Enqueue a tick. Non-blocking; drops the oldest tick if the queue is full."""
        try:
            self._tick_queue.put_nowait(tick)
            return True
        except queue.Full:
            try:
                self._tick_queue.get_nowait()
                self._tick_queue.put_nowait(tick)
                return True
            except queue.Empty:
                return False

    def ingest_fyers_message(self, msg: dict) -> bool:
        """Parse a raw Fyers tick dict and enqueue it."""
        return self.ingest(CVDTick.from_fyers_message(msg))

    def ingest_upstox_message(self, msg: Any) -> int:
        """Parse an Upstox V3 message envelope and enqueue every instrument tick."""
        ticks = upstox_message_to_ticks(msg)
        for tick in ticks:
            self.ingest(tick)
        return len(ticks)

    def queue_size(self) -> int:
        return self._tick_queue.qsize()

    def latest_candle(self, symbol: str) -> Optional[dict]:
        with self._state_lock:
            st = self._states.get(symbol)
            return st["last_candle"] if st else None

    def snapshot(self) -> Dict[str, dict]:
        """Latest finalized candle per symbol (thread-safe read)."""
        with self._state_lock:
            return {
                symbol: st["last_candle"]
                for symbol, st in self._states.items()
                if st["last_candle"] is not None
            }

    def reset(self) -> None:
        """Manually reset cumulative state (e.g. from a scheduled 09:15 job)."""
        with self._state_lock:
            self._reset_all()
            self._session_date = None


    # ── Worker thread ──────────────────────────────────────────────────────────

    def _worker_loop(self) -> None:
        self.logger.info("CVD worker thread started.")
        while not self._stop_event.is_set():
            batch = self._drain()
            to_publish: List[dict] = []
            with self._state_lock:
                for tick in batch:
                    self._process_tick(tick)
                to_publish = self._flush_due_candles()
            for payload in to_publish:
                self._publish(payload)
        self.logger.info("CVD worker thread stopped.")

    def _drain(self) -> List["CVDTick"]:
        ticks: List["CVDTick"] = []
        try:
            ticks.append(self._tick_queue.get(timeout=self._flush_interval))
        except queue.Empty:
            return ticks
        while True:
            try:
                ticks.append(self._tick_queue.get_nowait())
            except queue.Empty:
                break
        return ticks

    # ── Tick -> delta / volume ──────────────────────────────────────────────────

    def _process_tick(self, tick: "CVDTick") -> None:
        if not tick.symbol:
            return
        ts = self._to_ist(tick.timestamp)
        self._check_session_reset(ts)
        st = self._state_for(tick.symbol)

        tick_volume = self._tick_volume(st, tick.total_volume)
        delta = self._tick_delta(tick.ltp, tick.bid, tick.ask, tick_volume)

        st["buffer"].append(
            {
                "ts": ts,
                "price": float(tick.ltp),
                "volume": int(tick_volume),
                "delta": float(delta),
            }
        )

    def _tick_volume(self, st: dict, total_volume: int) -> int:
        """Per-tick volume = current cumulative volume - previous cumulative volume."""
        total = int(total_volume or 0)
        prev = st["last_total_volume"]
        if prev is None:
            vol = 0  # first observation: baseline, no delta yet
        else:
            vol = total - prev
            if vol < 0:  # broker reset / reconnect / day rollover
                vol = 0
        st["last_total_volume"] = total
        return vol

    @staticmethod
    def _tick_delta(
        ltp: float, bid: Optional[float], ask: Optional[float], vol: int
    ) -> float:
        if vol == 0:
            return 0.0
        ltp = float(ltp)
        ask_f = _to_float(ask)
        bid_f = _to_float(bid)
        if ask_f is not None and ltp >= ask_f:  # aggressive buying
            return float(vol)
        if bid_f is not None and ltp <= bid_f:  # aggressive selling
            return -float(vol)
        return 0.0  # inside the spread / unknown


    # ── 2-minute resampling (Pandas) + CVD ─────────────────────────────────────

    def _flush_due_candles(self) -> List[dict]:
        now = self._now()
        current_bucket = self._floor_bucket(now)
        to_publish: List[dict] = []

        for symbol, st in list(self._states.items()):
            if not st["buffer"]:
                continue
            df = pd.DataFrame(st["buffer"])
            df["ts"] = pd.to_datetime(df["ts"])
            df["bucket"] = df["ts"].dt.floor(self._freq)

            closed = df[df["bucket"] < current_bucket]
            open_rows = df[df["bucket"] >= current_bucket].drop(columns=["bucket"])

            if not closed.empty:
                for bucket_ts, grp in closed.groupby("bucket", sort=True):
                    grp = grp.sort_values("ts")
                    to_publish.append(self._emit_candle(symbol, st, bucket_ts, grp))

            st["buffer"] = open_rows.to_dict("records")

        return to_publish

    def _emit_candle(self, symbol, st, bucket_ts, grp) -> dict:
        open_ = float(grp["price"].iloc[0])
        high = float(grp["price"].max())
        low = float(grp["price"].min())
        close = float(grp["price"].iloc[-1])
        volume = int(grp["volume"].sum())
        delta = float(grp["delta"].sum())

        st["cvd"] += delta
        st["cvd_history"].append((bucket_ts, st["cvd"]))
        cvd_trend = self._cvd_trend(st)

        candle = {
            "symbol": symbol,
            "timeframe": self.timeframe_label,
            "ts_open": bucket_ts.isoformat(),
            "ts_close": (bucket_ts + pd.Timedelta(seconds=self.candle_seconds)).isoformat(),
            "open": open_,
            "high": high,
            "low": low,
            "close": close,
            "volume": volume,
            "delta": delta,
            "cvd": st["cvd"],
            "cvd_trend_4m": cvd_trend,
        }
        st["last_candle"] = candle
        return candle

    def _cvd_trend(self, st: dict) -> Optional[float]:
        hist = st["cvd_history"]
        if len(hist) < self.trend_periods + 1:
            return None
        return float(hist[-1][1] - hist[-(self.trend_periods + 1)][1])

    # ── Session / state helpers ────────────────────────────────────────────────

    def _now(self) -> datetime:
        return self._clock()

    def _floor_bucket(self, ts: datetime) -> pd.Timestamp:
        return pd.Timestamp(ts).floor(self._freq)

    @staticmethod
    def _to_ist(ts: datetime) -> datetime:
        if ts.tzinfo is None:
            return ts.replace(tzinfo=IST)
        return ts.astimezone(IST)

    def _check_session_reset(self, ts: datetime) -> None:
        d = ts.date()
        if self._session_date is None:
            self._session_date = d
            return
        if d != self._session_date:
            # New trading day. In practice the first tick of a new day arrives at the
            # 09:15 IST open, so a calendar rollover == the 09:15 CVD reset.
            self._reset_all()
            self._session_date = d

    def _reset_all(self) -> None:
        self._states.clear()
        self.logger.info("CVD state reset for a new trading session (09:15 IST).")

    def _state_for(self, symbol: str) -> dict:
        st = self._states.get(symbol)
        if st is None:
            st = {
                "last_total_volume": None,
                "buffer": [],
                "cvd": 0.0,
                "cvd_history": deque(maxlen=self.trend_periods + 1),
                "last_candle": None,
            }
            self._states[symbol] = st
        return st


    # ── Redis Pub/Sub ──────────────────────────────────────────────────────────

    def _get_redis_client(self):
        if self._redis_client is None:
            self._redis_client = redis.Redis.from_url(self.redis_url, decode_responses=True)
        return self._redis_client

    def _publish(self, payload: dict) -> None:
        if self.on_candle:
            try:
                self.on_candle(payload)
            except Exception:
                self.logger.exception("on_candle callback failed")

        if not self.publish_enabled:
            return
        if redis is None:
            self.logger.warning("redis-py is not installed; skipping CVD Redis publish.")
            return

        try:
            client = self._get_redis_client()
            data = json.dumps(payload, default=str, separators=(",", ":"))
            # SET a snapshot key (for GET-based consumers) + PUBLISH the full payload
            # (for Pub/Sub subscribers / the Django dashboard).
            client.set(self.redis_data_key, data)
            client.publish(self.redis_channel, data)
            self._redis_down = False
        except redis.RedisError as exc:
            if not self._redis_down:
                self.logger.warning("Failed to publish CVD candle to Redis: %s", exc)
                self._redis_down = True


__all__ = [
    "CVDTick",
    "CVDEngine",
    "IST",
    "MARKET_OPEN",
    "upstox_message_to_ticks",
    "upstox_feed_to_tick",
]


# ════════════════════════════════════════════════════════════════════════════
# SELF-TEST (no broker / no Redis required)
# ════════════════════════════════════════════════════════════════════════════
def _self_test() -> None:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    )
    collected: List[dict] = []

    # Fixed clock so 2-minute bucket boundaries are deterministic.
    now_holder = {"now": datetime(2026, 9, 4, 10, 2, 30, tzinfo=IST)}

    engine = CVDEngine(
        candle_seconds=120,
        trend_periods=2,
        redis_url=None,  # no Redis in the self-test
        publish=False,
        on_candle=collected.append,
        clock=lambda: now_holder["now"],
    )

    def feed(ts, ltp, bid, ask, total_volume):
        engine._process_tick(
            CVDTick("NSE:RELIANCE-EQ", ltp, bid, ask, total_volume, ts)
        )

    # Bucket 10:00
    feed(datetime(2026, 9, 4, 10, 0, 10, tzinfo=IST), 100.0, 99.95, 100.05, 100)   # baseline vol=0
    feed(datetime(2026, 9, 4, 10, 0, 20, tzinfo=IST), 100.10, 99.95, 100.05, 150)  # buy +50
    feed(datetime(2026, 9, 4, 10, 0, 40, tzinfo=IST), 99.90, 99.95, 100.05, 160)   # sell -10
    feed(datetime(2026, 9, 4, 10, 1, 30, tzinfo=IST), 100.00, 99.95, 100.05, 170)  # inside spread 0
    # Bucket 10:02
    feed(datetime(2026, 9, 4, 10, 2, 5, tzinfo=IST), 101.10, 100.9, 101.05, 200)   # buy +30

    published = engine._flush_due_candles()
    c1 = published[-1]
    assert c1["delta"] == 40.0, f"10:00 delta {c1['delta']}"
    assert c1["volume"] == 70, f"10:00 volume {c1['volume']}"
    assert c1["cvd"] == 40.0, f"10:00 cvd {c1['cvd']}"
    assert (c1["open"], c1["high"], c1["low"], c1["close"]) == (100.0, 100.10, 99.90, 100.00)
    assert c1["cvd_trend_4m"] is None
    print(f"[OK] 10:00 candle -> delta={c1['delta']}, cvd={c1['cvd']}, trend={c1['cvd_trend_4m']}")

    # Advance past 10:02 and feed the 10:04 bucket's first tick.
    now_holder["now"] = datetime(2026, 9, 4, 10, 4, 30, tzinfo=IST)
    feed(datetime(2026, 9, 4, 10, 4, 5, tzinfo=IST), 102.10, 101.9, 102.05, 220)  # buy +20
    published = engine._flush_due_candles()
    c2 = published[-1]
    assert c2["delta"] == 30.0 and c2["cvd"] == 70.0, f"10:02 {c2}"
    assert c2["cvd_trend_4m"] is None
    print(f"[OK] 10:02 candle -> delta={c2['delta']}, cvd={c2['cvd']}, trend={c2['cvd_trend_4m']}")

    # Advance again to close 10:04 -> three candles now, trend = cvd(10:04) - cvd(10:00).
    now_holder["now"] = datetime(2026, 9, 4, 10, 6, 30, tzinfo=IST)
    feed(datetime(2026, 9, 4, 10, 6, 5, tzinfo=IST), 103.10, 102.9, 103.05, 240)  # buy +20
    published = engine._flush_due_candles()
    c3 = published[-1]
    assert c3["delta"] == 20.0 and c3["cvd"] == 90.0, f"10:04 {c3}"
    assert c3["cvd_trend_4m"] == 50.0, f"trend {c3['cvd_trend_4m']}"
    print(f"[OK] 10:04 candle -> delta={c3['delta']}, cvd={c3['cvd']}, trend_4m={c3['cvd_trend_4m']}")

    # ── Thread + queue smoke test ─────────────────────────────────────────────
    engine2 = CVDEngine(
        candle_seconds=120,
        trend_periods=2,
        redis_url=None,
        publish=False,
        on_candle=collected.append,
        clock=lambda: datetime(2026, 9, 4, 10, 6, 30, tzinfo=IST),
    )
    engine2.start()
    try:
        engine2.ingest(
            CVDTick("NSE:TCS-EQ", 3000.0, 2999.9, 3000.1, 1000,
                    datetime(2026, 9, 4, 10, 0, 5, tzinfo=IST))
        )
        engine2.ingest(
            CVDTick("NSE:TCS-EQ", 3000.2, 2999.9, 3000.1, 1200,
                    datetime(2026, 9, 4, 10, 0, 25, tzinfo=IST))
        )
        deadline = time.time() + 3.0
        while time.time() < deadline and not any(
            c.get("symbol") == "NSE:TCS-EQ" for c in collected
        ):
            time.sleep(0.05)
        assert any(c.get("symbol") == "NSE:TCS-EQ" for c in collected), (
            "worker did not emit a candle"
        )
        print("[OK] Worker thread + queue emitted a candle asynchronously.")
    finally:
        engine2.stop()

    print("ALL CVD ENGINE SELF-TESTS PASSED")


if __name__ == "__main__":
    _self_test()


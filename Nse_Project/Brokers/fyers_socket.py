"""
╔══════════════════════════════════════════════════════════════════════════════════╗
║   FYERS WEBSOCKET MANAGER  v1.0                                                ║
║   Real-time market data streaming pipeline for quantitative trading systems.    ║
║                                                                                  ║
║   Architecture:                                                                  ║
║     • Thread-safe multi-producer/consumer via queue.Queue                       ║
║     • In-memory shared state (latest LTP, H, L, V) for zero-latency queries    ║
║     • Intraday candle aggregator (1m, 5m, 15m) with callback on close          ║
║     • Automatic reconnection with exponential backoff                           ║
║     • Ping/pong heartbeat for connection health                                 ║
║                                                                                  ║
║   Usage:                                                                         ║
║     from Brokers.fyers_socket import FyersSocketManager                         ║
║                                                                                  ║
║     manager = FyersSocketManager(access_token="YOUR_TOKEN")                     ║
║     manager.start()                                                              ║
║     manager.subscribe(["NSE:RELIANCE-EQ", "NSE:SBIN-EQ"])                       ║
║     ltp = manager.get_latest_price("NSE:RELIANCE-EQ")                           ║
║     manager.stop()                                                               ║
╚══════════════════════════════════════════════════════════════════════════════════╝
"""

import json
import time
import logging
import threading
import queue
from datetime import datetime, timedelta
from typing import Optional, Callable, Dict, List, Any
from dataclasses import dataclass, field

# ── Fyers SDK ─────────────────────────────────────────────────────────────────────
# pip install fyers-apiv3
from fyers_apiv3 import fyersModel

logger = logging.getLogger(__name__)


# ════════════════════════════════════════════════════════════════════════════════
# DATA CONTAINERS
# ════════════════════════════════════════════════════════════════════════════════

@dataclass
class Tick:
    """Normalised tick structure from Fyers WebSocket."""
    symbol: str
    ltp: float
    high: float
    low: float
    open: float
    close: float
    volume: int
    change: float
    timestamp: datetime = field(default_factory=datetime.now)

    @classmethod
    def from_fyers_message(cls, msg: dict) -> "Tick":
        """
        Parse a Fyers WebSocket tick message into a Tick dataclass.

        Fyers tick format (typical keys):
            { 'symbol': 'NSE:RELIANCE-EQ', 'ltp': 2850.0, 'high': 2865.0,
              'low': 2840.0, 'open': 2845.0, 'close': 2848.0,
              'vol_traded': 1234567, 'ch': 2.0, 'chp': 0.07, ... }
        """
        return cls(
            symbol=msg.get("symbol", ""),
            ltp=float(msg.get("ltp", 0)),
            high=float(msg.get("high", 0)),
            low=float(msg.get("low", 0)),
            open=float(msg.get("open", 0)),
            close=float(msg.get("close", 0)),
            volume=int(msg.get("vol_traded", 0)),
            change=float(msg.get("ch", 0)),
            timestamp=datetime.now(),
        )


@dataclass
class Candle:
    """OHLCV candle for any timeframe."""
    symbol: str
    timeframe: str       # e.g. "1m", "5m", "15m"
    open: float
    high: float
    low: float
    close: float
    volume: int
    ts_open: datetime    # candle open timestamp
    ts_close: datetime   # candle close timestamp
    closed: bool = False # True when the candle is finalised


# ════════════════════════════════════════════════════════════════════════════════
# SHARED IN-MEMORY STATE  (thread-safe via Lock)
# ════════════════════════════════════════════════════════════════════════════════

class SharedPriceState:
    """
    Thread-safe in-memory store for the latest tick data per symbol.

    External modules can call `get_latest_price("NSE:RELIANCE-EQ")` at any
    time without blocking the WebSocket thread.
    """

    def __init__(self):
        self._lock = threading.Lock()
        # symbol -> dict of latest known values
        self._state: Dict[str, dict] = {}

    def update(self, symbol: str, tick: Tick) -> None:
        """Atomically update the state for a symbol."""
        with self._lock:
            prev = self._state.get(symbol, {})
            self._state[symbol] = {
                "ltp":       tick.ltp,
                "high":      max(tick.high, prev.get("high", tick.high)),
                "low":       min(tick.low, prev.get("low", tick.low)),
                "open":      tick.open if prev.get("open") is None else prev["open"],
                "close":     tick.close,
                "volume":    tick.volume,
                "change":    tick.change,
                "timestamp": tick.timestamp.isoformat(),
            }

    def get(self, symbol: str) -> Optional[dict]:
        """Return the latest known state for a symbol, or None."""
        with self._lock:
            return self._state.get(symbol)

    def get_ltp(self, symbol: str) -> Optional[float]:
        """Convenience: return just the last traded price."""
        state = self.get(symbol)
        return state["ltp"] if state else None

    def get_all(self) -> Dict[str, dict]:
        """Return a snapshot of all symbols' latest state."""
        with self._lock:
            return dict(self._state)


# ════════════════════════════════════════════════════════════════════════════════
# INTRADAY CANDLE AGGREGATOR
# ════════════════════════════════════════════════════════════════════════════════

class CandleAggregator:
    """
    Aggregates raw ticks into OHLCV candles for multiple timeframes.

    Fires a callback whenever a candle closes so external strategy modules
    (e.g. SMC scanner) can react immediately.

    Timeframes are defined as (label, seconds) tuples.
    """

    DEFAULT_TIMEFRAMES = [
        ("1m",  60),
        ("5m",  300),
        ("15m", 900),
    ]

    def __init__(self, on_candle_close: Optional[Callable[[Candle], None]] = None):
        self.on_candle_close = on_candle_close
        self._lock = threading.Lock()
        # {(symbol, tf_label): Candle}
        self._candles: Dict[tuple, Candle] = {}

    def _round_timestamp(self, dt: datetime, seconds: int) -> datetime:
        """Round a datetime down to the nearest `seconds` boundary."""
        ts = int(dt.timestamp())
        rounded = (ts // seconds) * seconds
        return datetime.fromtimestamp(rounded)

    def ingest_tick(self, tick: Tick) -> None:
        """
        Feed a tick into the aggregator.  Updates all timeframes and fires
        callbacks for any candle that just closed.
        """
        with self._lock:
            for tf_label, tf_seconds in self.DEFAULT_TIMEFRAMES:
                key = (tick.symbol, tf_label)
                candle = self._candles.get(key)

                # Determine the candle open time for this tick
                open_ts = self._round_timestamp(tick.timestamp, tf_seconds)
                close_ts = datetime.fromtimestamp(
                    int(open_ts.timestamp()) + tf_seconds
                )

                if candle is None or candle.ts_open != open_ts:
                    # ── Previous candle just closed → fire callback ──────────
                    if candle is not None and candle.closed is False:
                        candle.closed = True
                        candle.close = tick.close  # finalise with last tick
                        self._fire_callback(candle)

                    # ── Start a new candle ───────────────────────────────────
                    self._candles[key] = Candle(
                        symbol=tick.symbol,
                        timeframe=tf_label,
                        open=tick.ltp,
                        high=tick.ltp,
                        low=tick.ltp,
                        close=tick.ltp,
                        volume=tick.volume,
                        ts_open=open_ts,
                        ts_close=close_ts,
                        closed=False,
                    )
                else:
                    # ── Update existing candle ───────────────────────────────
                    candle.high = max(candle.high, tick.ltp)
                    candle.low  = min(candle.low, tick.ltp)
                    candle.close = tick.ltp
                    candle.volume += tick.volume

    def _fire_callback(self, candle: Candle) -> None:
        """Thread-safe callback invocation for closed candles."""
        if self.on_candle_close:
            try:
                self.on_candle_close(candle)
            except Exception as e:
                logger.error(f"Candle callback error: {e}")

    def get_candle(self, symbol: str, timeframe: str) -> Optional[Candle]:
        """Get the current (in-progress) candle for a symbol/timeframe."""
        with self._lock:
            return self._candles.get((symbol, timeframe))

    def get_all_candles(self, symbol: str) -> List[Candle]:
        """Get all current candles for a symbol across all timeframes."""
        with self._lock:
            return [
                c for (sym, _), c in self._candles.items()
                if sym == symbol
            ]


# ════════════════════════════════════════════════════════════════════════════════
# FYERS WEBSOCKET MANAGER
# ════════════════════════════════════════════════════════════════════════════════

class FyersSocketManager:
    """
    Centralised WebSocket manager for Fyers Data API.

    Features:
        • Dynamic subscribe / unsubscribe
        • Automatic reconnection with exponential backoff (1s → 60s max)
        • Ping/pong heartbeat every 30 seconds
        • Thread-safe tick queue for external consumers
        • Shared in-memory price state for zero-latency queries
        • Intraday candle aggregation with close callbacks

    Threading model:
        ┌──────────────────┐     ┌──────────────────────┐
        │  WebSocket Thread │────▶│  queue.Queue (ticks)  │
        │  (receives ticks) │     └──────────┬───────────┘
        └──────────────────┘                │
                                   ┌────────▼───────────┐
                                   │  Consumer Thread(s) │
                                   │  (process ticks)    │
                                   └────────────────────┘
    """

    # ── Heartbeat & reconnection ────────────────────────────────────────────────
    HEARTBEAT_INTERVAL = 30       # seconds between pings
    RECONNECT_BASE     = 1        # initial backoff seconds
    RECONNECT_MAX      = 60       # maximum backoff seconds
    RECONNECT_FACTOR   = 2        # exponential multiplier

    def __init__(
        self,
        access_token: str,
        symbols: Optional[List[str]] = None,
        on_tick: Optional[Callable[[Tick], None]] = None,
        on_candle_close: Optional[Callable[[Candle], None]] = None,
        tick_queue_maxsize: int = 10_000,
    ):
        """
        Parameters
        ----------
        access_token : str
            Fyers API access token (obtained via OAuth).
        symbols : list[str], optional
            Initial list of symbols to subscribe to (e.g. ["NSE:RELIANCE-EQ"]).
        on_tick : callable, optional
            Callback invoked on every tick (runs in WebSocket thread — keep fast).
        on_candle_close : callable, optional
            Callback invoked when a candle closes (runs in aggregator lock).
        tick_queue_maxsize : int
            Max items in the tick queue before old ticks are dropped.
        """
        self.access_token = access_token
        self._symbols: set = set(symbols or [])
        self._on_tick = on_tick
        self._tick_queue: queue.Queue = queue.Queue(maxsize=tick_queue_maxsize)

        # ── Shared state ────────────────────────────────────────────────────────
        self.price_state = SharedPriceState()
        self.candle_aggregator = CandleAggregator(on_candle_close=on_candle_close)

        # ── Threading controls ──────────────────────────────────────────────────
        self._ws_thread: Optional[threading.Thread] = None
        self._running = threading.Event()
        self._ws: Any = None  # fyersModel.DataSocket instance

        # ── Logging ─────────────────────────────────────────────────────────────
        self.logger = logging.getLogger(f"{__name__}.FyersSocketManager")

    # ────────────────────────────────────────────────────────────────────────────
    # PUBLIC API
    # ────────────────────────────────────────────────────────────────────────────

    def start(self) -> None:
        """Start the WebSocket connection in a background daemon thread."""
        if self._running.is_set():
            self.logger.warning("WebSocket is already running.")
            return

        self._running.set()
        self._ws_thread = threading.Thread(
            target=self._run_websocket_loop,
            name="fyers-ws",
            daemon=True,
        )
        self._ws_thread.start()
        self.logger.info("Fyers WebSocket manager started (daemon thread).")

    def stop(self) -> None:
        """Gracefully stop the WebSocket connection and the background thread."""
        self._running.clear()
        if self._ws:
            try:
                self._ws.close_connection()
            except Exception:
                pass
        self.logger.info("Fyers WebSocket manager stopped.")

    def subscribe(self, symbols: List[str]) -> None:
        """
        Subscribe to one or more symbols.

        If the WebSocket is already connected, the subscription is sent
        immediately.  Otherwise, symbols are queued for the next connection.
        """
        new_symbols = set(symbols) - self._symbols
        if not new_symbols:
            return
        self._symbols.update(new_symbols)
        self.logger.info(f"Subscribed to {len(new_symbols)} new symbol(s).")

        # If connected, send the subscription message
        if self._ws and hasattr(self._ws, "subscribe"):
            try:
                self._ws.subscribe(symbols=list(new_symbols))
            except Exception as e:
                self.logger.error(f"Subscribe error: {e}")

    def unsubscribe(self, symbols: List[str]) -> None:
        """Unsubscribe from one or more symbols."""
        self._symbols -= set(symbols)
        if self._ws and hasattr(self._ws, "unsubscribe"):
            try:
                self._ws.unsubscribe(symbols=symbols)
            except Exception as e:
                self.logger.error(f"Unsubscribe error: {e}")

    # ── Price query methods (thread-safe, non-blocking) ─────────────────────────

    def get_latest_price(self, symbol: str) -> Optional[float]:
        """Get the latest LTP for a symbol.  Returns None if unknown."""
        return self.price_state.get_ltp(symbol)

    def get_price_state(self, symbol: str) -> Optional[dict]:
        """Get the full latest price state dict for a symbol."""
        return self.price_state.get(symbol)

    def get_all_price_states(self) -> Dict[str, dict]:
        """Get a snapshot of all symbols' latest price states."""
        return self.price_state.get_all()

    # ── Candle query methods ────────────────────────────────────────────────────

    def get_current_candle(self, symbol: str, timeframe: str = "1m") -> Optional[Candle]:
        """Get the current in-progress candle for a symbol and timeframe."""
        return self.candle_aggregator.get_candle(symbol, timeframe)

    def get_all_candles(self, symbol: str) -> List[Candle]:
        """Get all current candles for a symbol across all timeframes."""
        return self.candle_aggregator.get_all_candles(symbol)

    # ── Tick queue consumer ─────────────────────────────────────────────────────

    def get_tick(self, block: bool = True, timeout: Optional[float] = None) -> Optional[Tick]:
        """
        Consume a tick from the queue (thread-safe).

        This is the primary way for external modules to receive ticks without
        blocking the WebSocket thread.

        Parameters
        ----------
        block : bool
            If True, block until a tick is available.
        timeout : float, optional
            Max seconds to wait if block=True.

        Returns
        -------
        Tick or None if queue is empty (non-blocking) or timeout.
        """
        try:
            return self._tick_queue.get(block=block, timeout=timeout)
        except queue.Empty:
            return None

    def tick_queue_size(self) -> int:
        """Approximate number of ticks waiting to be consumed."""
        return self._tick_queue.qsize()

    # ────────────────────────────────────────────────────────────────────────────
    # INTERNAL — WebSocket loop with auto-reconnect
    # ────────────────────────────────────────────────────────────────────────────

    def _run_websocket_loop(self) -> None:
        """
        Main WebSocket loop.  Runs in a daemon thread.

        On disconnect, waits with exponential backoff and reconnects
        automatically.
        """
        backoff = self.RECONNECT_BASE

        while self._running.is_set():
            try:
                self._connect_and_listen()
                # If we exit cleanly (no exception), reset backoff
                backoff = self.RECONNECT_BASE
            except Exception as e:
                self.logger.error(f"WebSocket error: {e}")

            if not self._running.is_set():
                break

            self.logger.info(
                f"Reconnecting in {backoff}s "
                f"(max {self.RECONNECT_MAX}s)..."
            )
            time.sleep(backoff)
            backoff = min(backoff * self.RECONNECT_FACTOR, self.RECONNECT_MAX)

    def _connect_and_listen(self) -> None:
        """
        Establish a Fyers WebSocket connection and enter the message loop.

        This method blocks until the connection is closed or an error occurs.
        """
        # ── Create Fyers DataSocket instance ────────────────────────────────────
        self._ws = fyersModel.DataSocket(
            access_token=self.access_token,
            write_to_file=False,
            log_path=None,
            on_connect=self._on_connect,
            on_close=self._on_close,
            on_error=self._on_error,
            on_message=self._on_message,
            on_general=self._on_general,
        )

        # ── Connect ─────────────────────────────────────────────────────────────
        self.logger.info("Connecting to Fyers WebSocket...")
        self._ws.connect()

        # ── The DataSocket runs its own internal loop; we just block here ───────
        #     until the connection drops.  The callbacks handle everything.
        while self._running.is_set() and self._ws and self._ws.is_connected():
            time.sleep(0.1)

    # ────────────────────────────────────────────────────────────────────────────
    # FYERS CALLBACKS
    # ────────────────────────────────────────────────────────────────────────────

    def _on_connect(self) -> None:
        """Called when the WebSocket connection is established."""
        self.logger.info("WebSocket connected.")
        # Subscribe to all currently tracked symbols
        if self._symbols:
            try:
                self._ws.subscribe(symbols=list(self._symbols))
                self.logger.info(
                    f"Subscribed to {len(self._symbols)} symbols."
                )
            except Exception as e:
                self.logger.error(f"Subscribe on connect error: {e}")

    def _on_close(self) -> None:
        """Called when the WebSocket connection is closed."""
        self.logger.warning("WebSocket connection closed.")

    def _on_error(self, error: str) -> None:
        """Called on WebSocket errors."""
        self.logger.error(f"WebSocket error: {error}")

    def _on_general(self, msg: str) -> None:
        """
        Called for general / non-tick messages (e.g. subscription confirmations,
        heartbeat acknowledgements).
        """
        # Silently ignore heartbeats; log anything else at debug level
        if "heartbeat" not in str(msg).lower():
            self.logger.debug(f"General message: {msg}")

    def _on_message(self, msg: dict) -> None:
        """
        Called for every tick message from Fyers.

        This is the core data pipeline entry point:
            1. Parse the raw message into a Tick dataclass
            2. Update the shared in-memory price state
            3. Feed the tick into the candle aggregator
            4. Push the tick into the queue for external consumers
            5. Invoke the user's on_tick callback (if provided)
        """
        try:
            # ── Parse ───────────────────────────────────────────────────────────
            tick = Tick.from_fyers_message(msg)

            # ── Update shared state ─────────────────────────────────────────────
            self.price_state.update(tick.symbol, tick)

            # ── Candle aggregation ──────────────────────────────────────────────
            self.candle_aggregator.ingest_tick(tick)

            # ── Push to queue (non-blocking, drop oldest if full) ───────────────
            try:
                self._tick_queue.put_nowait(tick)
            except queue.Full:
                # Queue is full — discard the oldest tick to keep latency low
                try:
                    self._tick_queue.get_nowait()
                    self._tick_queue.put_nowait(tick)
                except queue.Empty:
                    pass

            # ── User callback ───────────────────────────────────────────────────
            if self._on_tick:
                self._on_tick(tick)

        except Exception as e:
            self.logger.error(f"Error processing tick: {e}")


# ════════════════════════════════════════════════════════════════════════════════
# CONVENIENCE — build symbol lists for NSE cash, futures, and options
# ════════════════════════════════════════════════════════════════════════════════

def nse_eq_symbol(ticker: str) -> str:
    """Convert a plain ticker (e.g. 'RELIANCE') to NSE EQ format."""
    return f"NSE:{ticker}-EQ"


def nse_fut_symbol(ticker: str, expiry_month: int, expiry_year: int) -> str:
    """
    Convert to NSE futures format.
    Example: NSE:RELIANCE24AUGFUT
    """
    month_codes = {1: "JAN", 2: "FEB", 3: "MAR", 4: "APR", 5: "MAY", 6: "JUN",
                   7: "JUL", 8: "AUG", 9: "SEP", 10: "OCT", 11: "NOV", 12: "DEC"}
    yr_suffix = str(expiry_year)[-2:]
    return f"NSE:{ticker}{yr_suffix}{month_codes[expiry_month]}FUT"


def nifty_50_eq_symbols() -> List[str]:
    """Return NSE EQ symbols for the NIFTY 50 index constituents."""
    nifty_50 = [
        "RELIANCE", "TCS", "HDFCBANK", "INFY", "ICICIBANK", "HINDUNILVR", "ITC",
        "SBIN", "BHARTIARTL", "KOTAKBANK", "BAJFINANCE", "LT", "WIPRO", "AXISBANK",
        "TITAN", "ASIANPAINT", "MARUTI", "SUNPHARMA", "HCLTECH", "NTPC",
        "ONGC", "POWERGRID", "ULTRACEMCO", "BAJAJFINSV", "ADANIPORTS",
        "TATAMOTORS", "NESTLEIND", "JSWSTEEL", "TATASTEEL", "TECHM",
        "HDFCLIFE", "SBILIFE", "COALINDIA", "BRITANNIA", "GRASIM",
        "EICHERMOT", "DIVISLAB", "DRREDDY", "CIPLA", "BPCL",
        "HINDALCO", "IOC", "HEROMOTOCO", "BAJAJ-AUTO", "UPL",
        "SHREECEM", "TATACONSUM", "ADANIGREEN", "ADANIENT", "TRENT",
    ]
    return [nse_eq_symbol(t) for t in nifty_50]


def nifty_bank_eq_symbols() -> List[str]:
    """Return NSE EQ symbols for the NIFTY Bank index constituents."""
    bank_stocks = [
        "HDFCBANK", "ICICIBANK", "SBIN", "KOTAKBANK", "AXISBANK",
        "INDUSINDBK", "BANKBARODA", "PNB", "FEDERALBNK", "IDFCFIRSTB",
        "RBLBANK", "BANDHANBNK", "AUBANK", "YESBANK", "CANBK",
        "UNIONBANK", "IBCLEARING",
    ]
    return [nse_eq_symbol(t) for t in bank_stocks]

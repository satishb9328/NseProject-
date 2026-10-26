import asyncio
import atexit
import contextlib
import csv
import gzip
import io
import json
import logging
import math
import os
import pathlib
import sys
import tempfile
import threading
import time
from collections import deque
from datetime import datetime, date, timedelta, timezone
from pathlib import Path

# Single-instance guard primitives. Exactly one of these exists on any given platform,
# and both give a lock that the OS releases automatically if the process is killed -9,
# which a bare PID file cannot do.
try:  # POSIX
    import fcntl
except ImportError:
    fcntl = None
try:  # Windows
    import msvcrt
except ImportError:
    msvcrt = None

ROOT_DIR = Path(__file__).resolve().parent.parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from Nse_Project.graceful_shutdown import is_shutdown_requested, setup_signal_handlers
# Shared (expiry_date, strike_price) resolution. The dashboard and the Gamma engine use
# the SAME helpers, so what this producer considers "the active expiry" is exactly what
# they consider it to be.
from Nse_Project.expiry_utils import belongs_to_expiry, normalize_expiry, resolve_row_expiry
from Nse_Project.Brokers.cvd_engine import CVDEngine
from Nse_Project.Live_dashboard.ai_candle_confirmation import (
    CandleConfirmationEngine,
    is_no_trade_zone,
    set_market_no_trade_probe,
)
from Nse_Project.Live_dashboard.fib_bounce_engine import (
    FibBounceEngine,
    resolve_gamma_side,
)
from Nse_Project.Live_dashboard.market_state import (
    load_market_state,
    market_state_key,    save_market_state,
)
# Real Black-Scholes Gamma engine, reused read-only so the live score's gamma boost is
# driven by the SAME Net GEX that powers the Day Gamma card (not the old OI-skew proxy).
from Nse_Project.GAMMA import gamma_blast_engine as gamma_engine

import pandas as pd
import redis
import requests
from upstox_client import ApiClient, Configuration
from upstox_client.feeder.market_data_streamer_v3 import MarketDataStreamerV3

# Resolve the absolute path to the repository root (same level as manage.py)
BASE_DIR = pathlib.Path(__file__).resolve().parent.parent.parent
TOKEN_DIR = BASE_DIR / 'Nse_Project' / 'Brokers' / 'upstox'
JSON_PATH = os.path.join(BASE_DIR, 'live_options_data.json')
MASTER_INSTRUMENTS_URL = "https://assets.upstox.com/market-quote/instruments/exchange/NSE.csv.gz"
# Some Upstox asset endpoints reject the default python-requests User-Agent (HTTP 403).
UPSTOX_INSTRUMENTS_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0 Safari/537.36"
    ),
}
# Previous-day OI ("POI") comes from ONE Upstox option-chain REST call per refresh.
# This replaces the old per-instrument historical-candle baseline (774 HTTP calls),
# the on-disk baseline_oi_cache_*.json files, and all previous-trading-day/holiday
# date arithmetic. See fetch_previous_oi_map() for the exact contract.
OPTION_CHAIN_URL = "https://api.upstox.com/v2/option/chain"
REDIS_URL = os.getenv('REDIS_URL', 'redis://127.0.0.1:6379/0')
REDIS_DATA_KEY = "live_options_data"
REDIS_CHANNEL = "gamma:live_options_updated"

# ---------------------------------------------------------------------------
# Redis key namespacing (opt-in) + expiry sanitisation (always on)
# ---------------------------------------------------------------------------
# The primary key stays `live_options_data` because the dashboard and the Gamma engine
# both read it by name. What changed is that the payload written to it is SANITISED:
# snapshot_for_publish() serialises only instruments belonging to the active subscribed
# expiry, so a foreign chain can never reach a consumer even if one leaked into
# live_state (restored snapshot, mid-rollover tick, ...).
#
# Setting REDIS_EXPIRY_NAMESPACE=1 additionally mirrors every publish to
# `live_options_data:{expiry}`, which lets an external tool pin itself to one expiry.
# Default off so the single-SET-per-publish contract of the existing consumers (and of
# TESTING/day_gamma_fixes_test.py::test_redis_publish_throttle) is untouched.
REDIS_EXPIRY_NAMESPACE = os.getenv("REDIS_EXPIRY_NAMESPACE", "0").strip().lower() in {
    "1", "true", "yes", "on",
}
REDIS_DATA_KEY_TEMPLATE = os.getenv("REDIS_DATA_KEY_TEMPLATE", REDIS_DATA_KEY + ":{expiry}")

# ---------------------------------------------------------------------------
# Single-instance guard
# ---------------------------------------------------------------------------
# Two feeds writing the same Redis key is the worst failure mode this process has: each
# one SETs its OWN full snapshot ~1x/second, so consumers see the two chains alternating
# and any instrument that only one of them subscribes to appears and disappears. The lock
# below is an OS-level exclusive lock on a file that also carries our PID, so a crashed
# or -9'd predecessor never blocks a restart.
LOCK_PATH = os.getenv("UPSTOX_FEED_LOCK_PATH", os.path.join(BASE_DIR, "logs", "upstox_live_feed.lock"))
# Windows byte-range locks make the locked bytes unreadable to OTHER handles, so the
# sentinel byte is taken far past the PID text: the lock is still exclusive, but an
# operator (and _read_lock_pid) can always read who owns it while it is held.
LOCK_SENTINEL_OFFSET = 1 << 20
_instance_lock_handle = None

# ---------------------------------------------------------------------------
# Feed staleness
# ---------------------------------------------------------------------------
# A snapshot older than this (measured from the last tick actually received) is stale.
# dump_live_state() refuses to force-publish a stale snapshot, because doing so restamps
# `updated_at` and makes dead data look live to every consumer.
STALE_FEED_SECONDS = float(os.getenv("STALE_FEED_SECONDS", "30"))

# ---------------------------------------------------------------------------
# Previous-OI (POI) baseline persistence
# ---------------------------------------------------------------------------
# prev_oi is a STATIC end-of-previous-day figure, so it is safe (and necessary) to cache
# it across restarts: without it, every restart left previous_oi_map empty until the
# option-chain REST call returned, and every tick that arrived in that window wrote
# coi=None / coi_pct=None into the published snapshot.
POI_SNAPSHOT_DIR = os.getenv("POI_SNAPSHOT_DIR", os.path.join(BASE_DIR, "poi_baseline"))

# Underlying index spot instrument — subscribed separately so the option-chain
# feed never has to be used to infer the spot (which produced stale/wrong prices).
INDEX_INSTRUMENT_KEY = "NSE_INDEX|Nifty 50"

TOKEN_FILE_PATHS = [
    TOKEN_DIR / "upstox_v3_token.json",
]

# Upstox reports OI/COI in contracts (lots); multiply by lot size to match NSE's
# share-based OI convention everywhere downstream (Gamma engine, dashboards).
LOT_SIZE = int(os.getenv("GAMMA_LOT_SIZE", "65"))

# Upstox streams implied volatility as a fraction (0.1928 = 19.28%); the Gamma engine
# expects percent (it divides by 100 and defaults missing IV to 15.0).
IV_FRACTION_TO_PERCENT = 100.0

# Strict NIFTY 50 option tradingsymbol: 'NIFTY' followed immediately by a DIGIT (the
# start of the expiry code) and ending in CE/PE. Deliberately excludes NIFTYNXT50*,
# NIFTYFPI*, MIDCPNIFTY*, BANKNIFTY* and any other sibling index, all of which place a
# letter directly after 'NIFTY' (or prefix it).
NIFTY50_OPTION_SYMBOL_RE = r'^NIFTY\d[0-9A-Z]*(?:CE|PE)$'

# ---------------------------------------------------------------------------
# Previous OI (POI) configuration
# ---------------------------------------------------------------------------
# The Upstox V3 websocket proto (MarketDataFeedV3.proto) exposes only:
#   ltpc, marketLevel, optionGreeks, marketOHLC, atp, vtt, oi, iv, tbq, tsq
# There is NO `poi` and NO `eFeedDetails` message — `eFeedDetails` belonged to the
# deprecated V2 proto. Previous-day OI is therefore sourced from the option-chain
# REST endpoint, which returns `market_data.prev_oi` for every strike in a SINGLE
# request. `extract_live_values()` still sniffs the tick for a `poi` key first, so
# if Upstox ever ships previous OI on the wire it is preferred automatically with
# zero code changes.
POI_REFRESH_SECONDS = int(os.getenv("POI_REFRESH_SECONDS", "900"))  # 15 min
POI_FETCH_TIMEOUT = 15      # seconds for the single option-chain request
POI_FETCH_RETRIES = 3       # attempts before giving up on a refresh cycle
POI_FETCH_BACKOFF = 1.5     # seconds (multiplied by attempt number)

# ---------------------------------------------------------------------------
# Volume Velocity configuration (shares/sec)
# ---------------------------------------------------------------------------
# Rolling window over the cumulative volume-traded-today (`vtt`) of each strike.
# velocity = (vtt_now - vtt_oldest_in_window) / (t_now - t_oldest_in_window)
VELOCITY_WINDOW_SECONDS = float(os.getenv("VELOCITY_WINDOW_SECONDS", "3.0"))
# Ignore sub-sampled deltas: below this dt the divisor is too small to be stable.
VELOCITY_MIN_ELAPSED_SECONDS = 0.25

# ---------------------------------------------------------------------------
# Redis publish throttle
# ---------------------------------------------------------------------------
# Redis is now the ONLY live IPC path: write_live_state() no longer touches the disk on
# the tick path (see atomic_write_json / dump_live_state below), so this throttle is the
# single rate limiter protecting both the event loop and the Gamma engine, which
# recomputes Black-Scholes greeks for the whole chain on every notification.
#
# Kept at 1.0s because Nse_Project/TESTING/day_gamma_fixes_test.py pins that contract
# (test_redis_publish_throttle asserts the default and calibrates its fake clock to it).
# Removing the per-tick 28ms blocking file write freed ~91% of a CPU core, so there is
# now ample headroom to tighten this WITHOUT touching code — export
# REDIS_PUBLISH_MIN_INTERVAL=0.25 for a snappier dashboard (a 250ms floor keeps every
# 1Hz poll fresher than its own period). Change the default only together with that test.
REDIS_PUBLISH_MIN_INTERVAL = float(os.getenv("REDIS_PUBLISH_MIN_INTERVAL", "1.0"))
_last_redis_publish_ts = 0.0

LOG_FORMAT = "%(asctime)s | %(levelname)-8s | %(message)s"
# File + console logging. Previously the feed logged only to the console (stderr),
# so every crash / backfill / evaluation detail was lost the moment the terminal
# closed. Persist to logs/upstox_feed.log as well so a late-start diagnosis (or a
# silent exception) can be traced after the fact.
_LOG_DIR = BASE_DIR / "logs"
_LOG_DIR.mkdir(parents=True, exist_ok=True)
_UPSTOX_FEED_LOG_PATH = _LOG_DIR / "upstox_feed.log"
logging.basicConfig(
    level=logging.INFO,
    format=LOG_FORMAT,
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(_UPSTOX_FEED_LOG_PATH, encoding="utf-8"),
    ],
)
logger = logging.getLogger("LiveUpstoxFeed")
redis_client = redis.Redis.from_url(
    REDIS_URL,
    decode_responses=True,
    socket_connect_timeout=1,
    socket_timeout=1,
)

# ---------------------------------------------------------------------------
# Live market-metrics persistence (CVD + peak Bias / Momentum / Score)
# ---------------------------------------------------------------------------
# The four dashboard metrics below are cumulative / peak-hold values that used to
# live only in memory. They are now mirrored to Redis under a date-scoped key
# (``market_state:YYYY-MM-DD``) so they survive a worker restart and can be served
# by views.py on the initial page render. See Live_dashboard/market_state.py.
market_state = {
    "cumulative_cvd": 0.0,
    "peak_bias": 0.0,
    "peak_momentum": 0.0,
    "peak_score": 50.0,
    # Authoritative current sentiment — the single source of truth the dashboard renders.
    "bias": 0.0,
    "momentum": 0.0,
    "score": 50.0,
    "delta_wt_impact": 0.0,
    "gamma_boost_val": 0.0,
    "iv_skew_est": 0.0,
    "acceleration": 0.0,
    # Real Black-Scholes Net GEX + its bounded conviction ratio (net_gex / gross_gex).
    "net_gex": 0.0,
    "net_gex_ratio": 0.0,
    # Advanced Greek Blend inputs (Vanna / Charm / Gamma Flip level) driving the score.
    "net_vanna": 0.0,
    "net_charm": 0.0,
    "gamma_flip_level": 0.0,
    # Whole-chain macro metrics + session peaks (Net OI Bias / Total Vol Skew / Total PCR).
    "net_oi_bias": 0.0,
    "total_vol_skew": 0.0,
    "total_pcr": 1.0,
    "peak_net_oi_bias": 0.0,
    "peak_total_vol_skew": 0.0,
    "peak_total_pcr": 1.0,
}
# Peak-hold extremes needed to derive the single "current-side" peak the dashboard
# shows (mirrors the frontend's peakMax/peakMin logic). Score is centred on 50.
_sentiment_extremes = {
    "bias_max": 0.0,
    "bias_min": 0.0,
    "mom_max": 0.0,
    "mom_min": 0.0,
    "score_max": 50.0,
    "score_min": 50.0,
}
# Monotonic timestamp of the last score-peak update. Drives the 5-minute half-life
# decay so the session High-Water Mark retreats toward the live score instead of
# freezing at a stale extreme.
_score_peak_updated_at = None
# Latest cumulative CVD, fed by CVDEngine's on_candle callback (a separate worker
# thread) and folded into the Redis market state on the next publish beat.
_latest_cumulative_cvd = 0.0
# Advanced-quant sentiment state. ``_previous_directional_coi`` drives the momentum
# ACCELERATION (rate of change of the DIRECTIONAL COI signal, call_coi - put_coi, vs
# the last tick). The advanced diagnostics are persisted to the Redis
# ``market_state`` by _refresh_market_state so views.py can serve them at the 1 Hz
# option-chain cadence.
_previous_directional_coi = None
# Advanced-quant tuning (score points on the 0-100 scale).
# The gamma boost is now an ADVANCED GREEK BLEND + GAMMA FLIP MULTIPLIER. The real
# Black-Scholes Net GEX, Vanna and Charm are each normalized to a [-1, 1] ratio (net /
# gross, like net_gex_ratio), then weighted into a composite base boost. When price sits
# inside the gamma flip zone (|spot - flip| / spot < _FLIP_ZONE_PCT), the whole blend is
# scaled by _FLIP_ZONE_MULTIPLIER to reflect the explosive potential of the zone.
_GEX_BLEND_WEIGHT = 10.0       # max score points from the Net GEX ratio ([-1, 1])
_VANNA_BLEND_WEIGHT = 5.0      # max score points from the normalized Vanna ratio ([-1, 1])
_CHARM_BLEND_WEIGHT = 5.0      # max score points from the normalized Charm ratio ([-1, 1])
_GAMMA_BOOST_CAP = 25.0        # clamp the final gamma boost to ±25 score points
_FLIP_ZONE_PCT = 0.2           # |spot - flip| / spot < 0.2% => explosive flip zone
_FLIP_ZONE_MULTIPLIER = 1.5    # multiplier applied to the blend inside the flip zone
_ACCEL_GAIN = 0.5              # scale COI acceleration into momentum points
_ACCEL_CAP = 15.0              # clamp the acceleration boost to ±15 momentum points
# Score High-Water Mark decay: the session peak loses half its distance-to-neutral
# every SCORE_PEAK_HALF_LIFE_SECONDS, using decayFactor = exp(-0.693 * t / half_life).
SCORE_PEAK_HALF_LIFE_SECONDS = 300.0   # 5 minutes
SCORE_PEAK_DECAY_RATE = 0.693          # ln(2)

state_lock = asyncio.Lock()
live_state = {
    "updated_at": None,
    "expiry_date": None,
    # Canonical live index spot, sourced from the dedicated index tick (never from
    # option greeks / underlying-price fields, which lag).
    "spot_price": None,
    "underlying_ltp": None,
    "underlying_spot": None,
    "index_ltp": None,
    "nifty_ltp": None,
    "spot_source": None,
    "spot_updated_at": None,
    "poi_source": None,
    "poi_updated_at": None,
    "poi_instruments": 0,
    # Set when the chain was seeded from the previous on-disk snapshot at startup
    # (so an out-of-hours dashboard is explainable rather than mysteriously stale).
    "restored_from_disk": False,
    "restored_instruments": 0,
    "restored_snapshot_updated_at": None,
    # Websocket liveness, published with every snapshot so a consumer can tell a frozen
    # market apart from a frozen feed instead of trusting `updated_at` blindly.
    "feed_connected": False,
    "feed_status": "starting",
    "last_tick_at": None,
    "seconds_since_last_tick": None,
    "stale": True,
    "instrument_data": {},
    # Directional Fibonacci Bounce: anchor + directional Fib levels + last touch,
    # folded in on every index tick by update_index_spot().
    "fib_bounce": {},
}
# Websocket health, tracked on the monotonic clock so a system-clock change cannot make
# a stale feed look fresh.
feed_health = {
    "connected": False,
    "last_tick_monotonic": None,
    "last_tick_at": None,
    "connected_at": None,
    "disconnected_at": None,
    "stale_publish_skips": 0,
}
# instrument_key -> previous-day OI, already scaled to the internal share convention.
previous_oi_map: dict[str, float] = {}
# instrument_key -> deque[(monotonic_ts, cumulative_volume)] for the rolling window.
volume_history: dict[str, deque] = {}
INSTRUMENT_METADATA: dict[str, dict] = {}


# ---------------------------------------------------------------------------
# Single-instance process guard
# ---------------------------------------------------------------------------
def _lock_file_exclusive(handle) -> bool:
    """Take a non-blocking exclusive OS lock on ``handle``. False when already held."""
    try:
        if msvcrt is not None:
            # Lock ONE byte at the sentinel offset (msvcrt.locking works from the current
            # file position, and Windows allows locking past EOF), so the PID text at
            # offset 0 stays readable by other handles.
            handle.seek(LOCK_SENTINEL_OFFSET)
            try:
                msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
            finally:
                handle.seek(0)
            return True
        if fcntl is not None:
            # flock is advisory and whole-file: it never blocks a reader.
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            return True
    except OSError:
        return False
    # Neither primitive available (should not happen on CPython): fall back to the PID
    # annotation alone rather than refusing to start.
    return True


def _read_lock_pid(path: str) -> str:
    try:
        with open(path, "r", encoding="utf-8") as fh:
            return (fh.read() or "").strip() or "unknown"
    except OSError:
        return "unknown"


def acquire_single_instance_lock(path: str = LOCK_PATH) -> bool:
    """Ensure only ONE upstox_live_feed.py process runs at a time.

    Two instances are indistinguishable from the outside: both SET the same Redis key with
    their own full snapshot roughly once a second, so consumers see two chains interleaved
    and rows flicker between them.

    The guard is an exclusive OS lock (``msvcrt.locking`` on Windows, ``flock`` on POSIX)
    on a file that also stores the owning PID. Because the lock lives in the kernel it is
    dropped automatically when the owner dies - even on SIGKILL or a power cut - so a stale
    lock file can never block a legitimate restart. The PID is written purely so an
    operator can see WHO holds it.

    Returns True when the lock is ours, False when another instance already owns it.
    """
    global _instance_lock_handle

    if _instance_lock_handle is not None:
        return True

    directory = os.path.dirname(path) or "."
    try:
        os.makedirs(directory, exist_ok=True)
        # r+ so an existing file is NOT truncated before we know we own the lock: the
        # holder's PID has to survive a failed acquisition attempt.
        mode = "r+" if os.path.exists(path) else "w+"
        handle = open(path, mode, encoding="utf-8")
    except OSError as exc:
        logger.error("❌ Cannot open the single-instance lock file %s: %s", path, exc)
        return False

    if not _lock_file_exclusive(handle):
        holder = _read_lock_pid(path)
        handle.close()
        logger.error(
            "🚫 Another upstox_live_feed.py instance is already running (lock %s held by PID %s). "
            "Refusing to start: two feeds publishing to '%s' interleave their snapshots and "
            "corrupt every consumer.",
            path, holder, REDIS_DATA_KEY,
        )
        return False

    try:
        handle.seek(0)
        handle.truncate()
        handle.write(f"{os.getpid()}\n")
        handle.flush()
        os.fsync(handle.fileno())
    except OSError as exc:
        # Losing the PID annotation is cosmetic; the kernel lock is what enforces the rule.
        logger.warning("Could not record the PID in the lock file %s: %s", path, exc)

    _instance_lock_handle = handle
    atexit.register(release_single_instance_lock)
    logger.info("🔒 Single-instance lock acquired (%s, PID %s).", path, os.getpid())
    return True


def release_single_instance_lock() -> None:
    """Drop the single-instance lock. Safe to call twice and on a never-locked process."""
    global _instance_lock_handle

    handle = _instance_lock_handle
    if handle is None:
        return
    _instance_lock_handle = None
    try:
        if msvcrt is not None:
            handle.seek(LOCK_SENTINEL_OFFSET)
            with contextlib.suppress(OSError):
                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
        elif fcntl is not None:
            with contextlib.suppress(OSError):
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
    finally:
        with contextlib.suppress(OSError):
            handle.close()
    logger.info("🔓 Single-instance lock released.")


# ---------------------------------------------------------------------------
# Websocket liveness
# ---------------------------------------------------------------------------
def mark_socket_connected() -> None:
    feed_health["connected"] = True
    feed_health["connected_at"] = datetime.now(IST).isoformat()
    live_state["feed_connected"] = True
    live_state["feed_status"] = "connected"


def mark_socket_disconnected(reason: str = "closed") -> None:
    feed_health["connected"] = False
    feed_health["disconnected_at"] = datetime.now(IST).isoformat()
    live_state["feed_connected"] = False
    live_state["feed_status"] = f"disconnected:{reason}"


def mark_tick_received() -> None:
    """Record that a real tick arrived. This is the ONLY freshness source of truth."""
    feed_health["last_tick_monotonic"] = time.monotonic()
    feed_health["last_tick_at"] = datetime.now(IST).isoformat()
    if not feed_health["connected"]:
        # Ticks are flowing, so the socket is up whatever the last event said.
        mark_socket_connected()


def seconds_since_last_tick() -> float | None:
    last = feed_health.get("last_tick_monotonic")
    if last is None:
        return None
    return max(0.0, time.monotonic() - float(last))


def is_feed_live() -> bool:
    """True only when the socket is up AND a tick arrived within STALE_FEED_SECONDS."""
    if not feed_health.get("connected"):
        return False
    age = seconds_since_last_tick()
    return age is not None and age <= STALE_FEED_SECONDS


def refresh_liveness_fields() -> tuple[bool, float | None]:
    """Stamp the liveness fields onto ``live_state``; return ``(is_live, tick_age)``."""
    age = seconds_since_last_tick()
    live = is_feed_live()
    live_state["feed_connected"] = bool(feed_health.get("connected"))
    live_state["last_tick_at"] = feed_health.get("last_tick_at")
    live_state["seconds_since_last_tick"] = None if age is None else round(age, 2)
    live_state["stale"] = not live
    if live:
        live_state["feed_status"] = "live"
    elif feed_health.get("connected"):
        live_state["feed_status"] = "stale_no_ticks"
    elif feed_health.get("last_tick_monotonic") is None and not str(
        live_state.get("feed_status") or ""
    ).startswith("disconnected"):
        live_state["feed_status"] = "starting"
    return live, age


def read_access_token() -> str | None:
    for path in TOKEN_FILE_PATHS:
        try:
            if path.exists():
                with path.open("r", encoding="utf-8") as token_file:
                    payload = json.load(token_file)
                    token = payload.get("access_token")
                    if token:
                        logger.info(f"✅ Loaded access token from {path}")
                        return token
                    logger.warning(f"Token file exists but no access_token field: {path}")
        except Exception as exc:
            logger.warning(f"Failed to read token file {path}: {exc}")
    logger.error("No Upstox token JSON file found in Brokers/upstox.")
    return None


def parse_instruments_csv(compressed_bytes: bytes) -> list[dict]:
    with gzip.open(io.BytesIO(compressed_bytes), mode="rt", encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile)
        return [row for row in reader]


def find_nifty_weekly_option_keys() -> tuple[date, list[str]]:
    try:
        logger.info("📥 Downloading Upstox NSE instrument master file...")
        response = requests.get(MASTER_INSTRUMENTS_URL, timeout=20)
        response.raise_for_status()
        content = gzip.decompress(response.content)
        df = pd.read_csv(io.BytesIO(content))
        logger.info("✅ Upstox instrument master file downloaded successfully.")
        logger.info("🔍 Master file columns: %s", list(df.columns[:20]))
        logger.info("📄 Master file preview:\n%s", df.head(5).to_dict(orient='records'))
    except Exception as exc:
        logger.error(f"❌ Unable to load Upstox instrument master data: {exc}")
        return None, []

    # Keep original column names for robust mapping
    columns_lower = {col.lower().strip(): col for col in df.columns}
    symbol_col = columns_lower.get('tradingsymbol') or next((col for key, col in columns_lower.items() if 'tradingsymbol' in key or key == 'symbol'), None)
    type_col = columns_lower.get('instrument_type') or next((col for key, col in columns_lower.items() if 'instrument_type' in key or key == 'type'), None)
    expiry_col = columns_lower.get('expiry') or next((col for key, col in columns_lower.items() if 'expiry' in key), None)
    strike_col = columns_lower.get('strike') or next((col for key, col in columns_lower.items() if 'strike' in key), None)
    option_type_col = columns_lower.get('option_type') or next((col for key, col in columns_lower.items() if 'option_type' in key or key == 'optiontype'), None)
    key_col = columns_lower.get('instrument_key') or next((col for key, col in columns_lower.items() if 'instrument_key' in key or 'instrumentkey' in key), None)
    # The master file also carries a clean underlying name ('NIFTY', 'NIFTYNXT50',
    # 'MIDCPNIFTY', ...) which is the most reliable discriminator available.
    name_col = columns_lower.get('name')

    if not symbol_col or not type_col or not expiry_col or not strike_col or not key_col:
        logger.error("❌ Upstox instrument master file is missing expected columns. Available columns: %s", list(df.columns[:20]))
        return None, []

    # ---------------------------------------------------------------------
    # STEP 1 - STRICT identity filter, on RAW STRING COLUMNS ONLY.
    # ---------------------------------------------------------------------
    # The master file is the whole exchange: ~100k rows of equities, ETFs, SGB/bonds,
    # futures, stock options and every sibling index. Equities carry NO expiry, NO strike
    # and NO option_type, so those cells are NaN. Nothing below this point may touch a
    # date, a strike or the expiry helpers until the frame has been reduced to genuine
    # NIFTY 50 index options - both because parsing ~100k dates to discard 99% of them is
    # pure waste, and because NaN/NaT rows must never reach the parsing code at all.
    df[symbol_col] = df[symbol_col].astype(str).str.strip()
    df[type_col] = df[type_col].astype(str)

    # (a) instrument_type == OPTIDX  -> drops equities, ETFs, bonds, futures, stock options.
    is_optidx = df[type_col].str.upper().str.strip().eq('OPTIDX')
    # (b) name == NIFTY -> drops MIDCPNIFTY, BANKNIFTY, FINNIFTY, NIFTYNXT50, NIFTYFPI...
    if name_col:
        underlying = df[name_col].astype(str).str.strip().str.upper()
        is_nifty_name = underlying.eq('NIFTY')
    else:
        # No `name` column in this master file revision: the symbol regex is the only
        # discriminator available, so require it on its own.
        is_nifty_name = pd.Series(True, index=df.index)
    # (c) A genuine NIFTY 50 option symbol is 'NIFTY' followed IMMEDIATELY by the expiry
    # code, which always begins with a digit (NIFTY26AUG24300CE, NIFTY2590224300CE).
    # Every sibling index puts a LETTER right after 'NIFTY', so anchoring on that digit
    # is both strict and independent of the weekly/monthly symbol format. Without this,
    # `startswith('NIFTY')` admitted 142 NIFTYNXT50 + 44 NIFTYFPI legs out of 277.
    symbol_upper = df[symbol_col].str.upper().fillna('')
    is_nifty_symbol = symbol_upper.str.match(NIFTY50_OPTION_SYMBOL_RE, na=False)

    is_nifty50 = is_optidx & is_nifty_name & is_nifty_symbol

    rejected = df[is_optidx & ~is_nifty50]
    if not rejected.empty:
        sample = rejected[symbol_col].dropna().unique()[:5].tolist()
        logger.info(
            "🚫 Excluded %d non-NIFTY-50 index option rows (e.g. %s).",
            len(rejected), sample,
        )
    logger.info(
        "🔎 Master file reduced from %d rows to %d strict NIFTY 50 OPTIDX rows before any "
        "expiry/strike parsing.",
        len(df), int(is_nifty50.sum()),
    )

    df = df[is_nifty50].copy()
    if df.empty:
        logger.error("❌ No strict NIFTY 50 OPTIDX records found in the instrument master file.")
        return None, []

    # ---------------------------------------------------------------------
    # STEP 2 - Only now parse expiry/strike, and DROP anything unparseable.
    # ---------------------------------------------------------------------
    # errors='coerce' turns a bad/blank expiry into NaT and a bad strike into NaN; both are
    # then dropped explicitly instead of being carried into INSTRUMENT_METADATA (a NaT
    # expiry propagates into every downstream (expiry, strike) row key, and a NaN strike
    # can never be matched to an option-chain row).
    df[expiry_col] = pd.to_datetime(df[expiry_col], errors='coerce')
    df[strike_col] = pd.to_numeric(df[strike_col], errors='coerce')
    before = len(df)
    df = df.dropna(subset=[expiry_col, strike_col, key_col])
    dropped = before - len(df)
    if dropped:
        logger.warning(
            "🧹 Dropped %d NIFTY OPTIDX row(s) with a missing/unparseable expiry, strike or "
            "instrument_key.", dropped,
        )
    if df.empty:
        logger.error("❌ Every NIFTY OPTIDX row had an unusable expiry/strike/instrument_key.")
        return None, []

    df['expiry'] = df[expiry_col].dt.date
    earliest = earliest_tradable_expiry()
    candidates = sorted({value for value in df['expiry'].tolist() if value is not None})
    df = df[df['expiry'] >= earliest]
    if df.empty:
        logger.error("❌ No NIFTY OPTIDX expiry on/after %s. Candidate expiries: %s",
                     earliest, candidates)
        return None, []

    expiry_date = df['expiry'].min()
    filtered = df[df['expiry'] == expiry_date]
        # BUG #2 FIX: Build the new metadata in a LOCAL dict and swap atomically.
    # The original .clear() + repopulate created a race window: this function
    # runs via run_in_executor (thread pool) while the event loop reads
    # INSTRUMENT_METADATA on every tick (update_instrument_state).
    # .clear() momentarily emptied the dict for any concurrent reader, so a
    # tick arriving mid-rebuild saw meta={} → strike=None, option_type=None.
    # An atomic pointer swap closes the window: CPython's GIL makes the
    # STORE_GLOBAL bytecode (INSTRUMENT_METADATA = new_metadata) atomic.
    new_metadata: dict[str, dict] = {}

    for _, row in filtered.iterrows():
        key = str(row[key_col])
        strike = row.get(strike_col)
        expiry_value = row.get('expiry')
        # Belt and braces after the dropna above: a NaN strike or NaT expiry must never
        # reach INSTRUMENT_METADATA, because both are copied onto every published
        # instrument entry and would then poison the (expiry, strike) row keys.
        if pd.isna(strike) or expiry_value is None or pd.isna(expiry_value):
            continue
        option_side = None
        raw_side = row.get(option_type_col) if option_type_col else None
        if raw_side is not None and not pd.isna(raw_side):
            option_side = str(raw_side).strip().upper()
        if option_side not in {"CE", "PE"}:
            # Fall back to the symbol suffix (the regex already guaranteed CE/PE) rather
            # than storing the literal string "NAN" as an option type.
            symbol_text = str(row[symbol_col]).strip().upper()
            option_side = symbol_text[-2:] if symbol_text[-2:] in {"CE", "PE"} else None
        new_metadata[key] = {
            'strike': int(strike) if float(strike).is_integer() else float(strike),
            'option_type': option_side,
            'tradingsymbol': str(row[symbol_col]),
            'expiry': expiry_value,
        }

    # Atomic swap: the old dict is replaced in one GIL-protected STORE_GLOBAL
    # instruction. Any concurrent reader sees either the OLD or the NEW dict,
    # never an empty one.
    global INSTRUMENT_METADATA
    INSTRUMENT_METADATA = new_metadata

    instrument_keys = [k for k in filtered[key_col].dropna().astype(str).unique().tolist() if k in new_metadata]
    instrument_keys.sort(key=lambda key: new_metadata[key]['strike'])
    
    logger.info(f"✅ Found {len(instrument_keys)} NIFTY option keys for expiry {expiry_date}")
    return expiry_date, instrument_keys


# ---------------------------------------------------------------------------
# Cold-backup writer hardening (Windows)
# ---------------------------------------------------------------------------
# os.replace() maps to MoveFileExW(MOVEFILE_REPLACE_EXISTING), which must unlink the
# destination. A reader holding the destination open with a plain open() grants
# FILE_SHARE_READ|WRITE but NOT FILE_SHARE_DELETE, so the rename fails with
# ERROR_ACCESS_DENIED (5); a scanner sitting on the source yields
# ERROR_SHARING_VIOLATION (32). Both clear within milliseconds, so a short bounded
# retry turns a permanently dropped snapshot into a slightly delayed one. This only
# guards the 5-second cold backup now — the live path is Redis and never renames.
ATOMIC_WRITE_RETRIES = int(os.getenv("ATOMIC_WRITE_RETRIES", "5"))
ATOMIC_WRITE_RETRY_DELAY = float(os.getenv("ATOMIC_WRITE_RETRY_DELAY", "0.05"))
WINDOWS_REPLACE_WINERRORS = (5, 32)


def atomic_write_json(path: str, data: dict) -> None:
    """Atomically serialize ``data`` to ``path``.

    Called only by dump_live_state() (once every 5s) — never on the tick path.

    Two hardenings over a naive tmp+replace:

    * The temp file gets a UNIQUE name via ``tempfile.mkstemp`` in the destination
      directory. The old fixed ``<path>.tmp`` was a predictable target for AV/indexers
      and, worse, every failed replace left one stale turd behind holding the newest
      data while the real file kept older content.
    * ``os.replace`` is retried on the two transient Windows sharing errors instead of
      giving up on the first collision.
    """
    directory = os.path.dirname(path) or "."
    os.makedirs(directory, exist_ok=True)

    # Same directory as the destination: os.replace is only atomic within one volume.
    fd, tmp_path = tempfile.mkstemp(prefix=f".{os.path.basename(path)}.", suffix=".tmp", dir=directory)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(data, fh, separators=(",", ":"), default=str)
            fh.flush()
            os.fsync(fh.fileno())

        last_error: OSError | None = None
        for attempt in range(1, ATOMIC_WRITE_RETRIES + 1):
            try:
                os.replace(tmp_path, path)
                if attempt > 1:
                    logger.info(
                        "Cold backup written after %d attempt(s) (transient file lock).", attempt
                    )
                return
            except PermissionError as exc:
                if getattr(exc, "winerror", None) not in WINDOWS_REPLACE_WINERRORS:
                    raise
                last_error = exc
                if attempt < ATOMIC_WRITE_RETRIES:
                    time.sleep(ATOMIC_WRITE_RETRY_DELAY * attempt)
        if last_error is not None:
            raise last_error
    finally:
        # Never leave a stray temp file behind; a successful replace already consumed it.
        if os.path.exists(tmp_path):
            with contextlib.suppress(OSError):
                os.remove(tmp_path)


# Indian market session close, used to decide when an expiry stops being tradable.
IST = timezone(timedelta(hours=5, minutes=30))
MARKET_CLOSE_HOUR = 15
MARKET_CLOSE_MINUTE = 30
# How often the running feed re-checks whether its expiry has rolled over.
EXPIRY_ROLLOVER_CHECK_SECONDS = float(os.getenv("EXPIRY_ROLLOVER_CHECK_SECONDS", "60"))


def market_close_at(moment: datetime) -> datetime:
    """Return the 15:30 IST close on the same calendar day as ``moment``."""
    return moment.replace(
        hour=MARKET_CLOSE_HOUR, minute=MARKET_CLOSE_MINUTE, second=0, microsecond=0
    )


def earliest_tradable_expiry(now_ist: datetime | None = None) -> date:
    """Return the earliest expiry date that is still tradable right now.

    Contracts stop trading at 15:30 IST on their expiry day. Selecting expiries with
    ``expiry >= today`` kept the feed bound to a dead chain from 15:30 until midnight:
    every greek collapses (time-to-expiry is past) and no ticks arrive, so the whole
    dashboard silently flatlines for 8.5 hours. Once the closing bell passes we
    therefore require ``expiry >= tomorrow``, which rolls the chain forward
    immediately instead of waiting for the date to change.
    """
    now_ist = now_ist or datetime.now(IST)
    if now_ist > market_close_at(now_ist):
        return now_ist.date() + timedelta(days=1)
    return now_ist.date()


# How often (seconds) to re-resolve the nearest NIFTY future while the feed runs, so
# the CVD engine rolls over to the next monthly contract without a process restart.
FUTURE_ROLLOVER_CHECK_SECONDS = 3600


def find_nearest_nifty_future_key():
    """Return the nearest (current-month) NIFTY index future ``(instrument_key, expiry)``.

    Downloads the Upstox NSE instrument master, keeps ``instrument_type == 'FUTIDX'``
    rows whose ``name`` is 'NIFTY', then returns the ``instrument_key`` of the contract
    with the earliest expiry on/after ``earliest_tradable_expiry()``.  Returns
    ``(None, None)`` on any failure so the caller can fall back to a manual list.
    """
    try:
        logger.info("📥 Fetching Upstox instrument master to resolve the nearest NIFTY future...")
        response = requests.get(
            MASTER_INSTRUMENTS_URL, headers=UPSTOX_INSTRUMENTS_HEADERS, timeout=30
        )
        response.raise_for_status()
        df = pd.read_csv(io.BytesIO(gzip.decompress(response.content)))
    except Exception as exc:
        logger.error("❌ Unable to load Upstox instrument master for NIFTY future: %s", exc)
        return None, None

    columns_lower = {col.lower().strip(): col for col in df.columns}
    key_col = columns_lower.get("instrument_key")
    name_col = columns_lower.get("name")
    type_col = columns_lower.get("instrument_type")  
    expiry_col = columns_lower.get("expiry")
    symbol_col = columns_lower.get("tradingsymbol")

    if not (key_col and type_col and expiry_col):
        logger.error(
            "❌ Upstox master file missing required columns "
            "(instrument_key/instrument_type/expiry). Available: %s", list(df.columns),
        )
        return None, None

    df = df.copy()
    df[type_col] = df[type_col].astype(str).str.upper().str.strip()
    fut = df[df[type_col].eq("FUTIDX")]

    if name_col:
        fut = fut[fut[name_col].astype(str).str.upper().str.strip().eq("NIFTY")]
    elif symbol_col:
        # Older master-file revisions without `name`: fall back to tradingsymbol prefix.
        fut = fut[fut[symbol_col].astype(str).str.upper().str.startswith("NIFTY")]

    if fut.empty:
        logger.error("❌ No NIFTY FUTIDX rows found in the Upstox instrument master.")
        return None, None

    fut[expiry_col] = pd.to_datetime(fut[expiry_col], errors="coerce")
    fut = fut.dropna(subset=[expiry_col, key_col])
    if fut.empty:
        logger.error("❌ NIFTY futures have no usable expiry/instrument_key.")
        return None, None

    earliest = earliest_tradable_expiry()
    fut = fut[fut[expiry_col] >= pd.Timestamp(earliest)]
    if fut.empty:
        logger.error("❌ No NIFTY future expiry on/after %s.", earliest)
        return None, None

    fut = fut.sort_values(expiry_col)
    nearest_key = str(fut.iloc[0][key_col])
    nearest_expiry = fut.iloc[0][expiry_col].date()

    logger.info(
        "🎯 Auto-selected nearest NIFTY future: %s (expiry %s).",
        nearest_key, nearest_expiry.isoformat(),
    )
    return nearest_key, nearest_expiry


async def _rollover_nifty_future(streamer, active, new_future_key, new_future_expiry, cvd_engine):
    """Swap the subscription + CVD allowlist to a new NIFTY future contract."""
    old_key = active.get("nifty_future_key")
    if old_key and old_key != new_future_key:
        try:
            streamer.unsubscribe([old_key])
            logger.info("🔻 Unsubscribed expired NIFTY future %s.", old_key)
        except Exception as exc:
            logger.warning("Could not unsubscribe expired NIFTY future (%s); continuing.", exc)

    try:
        streamer.subscribe([new_future_key], mode=MarketDataStreamerV3.Mode["FULL"])
    except Exception:
        logger.error("❌ Failed to subscribe new NIFTY future %s.", new_future_key, exc_info=True)
        return

    active["nifty_future_key"] = new_future_key
    active["nifty_future_expiry"] = new_future_expiry
    if cvd_engine is not None:
        cvd_engine.set_symbol_allowlist({new_future_key})
    logger.info(
        "🔁 NIFTY future rolled over to %s (expiry %s).",
        new_future_key, new_future_expiry.isoformat() if new_future_expiry else "?",
    )


# ---------------------------------------------------------------------------
# Previous OI (POI) — single-call REST snapshot
# ---------------------------------------------------------------------------
def _coerce_float(value: object) -> float | None:
    """Return ``value`` as a float, or None when it is missing/unparseable."""
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value.strip())
        except ValueError:
            return None
    return None


def _scale_oi_to_internal(value: float) -> float:
    """Convert an Upstox OI quantity into this module's internal share scale.

    Every OI figure stored in ``live_state`` is ``upstox_oi * LOT_SIZE`` (the
    convention already used for the live websocket ``oi``). POI must use the exact
    same scale, otherwise COI would be wrong by a factor of LOT_SIZE.
    """
    return float(value) * LOT_SIZE


def fetch_previous_oi_map(expiry_date: date) -> dict[str, float]:
    """Return ``{instrument_key: previous_day_oi}`` for the whole option chain.

    A single request to ``/v2/option/chain`` returns every strike for
    ``expiry_date`` with ``market_data.prev_oi`` on both the call and the put leg,
    keyed by the same ``instrument_key`` the websocket feed uses. That one call
    replaces the old per-instrument historical-candle loop entirely.
    """
    token = read_access_token()
    if not token:
        logger.error("Cannot fetch previous OI: no Upstox access token available.")
        return {}

    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
    }
    params = {
        "instrument_key": INDEX_INSTRUMENT_KEY,
        "expiry_date": expiry_date.isoformat(),
    }

    for attempt in range(1, POI_FETCH_RETRIES + 1):
        try:
            response = requests.get(
                OPTION_CHAIN_URL, headers=headers, params=params, timeout=POI_FETCH_TIMEOUT
            )
            if response.status_code == 429 or response.status_code >= 500:
                logger.warning(
                    "Previous-OI fetch attempt %d/%d got HTTP %s; retrying.",
                    attempt, POI_FETCH_RETRIES, response.status_code,
                )
                time.sleep(POI_FETCH_BACKOFF * attempt)
                continue
            if response.status_code != 200:
                logger.error(
                    "Previous-OI fetch failed with HTTP %s: %s",
                    response.status_code, response.text[:300],
                )
                return {}

            payload = response.json()
            rows = payload.get("data") if isinstance(payload, dict) else None
            if not isinstance(rows, list):
                logger.error("Option-chain response had no 'data' list; cannot build a POI map.")
                return {}

            poi_map: dict[str, float] = {}
            missing = 0
            for row in rows:
                if not isinstance(row, dict):
                    continue
                for leg_key in ("call_options", "put_options"):
                    leg = row.get(leg_key)
                    if not isinstance(leg, dict):
                        continue
                    instrument_key = leg.get("instrument_key")
                    if not isinstance(instrument_key, str) or not instrument_key:
                        continue
                    market_data = leg.get("market_data")
                    prev_oi = _coerce_float(
                        market_data.get("prev_oi") if isinstance(market_data, dict) else None
                    )
                    if prev_oi is None or prev_oi < 0:
                        missing += 1
                        continue
                    poi_map[instrument_key] = _scale_oi_to_internal(prev_oi)

            if not poi_map:
                logger.warning("Option-chain call succeeded but yielded 0 usable prev_oi values.")
            else:
                logger.info(
                    "Loaded previous OI for %d instruments in a single option-chain call "
                    "(%d legs carried no prev_oi).",
                    len(poi_map), missing,
                )
            return poi_map
        except requests.RequestException as exc:
            logger.warning(
                "Previous-OI fetch attempt %d/%d failed: %s", attempt, POI_FETCH_RETRIES, exc
            )
            if attempt < POI_FETCH_RETRIES:
                time.sleep(POI_FETCH_BACKOFF * attempt)
        except ValueError as exc:
            logger.error("Option-chain response was not valid JSON: %s", exc)
            return {}

    logger.error("Previous-OI fetch exhausted all %d attempts.", POI_FETCH_RETRIES)
    return {}


def poi_snapshot_path(expiry_date: date) -> str:
    """Return the on-disk path of the persisted POI baseline for one expiry."""
    return os.path.join(POI_SNAPSHOT_DIR, f"poi_baseline_{expiry_date.isoformat()}.json")


def persist_previous_oi(expiry_date: date, poi_map: dict[str, float]) -> None:
    """Persist the POI baseline so a restart can re-hydrate it before the first tick."""
    if not poi_map:
        return
    try:
        atomic_write_json(poi_snapshot_path(expiry_date), {
            "expiry_date": expiry_date.isoformat(),
            "saved_at": datetime.now(IST).isoformat(),
            "source": "upstox_option_chain_prev_oi",
            "lot_size": LOT_SIZE,
            "scale": "internal_shares",  # already multiplied by LOT_SIZE
            "previous_oi": poi_map,
        })
    except Exception as exc:
        # Non-fatal: the in-memory map is authoritative for this session.
        logger.warning("⚠️ Could not persist the POI baseline for %s: %s", expiry_date, exc)


def load_persisted_previous_oi(expiry_date: date) -> dict[str, float]:
    """Return the persisted POI baseline for ``expiry_date``, or {} when unusable.

    The expiry is part of the filename AND re-checked inside the payload, so a baseline
    can never be applied to the wrong chain.
    """
    path = poi_snapshot_path(expiry_date)
    try:
        if not os.path.exists(path):
            return {}
        with open(path, "r", encoding="utf-8") as fh:
            payload = json.load(fh)
    except (OSError, json.JSONDecodeError) as exc:
        logger.warning("Persisted POI baseline %s unusable (%s).", path, exc)
        return {}

    if not isinstance(payload, dict) or payload.get("expiry_date") != expiry_date.isoformat():
        logger.info("Discarding the persisted POI baseline: it does not belong to %s.", expiry_date)
        return {}

    stored = payload.get("previous_oi")
    if not isinstance(stored, dict):
        return {}

    restored = {
        key: float(value) for key, value in stored.items()
        if isinstance(key, str) and isinstance(value, (int, float)) and not isinstance(value, bool)
    }
    if restored:
        live_state["poi_updated_at"] = payload.get("saved_at")
    return restored


def _previous_oi_from_restored_snapshot() -> dict[str, float]:
    """Recover POI from the `poi` already stored on each restored instrument entry.

    Second line of defence for when no baseline file exists: the cold snapshot that
    load_persisted_live_state() just restored still carries the POI that was in force when
    it was written, and POI is a static previous-day figure, so it is still correct.
    """
    recovered: dict[str, float] = {}
    for key, entry in (live_state.get("instrument_data") or {}).items():
        if not isinstance(key, str) or not isinstance(entry, dict):
            continue
        value = entry.get("poi")
        if isinstance(value, (int, float)) and not isinstance(value, bool) and value > 0:
            recovered[key] = float(value)
    return recovered


def hydrate_previous_oi(expiry_date: date) -> int:
    """Populate ``previous_oi_map`` from disk BEFORE the first REST refresh.

    Without this, a restart served ticks with ``coi=None``/``coi_pct=None`` for as long as
    the option-chain call took (and for the whole session if it failed), which the
    dashboard renders as 0 and the Gamma engine treats as "no OI change today".

    Returns how many instruments were re-hydrated.
    """
    if previous_oi_map:
        return len(previous_oi_map)

    restored = load_persisted_previous_oi(expiry_date)
    source = "persisted_baseline"
    if not restored:
        restored = _previous_oi_from_restored_snapshot()
        source = "restored_snapshot_poi"
    if not restored:
        logger.info("No POI baseline to re-hydrate for %s; waiting for the REST snapshot.", expiry_date)
        return 0

    previous_oi_map.update(restored)
    live_state["poi_source"] = source
    live_state["poi_instruments"] = len(previous_oi_map)
    logger.info(
        "♻️ Re-hydrated previous OI for %d instruments from %s so COI survives the restart.",
        len(restored), source,
    )
    return len(restored)


def refresh_previous_oi(expiry_date: date) -> None:
    """Refresh ``previous_oi_map`` in place, keeping the old values on failure.

    ``prev_oi`` is a static end-of-previous-day figure, so a failed refresh is
    harmless: we keep serving the snapshot we already hold. Periodic refreshes only
    matter for strikes that get listed after this process started.

    On success the baseline is also persisted, which is what lets the NEXT restart
    compute COI from the very first tick instead of publishing None.
    """
    fresh = fetch_previous_oi_map(expiry_date)
    if not fresh:
        # Nothing in memory yet (fresh start + failed REST call) is the case that used to
        # null out COI for the whole session; re-hydrate from disk instead.
        hydrated = hydrate_previous_oi(expiry_date)
        logger.warning(
            "Keeping the existing previous-OI snapshot (%d instruments, %d re-hydrated from disk) "
            "after a failed refresh.",
            len(previous_oi_map), hydrated,
        )
        return

    # BUG #3 FIX: In-place mutation instead of pointer swap to eliminate the
    # race condition with the event loop's .clear() operation.
    previous_oi_map.clear()
    previous_oi_map.update(fresh)
    live_state["poi_source"] = "upstox_option_chain_prev_oi"
    live_state["poi_updated_at"] = datetime.now(IST).isoformat()
    live_state["poi_instruments"] = len(fresh)
    persist_previous_oi(expiry_date, fresh)


def get_previous_oi(instrument_key: str) -> float | None:
    """Return the previous-day OI for ``instrument_key`` on the internal share scale."""
    value = previous_oi_map.get(instrument_key)
    return float(value) if isinstance(value, (int, float)) else None


# ---------------------------------------------------------------------------
# Volume Velocity — rolling shares/sec tracker
# ---------------------------------------------------------------------------
def compute_volume_velocity(instrument_key: str, cumulative_volume: float) -> float | None:
    """Return shares/sec for ``instrument_key`` over the rolling window.

    ``cumulative_volume`` is the exchange's volume-traded-today (``vtt``), which is
    monotonically increasing through the session. We retain the ``(timestamp, vtt)``
    samples covering ``VELOCITY_WINDOW_SECONDS`` and divide the volume delta by the
    *actual* elapsed time, so the result is a true rate that never depends on how
    often ticks happen to arrive.

    Returns None until the window holds enough history to be meaningful.
    """
    now = time.monotonic()
    history = volume_history.setdefault(instrument_key, deque(maxlen=600))
    history.append((now, float(cumulative_volume)))

    # Drop samples that aged out of the window, but always keep one sample at/older
    # than the cut-off so ``elapsed`` spans the full window instead of collapsing to
    # the gap between the last two ticks.
    cutoff = now - VELOCITY_WINDOW_SECONDS
    while len(history) > 2 and history[1][0] <= cutoff:
        history.popleft()

    oldest_ts, oldest_volume = history[0]
    elapsed = now - oldest_ts
    if elapsed < VELOCITY_MIN_ELAPSED_SECONDS:
        return None

    delta = float(cumulative_volume) - oldest_volume
    if delta < 0:
        # vtt went backwards (new session or a bad tick): restart the window cleanly.
        history.clear()
        history.append((now, float(cumulative_volume)))
        return None

    return round(delta / elapsed, 2)


def _find_value_by_key(node: object, key: str) -> object:
    """Return the first value whose dict key equals ``key`` (case-insensitive).

    Depth-first search. The V3 feed stores the live price (``ltp``) directly next
    to the close price (``cp``) under a ``ltpc`` wrapper, so a substring scan for
    ``"ltp"`` also matches ``ltpc.cp`` and would latch the spot onto the stale close.
    """
    if isinstance(node, dict):
        for k, value in node.items():
            if str(k).lower() == key.lower():
                return value
        for k, value in node.items():
            found = _find_value_by_key(value, key)
            if found is not None:
                return found
    elif isinstance(node, list):
        for item in node:
            found = _find_value_by_key(item, key)
            if found is not None:
                return found
    return None


def _first_numeric(value: object) -> float | None:
    """Coerce a scalar to float, accepting numeric strings.

    Upstox V3 ships cumulative volume (``vtt``) as a STRING (e.g. ``"3315845"``)
    while OI / IV arrive as floats. The old int/float-only guard silently returned
    None for those string values, so ``volume`` was never extracted and the
    whole-chain ``total_vol_skew`` stayed frozen at 0. Accepting numeric strings
    fixes volume extraction without disturbing the already-float fields.
    """
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return value
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return None
        try:
            return float(stripped)
        except ValueError:
            return None
    return None


def _nested(node: object, path: tuple) -> object:
    """Walk a dict path, returning None the moment it stops matching."""
    current = node
    for key in path:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


def extract_index_spot(payload: object) -> float | None:
    """Return the live index LTP from an index tick, or None.

    Reads the explicit, verified index paths first:
      * ``fullFeed.indexFF.ltpc.ltp``  — full / full_d30 mode
      * ``ltpc.ltp``                   — ltpc mode

    It deliberately NEVER falls back to ``cp`` or to anything under
    ``marketOHLC``. A real index tick looks like this::

        {"fullFeed": {"indexFF": {
            "ltpc": {"ltp": 24334.55, "cp": 24219.05},
            "marketOHLC": {"ohlc": [
                {"interval": "1d", "open": 24175.75, ...},
                {"interval": "I1", "open": 24260.05, ...}]}}}}

    ``ohlc[1].open`` is the *current minute's opening* index level — a
    plausible-looking value that lags the market by up to a minute, and exactly the
    kind of number that silently freezes a dashboard. Only ``ltpc.ltp`` is live.
    """
    if not isinstance(payload, dict):
        return None

    for path in (
        ("fullFeed", "indexFF", "ltpc", "ltp"),
        ("fullFeed", "marketFF", "ltpc", "ltp"),
        ("ltpc", "ltp"),
    ):
        value = _nested(payload, path)
        if isinstance(value, (int, float)) and value > 0:
            return float(value)

    # Last resort: an exact-key scan. Safe because "ltp" exists nowhere else in the
    # V3 schema (marketOHLC uses open/high/low/close), but the explicit paths above
    # are what normally match.
    value = _first_numeric(_find_value_by_key(payload, "ltp"))
    return float(value) if isinstance(value, (int, float)) and value > 0 else None


def extract_live_values(decoded: dict, instrument_key: str | None = None) -> dict:
    """Extract LTP / close / OI / POI / volume from an Upstox V3 protobuf-decoded tick."""
    ltp = _first_numeric(_find_value_by_key(decoded, "ltp"))
    cp = _first_numeric(_find_value_by_key(decoded, "cp"))
    oi = _first_numeric(_find_value_by_key(decoded, "oi"))
    # Defensive previous-OI sniff. The V3 proto does not currently carry previous OI,
    # but if Upstox ever adds a `poi` field (as the deprecated V2 `eFeedDetails.poi`
    # did) it is picked up here and preferred over the REST snapshot automatically.
    # _find_value_by_key matches keys EXACTLY (case-insensitively), so "poi" can
    # never accidentally match "oi".
    poi = _first_numeric(_find_value_by_key(decoded, "poi"))
    # Implied volatility. Upstox V3 marketFF carries `iv` as a FRACTION (0.1928 = 19.28%).
    # It was never extracted before, so every downstream greek ran on the hardcoded
    # 15.0 fallback in the Gamma engine's _repair_iv_surface() - i.e. a synthetic flat
    # 15% vol surface. See update_instrument_state() for the fraction -> percent step.
    iv = _first_numeric(_find_value_by_key(decoded, "iv"))
    volume = _first_numeric(_find_value_by_key(decoded, "vtt"))
    if volume is None:
        volume = _first_numeric(_find_value_by_key(decoded, "volume"))

    return {
        "instrument_key": instrument_key,
        "ltp": ltp,
        "cp": cp,
        "oi": oi,
        "poi": poi,
        "iv": iv,
        "volume": volume,
        "raw": decoded,
    }

def load_persisted_live_state(expiry_date: date) -> int:
    """Seed ``live_state`` from the last on-disk snapshot; return how many were reused.

    Restarting the feed outside market hours used to blank ``instrument_data``, which
    left the dashboard with an empty option chain until the next tick arrived (i.e.
    until the next session). Reusing the previous snapshot keeps the UI testable at
    any hour, and every entry is overwritten by the first live tick for that strike.

    The snapshot is rejected unless it belongs to the SAME expiry we are about to
    subscribe to - otherwise the chain would be repopulated with expired contracts.
    Returns 0 on any problem so the caller can fall back to an empty chain.
    """
    try:
        if not os.path.exists(JSON_PATH):
            logger.info("No previous snapshot at %s; starting with an empty option chain.", JSON_PATH)
            return 0
        with open(JSON_PATH, "r", encoding="utf-8") as fh:
            snapshot = json.load(fh)
    except (OSError, json.JSONDecodeError) as exc:
        logger.warning("Previous snapshot unusable (%s); starting with an empty option chain.", exc)
        return 0
    except Exception as exc:
        logger.warning("Unexpected error reading the previous snapshot (%s); starting empty.", exc)
        return 0

    if not isinstance(snapshot, dict):
        logger.warning("Previous snapshot is not a JSON object; starting with an empty option chain.")
        return 0

    snapshot_expiry = snapshot.get("expiry_date")
    if isinstance(snapshot_expiry, str) and snapshot_expiry != expiry_date.isoformat():
        logger.info(
            "Discarding the previous snapshot: it holds expiry %s but the live expiry is %s.",
            snapshot_expiry, expiry_date.isoformat(),
        )
        return 0

    instrument_data = snapshot.get("instrument_data")
    if not isinstance(instrument_data, dict) or not instrument_data:
        logger.info("Previous snapshot carried no instrument data; starting with an empty option chain.")
        return 0

    restored = {
        key: value for key, value in instrument_data.items()
        if isinstance(key, str) and isinstance(value, dict)
    }
    if not restored:
        return 0

    live_state["instrument_data"] = restored

    # Carry the last known spot forward too, clearly labelled, so the SPOT readout is
    # populated before the first index tick lands. update_index_spot() overwrites all
    # of this (including spot_source) the moment a real tick arrives.
    restored_spot = None
    for key in ("spot_price", "underlying_spot", "underlying_ltp", "index_ltp", "nifty_ltp"):
        candidate = snapshot.get(key)
        if isinstance(candidate, (int, float)) and candidate > 0:
            restored_spot = float(candidate)
            break
    if restored_spot is not None:
        for key in ("spot_price", "underlying_ltp", "underlying_spot", "index_ltp", "nifty_ltp"):
            live_state[key] = restored_spot
        live_state["spot_source"] = "restored_from_disk"
        live_state["spot_updated_at"] = snapshot.get("spot_updated_at")

    # Provenance, so a stale-looking dashboard is explainable rather than mysterious.
    live_state["restored_from_disk"] = True
    live_state["restored_instruments"] = len(restored)
    live_state["restored_snapshot_updated_at"] = snapshot.get("updated_at")
    return len(restored)




def active_expiry_iso() -> str | None:
    """Return the ISO date of the expiry this process is subscribed to, if known."""
    return normalize_expiry(live_state.get("expiry_date"))


def sanitized_instrument_data(active_expiry: str | None = None) -> tuple[dict, dict]:
    """Return ``(instrument_data_for_active_expiry, {foreign_expiry: count})``.

    Strict expiry sanitisation of what leaves this process. ``live_state`` can legitimately
    hold a foreign instrument for a short while (a tick that arrived between unsubscribe
    and the state reset, an entry restored from an older snapshot), and every consumer that
    folds ``instrument_data`` into rows would then have to defend itself. Filtering here,
    once, at the serialisation boundary means only ONE expiry is ever published.

    An instrument is dropped only when its expiry PROVABLY differs from the active one:
    from its own ``expiry`` stamp, from the instrument master metadata, or from the expiry
    code inside its tradingsymbol. Anything unresolvable is kept.
    """
    instrument_data = live_state.get("instrument_data") or {}
    if not active_expiry:
        return dict(instrument_data), {}

    kept: dict = {}
    foreign: dict[str, int] = {}
    for key, entry in instrument_data.items():
        if not isinstance(entry, dict):
            continue
        meta_expiry = normalize_expiry((INSTRUMENT_METADATA.get(key) or {}).get("expiry"))
        candidate = entry
        if meta_expiry and not normalize_expiry(entry.get("expiry")):
            # Judge on the authoritative master-file expiry when the entry has no stamp.
            candidate = {**entry, "expiry": meta_expiry}
        if belongs_to_expiry(candidate, active_expiry):
            kept[key] = entry
        else:
            label = resolve_row_expiry(candidate) or "unknown"
            foreign[label] = foreign.get(label, 0) + 1
    return kept, foreign


def snapshot_for_publish() -> tuple[dict, str | None]:
    """Return ``(payload, active_expiry)`` — the exact dict that goes to Redis/disk."""
    active_expiry = active_expiry_iso()
    instrument_data, foreign = sanitized_instrument_data(active_expiry)
    if foreign:
        logger.warning(
            "🧹 Excluded %d instrument(s) from expiries %s while publishing expiry %s.",
            sum(foreign.values()), sorted(foreign), active_expiry,
        )
    payload = dict(live_state)
    payload["instrument_data"] = instrument_data
    payload["instrument_count"] = len(instrument_data)
    payload["foreign_expiry_instruments"] = sum(foreign.values())
    return payload, active_expiry


_gamma_processor_instance = None


def _gamma_processor() -> gamma_engine.GammaProcessor:
    """Return a shared GammaProcessor for the real-GEX computation."""
    global _gamma_processor_instance
    if _gamma_processor_instance is None:
        _gamma_processor_instance = gamma_engine.GammaProcessor(gamma_engine.CONFIG["symbol"])
    return _gamma_processor_instance


def _real_net_gex(instrument_data=None, spot: float = 0.0) -> dict:
    """Return the real Black-Scholes Greek profile that drives the score's gamma boost.

    Reuses the SAME ``normalize_live_payload`` + ``GammaProcessor.process_payload`` path the
    downstream Gamma bot runs, so every value matches the Day Gamma card exactly.
    ``net_gex_ratio``, ``vanna_ratio`` and ``charm_ratio`` are each ``net / gross`` bounded
    in [-1, 1] — scale-invariant conviction measures safe to fold into the score. The engine
    only exposes a gross GEX, so gross Vanna/Charm are summed here from the same per-leg day
    exposures (``coi * greek``) the engine already aggregates, keeping all three ratios on an
    identical footing.

    Returns a neutral dict on any failure (no spot/expiry, empty chain, engine error) so the
    score's fallback is never disturbed by a leg that cannot be priced.
    """
    neutral = {
        "net_gex": 0.0,
        "net_gex_ratio": 0.0,
        "net_vanna": 0.0,
        "net_charm": 0.0,
        "vanna_ratio": 0.0,
        "charm_ratio": 0.0,
        "gamma_flip_level": None,
        "spot": float(spot) if spot > 0 else 0.0,
    }
    if spot <= 0:
        return neutral
    expiry = active_expiry_iso()
    if not expiry:
        return neutral
    try:
        payload = {
            "instrument_data": instrument_data or {},
            "spot_price": spot,
            "expiry_date": expiry,
        }
        normalized = gamma_engine.normalize_live_payload(payload)
        if not normalized:
            return neutral
        profile = _gamma_processor().process_payload(normalized)
        if not profile:
            return neutral
        day = profile.day_gamma

        # Gross Vanna / Charm: the engine stores per-leg day exposures only as net sums
        # (day_net_charm / day_net_vanna), so recompute the gross magnitude from the same
        # ``coi * greek`` legs it aggregates, mirroring how gross_gex is built.
        day_strikes = [s for s in profile.strikes if s.has_day_coi]
        gross_vanna = sum(
            abs(s.call_coi * s.call_vanna) + abs(s.put_coi * s.put_vanna)
            for s in day_strikes
        )
        gross_charm = sum(
            abs(s.call_coi * s.call_charm) + abs(s.put_coi * s.put_charm)
            for s in day_strikes
        )
        vanna_ratio = (day.net_vanna / gross_vanna) if gross_vanna > 0 else 0.0
        charm_ratio = (day.net_charm / gross_charm) if gross_charm > 0 else 0.0

        gex_ratio = day.net_gex_ratio if math.isfinite(day.net_gex_ratio) else 0.0
        flip = day.gamma_flip_level if day.gamma_flip_level is not None else profile.gamma_flip_level

        return {
            "net_gex": float(day.net_gex),
            "net_gex_ratio": max(-1.0, min(1.0, gex_ratio)),
            "net_vanna": float(day.net_vanna),
            "net_charm": float(day.net_charm),
            "vanna_ratio": max(-1.0, min(1.0, vanna_ratio)),
            "charm_ratio": max(-1.0, min(1.0, charm_ratio)),
            "gamma_flip_level": flip,
            "spot": float(spot),
        }
    except Exception:
        logger.warning("Real GEX computation failed; gamma boost disabled.", exc_info=True)
        return neutral


# ---------------------------------------------------------------------------
# Real-time Theta (time decay) — Black-Scholes via py_vollib
# ---------------------------------------------------------------------------
SECONDS_PER_YEAR = 31_536_000  # 365 * 24 * 60 * 60 (matches the dashboard day-count convention)
THETA_EXPIRY_HOUR = 15
THETA_EXPIRY_MINUTE = 30
THETA_MIN_SECONDS = 1_800  # 30-minute safety floor to avoid division-by-zero near expiry
THETA_RISK_FREE_RATE = 0.065  # matches the Gamma engine's risk_free_rate
# De-dupes the warning log so a persistently failing IV solve (e.g. py_vollib missing)
# is logged once instead of once per publish beat (1 Hz).
_last_theta_error = None


def calculate_hourly_theta(spot_price, strike_price, option_ltp, expiry_date, option_type="c") -> float:
    """Return the real-time hourly theta (₹/hour), rounded to 2 decimal places.

    The contract requested by the dashboard:

    * ``T`` is computed dynamically as the seconds remaining until 15:30:00 IST on
      ``expiry_date``, divided by 31,536,000 (the seconds in a 365-day year).
    * If fewer than 1,800 seconds (30 minutes) remain, 0.0 is returned to avoid the
      division-by-zero / sqrt(T) instability right at the close.
    * Implied Volatility is reverse-solved from the LTP first, then fed back into the
      Black-Scholes theta. NOTE: py_vollib's ``theta()`` already divides by 365
      internally (it returns a PER-DAY figure), so the hourly decay is that daily
      theta divided by 24 — dividing by 365 again would shrink the result ~1000x and
      round it to 0.00.

    ``option_type`` is ``"c"`` (call) or ``"p"`` (put) and defaults to ``"c"``.
    """
    # py_vollib is strict about numeric types: coerce everything to plain Python
    # floats up-front so a str / Decimal / numpy scalar can never reach the solver.
    # Reject bool explicitly (a bool IS an int subclass, so float(True) == 1.0).
    if isinstance(spot_price, bool) or isinstance(strike_price, bool) or isinstance(option_ltp, bool):
        return 0.0
    try:
        spot = float(spot_price)
        strike = float(strike_price)
        ltp = float(option_ltp)
    except (TypeError, ValueError):
        return 0.0

    if not (math.isfinite(spot) and math.isfinite(strike) and math.isfinite(ltp)):
        return 0.0
    if spot <= 0 or strike <= 0 or ltp <= 0:
        return 0.0

    flag = option_type.lower() if isinstance(option_type, str) else "c"
    if flag not in ("c", "p"):
        flag = "c"

    expiry_iso = normalize_expiry(expiry_date)
    if not expiry_iso:
        return 0.0

    try:
        expiry_dt = datetime.strptime(expiry_iso, "%Y-%m-%d")
    except ValueError:
        logger.debug("Hourly theta skipped: unrecognised expiry %r.", expiry_date)
        return 0.0

    expiry_dt = expiry_dt.replace(
        hour=THETA_EXPIRY_HOUR,
        minute=THETA_EXPIRY_MINUTE,
        second=0,
        microsecond=0,
        tzinfo=IST,
    )
    seconds_remaining = (expiry_dt - datetime.now(IST)).total_seconds()

    # Safety guard: under 30 minutes to expiry the time value is numerically unstable.
    if seconds_remaining < THETA_MIN_SECONDS:
        return 0.0

    T = seconds_remaining / SECONDS_PER_YEAR
    if T <= 0:
        return 0.0

    try:
        from py_vollib.black_scholes.implied_volatility import implied_volatility
        from py_vollib.black_scholes.greeks.analytical import theta as bs_theta

        # Reverse-solve IV from the live LTP, then use it for the theta calc.
        iv = implied_volatility(ltp, spot, strike, T, THETA_RISK_FREE_RATE, flag)
        # py_vollib's theta() divides by 365 internally and returns a PER-DAY figure
        # (see its docstring). We must NOT divide by 365 again.
        daily_theta = bs_theta(flag, spot, strike, T, THETA_RISK_FREE_RATE, iv)
    except Exception as exc:
        # Log the SPECIFIC py_vollib error (e.g. PriceIsBelowIntrinsic /
        # VolatilityNotCalculable) once per distinct message so the failure is
        # diagnosable instead of being silently swallowed into a 0.0.
        global _last_theta_error
        error = f"{type(exc).__name__}: {exc}"
        if error != _last_theta_error:
            logger.warning(
                "Hourly theta calculation failed (%s) for spot=%s strike=%s ltp=%s T=%.6f.",
                error, spot, strike, ltp, T,
            )
            _last_theta_error = error
        return 0.0

    # Hourly decay = daily theta / 24 (the daily figure is already annual/365).
    hourly_theta = daily_theta / 24.0
    if not math.isfinite(hourly_theta):
        return 0.0
    return round(hourly_theta, 2)


def _find_leg_ltp(instrument_data, strike, option_type) -> float | None:
    """Return the first valid LTP for the given (strike, option_type) leg, else None."""
    for item in instrument_data.values():
        if not isinstance(item, dict):
            continue
        if (item.get("option_type") or "").upper() != option_type:
            continue
        item_strike = _coerce_float(item.get("strike"))
        if item_strike is None or abs(item_strike - strike) > 1e-6:
            continue
        ltp = _coerce_float(item.get("ltp"))
        if ltp is not None and ltp > 0:
            return ltp
    return None


def _compute_atm_theta_decay(instrument_data, spot, expiry_date) -> float:
    """Return the ATM hourly theta (₹/hr) for the live chain, or 0.0 when undeterminable."""
    if not isinstance(instrument_data, dict) or not spot or spot <= 0:
        return 0.0

    nearest_strike = None
    for item in instrument_data.values():
        if not isinstance(item, dict):
            continue
        strike = _coerce_float(item.get("strike"))
        if strike is None or strike <= 0:
            continue
        if nearest_strike is None or abs(strike - spot) < abs(nearest_strike - spot):
            nearest_strike = strike

    if nearest_strike is None:
        return 0.0

    # Prefer the CE (call) leg so the "c" flag is applied to a genuine call price
    # (never a put's LTP, which could sit below a call's intrinsic and make the IV
    # solver raise PriceIsBelowIntrinsic). Fall back to PE with the "p" flag.
    ce_ltp = _find_leg_ltp(instrument_data, nearest_strike, "CE")
    if ce_ltp is not None:
        return calculate_hourly_theta(spot, nearest_strike, ce_ltp, expiry_date, "c")

    pe_ltp = _find_leg_ltp(instrument_data, nearest_strike, "PE")
    if pe_ltp is not None:
        return calculate_hourly_theta(spot, nearest_strike, pe_ltp, expiry_date, "p")

    return 0.0


def _compute_live_sentiment_full(instrument_data=None) -> dict:
    """Derive bias / momentum / score plus the advanced-quant diagnostics.

    Distance/Delta weighting: ATM and near-ITM strikes carry full weight, decaying
    with a Gaussian kernel over distance from spot so deep-OTM strikes barely move
    the aggregate. Falls back to equal weighting when spot / strike data is absent.

    Directional bias: heavy CALL WRITING (call OI > put OI) maps NEGATIVE, so a crash
    driven by call writing drags the score DOWN toward bearish instead of lifting it
    to CE. Momentum is DIRECTIONAL too (put_coi - call_coi): call-building drives it
    negative and its rate of change (acceleration) spikes it in the same direction.

    Gamma boost: an ADVANCED GREEK BLEND of the real Black-Scholes Net GEX, Vanna and
    Charm (each normalized to a [-1, 1] net/gross ratio) weighted GEX=10 / Vanna=5 /
    Charm=5 into a base boost. That blend is then scaled by a GAMMA FLIP MULTIPLIER when
    price sits within _FLIP_ZONE_PCT of the flip level, and clamped to ±_GAMMA_BOOST_CAP
    so heavy structure counters momentum without killing explosive scalps.

    Returns a dict of raw floats so the dashboard MATH console can show the maths.
    """
    global _previous_directional_coi

    if instrument_data is None:
        instrument_data = live_state.get("instrument_data", {})

    spot = live_state.get("spot_price")
    spot = float(spot) if isinstance(spot, (int, float)) and spot > 0 else 0.0

    call_oi = put_oi = call_coi = put_coi = 0.0
    w_call_oi = w_put_oi = 0.0
    weighted = False
    call_iv_sum = put_iv_sum = 0.0
    call_iv_n = put_iv_n = 0

    # Loop-invariant: the Gaussian moneyness band depends only on spot, so derive it
    # once instead of recomputing `sigma = max(spot * 0.01, 1.0)` for every strike.
    sigma = max(spot * 0.01, 1.0) if spot > 0 else 1.0

    for item in instrument_data.values():
        if not isinstance(item, dict):
            continue
        side = (item.get("option_type") or "").upper()
        oi = item.get("oi")
        oi = float(oi) if isinstance(oi, (int, float)) else 0.0
        coi = item.get("coi")
        coi = float(coi) if isinstance(coi, (int, float)) else 0.0
        strike = item.get("strike")
        strike = float(strike) if isinstance(strike, (int, float)) else 0.0
        iv = item.get("iv")
        iv = float(iv) if isinstance(iv, (int, float)) else 0.0

        weight = 1.0
        if spot > 0 and strike > 0:
            weight = math.exp(-0.5 * ((strike - spot) / sigma) ** 2)
            weighted = True

        if side == "CE":
            call_oi += oi
            call_coi += coi
            w_call_oi += oi * weight
            if iv > 0:
                call_iv_sum += iv
                call_iv_n += 1
        elif side == "PE":
            put_oi += oi
            put_coi += coi
            w_put_oi += oi * weight
            if iv > 0:
                put_iv_sum += iv
                put_iv_n += 1

    total_oi = call_oi + put_oi
    # DIRECTIONAL COI: puts being added (bearish) minus calls being added (bullish).
    # An Indian-market crash is driven by heavy CALL WRITING: call_coi >> 0 while
    # put_coi is muted, so put_coi - call_coi goes NEGATIVE and drives momentum DOWN.
    directional_coi = put_coi - call_coi

    # Acceleration = rate of change of the directional COI signal vs the last tick.
    acceleration = 0.0 if _previous_directional_coi is None else directional_coi - _previous_directional_coi
    _previous_directional_coi = directional_coi

    # Neutral fallback: an empty OR half-initialised chain is NOT directional.
    if total_oi <= 0 or call_oi <= 0 or put_oi <= 0:
        return {
            "bias": 0.0, "momentum": 0.0, "score": 50.0,
            "delta_wt_impact": 0.0, "gamma_boost_val": 0.0,
            "iv_skew_est": 0.0, "acceleration": round(acceleration, 2),
            "net_gex": 0.0, "net_gex_ratio": 0.0,
            "net_vanna": 0.0, "net_charm": 0.0, "gamma_flip_level": None,
            "theta_decay_per_hour": 0.0,
        }

    # BEARISH BIAS: heavy CALL WRITING (call OI > put OI) maps NEGATIVE, so a crash
    # driven by call writing penalises the score instead of lifting it toward CE.
    unweighted_bias = ((put_oi - call_oi) / total_oi) * 100.0
    base_momentum = (directional_coi / total_oi) * 100.0

    # Distance-weighted bias (equal-weight fallback when no strike/spot data).
    if weighted and (w_call_oi + w_put_oi) > 0:
        weighted_bias = ((w_put_oi - w_call_oi) / (w_call_oi + w_put_oi)) * 100.0
    else:
        weighted_bias = unweighted_bias

    # How many score points the distance weighting moved the bias (bias -> score *0.5).
    delta_wt_impact = (weighted_bias - unweighted_bias) * 0.5

    # Acceleration boost: a COI spike spikes momentum, clamped to a sane scale.
    accel_boost = max(-_ACCEL_CAP, min(_ACCEL_CAP, (acceleration / total_oi) * 100.0 * _ACCEL_GAIN))
    momentum = base_momentum + accel_boost

    # ADVANCED GREEK BLEND + GAMMA FLIP MULTIPLIER --------------------------------
    # Real Black-Scholes Net GEX, Vanna and Charm (each normalized to [-1, 1]) are
    # weighted into a composite base boost, then scaled by a flip multiplier when price
    # sits inside the gamma flip zone (explosive potential).
    greeks = _real_net_gex(instrument_data, spot)
    net_gex = greeks["net_gex"]
    net_gex_ratio = greeks["net_gex_ratio"]
    net_vanna = greeks["net_vanna"]
    net_charm = greeks["net_charm"]
    gamma_flip_level = greeks["gamma_flip_level"]

    base_boost = (
        net_gex_ratio * _GEX_BLEND_WEIGHT
        + greeks["vanna_ratio"] * _VANNA_BLEND_WEIGHT
        + greeks["charm_ratio"] * _CHARM_BLEND_WEIGHT
    )

    flip_multiplier = 1.0
    if gamma_flip_level is not None and spot > 0:
        flip_distance_pct = abs(spot - gamma_flip_level) / spot * 100.0
        if flip_distance_pct < _FLIP_ZONE_PCT:
            flip_multiplier = _FLIP_ZONE_MULTIPLIER

    gamma_boost_val = max(-_GAMMA_BOOST_CAP, min(_GAMMA_BOOST_CAP, base_boost * flip_multiplier))

    # IV skew estimate: mean(put IV) - mean(call IV) in IV percentage points.
    iv_skew_est = 0.0
    if call_iv_n and put_iv_n:
        iv_skew_est = (put_iv_sum / put_iv_n) - (call_iv_sum / call_iv_n)

    # Non-linear logistic sigmoid (0-100) replaces the hard linear clamp. Neutral
    # raw_delta (0) maps to exactly 50; extremes asymptote toward 100/0 with no hard
    # ceiling, so any drop in bias/momentum/gamma immediately lowers the score instead
    # of staying pinned at a saturated ceiling.
    raw_delta = weighted_bias * 0.5 + momentum + gamma_boost_val
    score = 100.0 / (1.0 + math.exp(-0.025 * raw_delta))
    # ATM time decay (₹/hr): reverse-solved IV -> annual theta -> hourly theta.
    theta_decay_per_hour = _compute_atm_theta_decay(
        instrument_data, spot, active_expiry_iso()
    )
    return {
        "bias": weighted_bias,
        "momentum": momentum,
        "score": score,
        "delta_wt_impact": round(delta_wt_impact, 4),
        "gamma_boost_val": round(gamma_boost_val, 4),
        "iv_skew_est": round(iv_skew_est, 4),
        "acceleration": round(acceleration, 2),
        "net_gex": net_gex,
        "net_gex_ratio": net_gex_ratio,
        "net_vanna": net_vanna,
        "net_charm": net_charm,
        "gamma_flip_level": gamma_flip_level,
        "theta_decay_per_hour": theta_decay_per_hour,
    }


def _compute_live_sentiment(instrument_data=None) -> tuple[float, float, float]:
    """Backward-compatible ``(bias, momentum, score)`` view of the live sentiment."""
    result = _compute_live_sentiment_full(instrument_data)
    return result["bias"], result["momentum"], result["score"]


def _on_cvd_candle(payload: dict) -> None:
    """Record the latest cumulative CVD from the CVD engine (its worker thread)."""
    global _latest_cumulative_cvd
    if not isinstance(payload, dict):
        return
    cvd = payload.get("cvd")
    if isinstance(cvd, (int, float)):
        _latest_cumulative_cvd = float(cvd)


# ── AI Trade Setup confirmation (option-premium candlesticks) ─────────────────
# Wiring for ai_candle_confirmation.CandleConfirmationEngine. The engine was
# previously orphaned (never instantiated). It is now attached to the live feed:
# option LTP ticks for the ATM CE/PE strikes are aggregated into 1-minute candles
# by a dedicated CVDEngine instance (publish disabled), and every closed candle is
# routed to the matching engine. When the entry-zone + candlestick (Pin bar /
# Engulfing) gates pass, the engine dispatches the ``trade_trigger`` (BUY_NOW)
# WebSocket payload.
AI_SETUP_CANDLE_SECONDS = 60                 # 1-minute option candles
MOMENTUM_PULLBACK_DIP = 0.85                 # Momentum Pullback entry = LTP * 0.85 (15% dip)

# setup_type strings MUST match live_signals.html resolveBox():
#   contains "momentum" -> momentum-signal-box. The Directional Fibonacci Bounce is a
#   separate engine (fib_bounce_engine.FibBounceEngine, setup_type "fib_bounce") and is
#   NOT routed through the 1-minute option-candle engine below.
MOMENTUM_PULLBACK_SETUP = "momentum_pullback"
_AI_SETUP_TYPES = (MOMENTUM_PULLBACK_SETUP,)

# (instrument_key, setup_type) -> CandleConfirmationEngine. Written from the
# asyncio loop, read from the option-CVD worker thread, hence the lock.
_ai_engines: dict[tuple, "CandleConfirmationEngine"] = {}
_ai_engines_lock = threading.Lock()
# Anchored reference premium per (instrument_key, setup_type). The 15%-dip entry is
# derived from this ONCE and held until the ATM strike rolls. Re-anchoring to the live
# LTP every tick made the entry an unreachable moving target (close ≈ 0.85*close ±1.5%
# is never satisfied).
_ai_ref_premium: dict[tuple, float] = {}


def _ai_entry_factor(setup_type: str) -> float:
    """Entry-price multiplier for a setup, always applied to the option LTP.

    Only Momentum Pullback remains on the 1-minute option-candle engine; the
    Fibonacci Bounce engine owns its own entry factor (PREMIUM_DIP_FACTOR = 0.85).
    """
    return MOMENTUM_PULLBACK_DIP


def _option_ltp(instrument_key: str, instrument_data: dict) -> float | None:
    """Live option premium for an instrument key, or None when untraded/unknown."""
    entry = instrument_data.get(instrument_key) if isinstance(instrument_data, dict) else None
    if not isinstance(entry, dict):
        return None
    ltp = entry.get("ltp")
    if isinstance(ltp, bool):
        return None
    try:
        value = float(ltp)
    except (TypeError, ValueError):
        return None
    return value if value > 0 else None


def _resolve_atm_option_keys(spot: float | None, instrument_data: dict) -> tuple[str | None, str | None]:
    """Return the CE and PE instrument keys for the strike nearest ``spot``."""
    if not isinstance(spot, (int, float)) or spot <= 0:
        return None, None
    best_strike: float | None = None
    best_dist: float | None = None
    for key, entry in instrument_data.items():
        if not isinstance(entry, dict):
            continue
        strike = entry.get("strike")
        if isinstance(strike, bool) or not isinstance(strike, (int, float)):
            continue
        dist = abs(float(strike) - float(spot))
        if best_dist is None or dist < best_dist:
            best_strike = float(strike)
            best_dist = dist
    if best_strike is None:
        return None, None

    ce_key = pe_key = None
    for key, entry in instrument_data.items():
        if not isinstance(entry, dict):
            continue
        strike = entry.get("strike")
        if isinstance(strike, bool) or not isinstance(strike, (int, float)):
            continue
        if float(strike) != best_strike:
            continue
        side = str(entry.get("option_type") or "").strip().upper()
        if side == "CE" and ce_key is None:
            ce_key = key
        elif side == "PE" and pe_key is None:
            pe_key = key
    return ce_key, pe_key


def _refresh_ai_engines(ai_option_cvd: "CVDEngine") -> None:
    """(Re)create / refresh CandleConfirmationEngine instances for the ATM CE/PE.

    The entry price is derived from a STABLE reference premium (anchored once per
    (key, setup_type)), not the live LTP, so the "15% dip" target is actually
    reachable. Engines are re-used (history preserved) on ordinary premium drift
    and only replaced on an ATM strike roll or option-side flip.

    Thread safety:
      * Runs on the asyncio event loop (from run_streamer's 1-second while loop).
      * Reads ``live_state`` and ``INSTRUMENT_METADATA`` WITHOUT ``state_lock`` —
        safe because asyncio is single-threaded (can't interleave with
        update_instrument_state), and INSTRUMENT_METADATA is atomically swapped
        (Bug #2 fix: GIL-protected pointer read).
      * The ``_ai_engines_lock`` guards the engine dict mutations against the CVD
        worker thread's ``_route_ai_option_candle()`` call, which also holds the
        lock during ``eng.on_candle()`` (Bug #7 fix), so ``eng.update_entry()``
        here can't race with a concurrent candle evaluation.
    """
    # ── Resolve the live spot + ATM CE/PE instrument keys ──────────────────────
    spot = live_state.get("spot_price")
    if isinstance(spot, bool):
        spot = None
    try:
        spot_f = float(spot) if spot is not None else None
    except (TypeError, ValueError):
        spot_f = None

    instrument_data = live_state.get("instrument_data") or {}
    ce_key, pe_key = _resolve_atm_option_keys(spot_f, instrument_data)

    # ── Build the desired (key, setup_type) → (side, ltp) map OUTSIDE the lock ─
    # Held only briefly inside the `with _ai_engines_lock:` block below.
    desired: dict[tuple, tuple[str, float]] = {}
    for key in (ce_key, pe_key):
        if not key:
            continue
        ltp = _option_ltp(key, instrument_data)
        if not ltp:
            continue
        # INSTRUMENT_METADATA is atomically swapped (Bug #2 fix); .get() resolves
        # the current pointer under the GIL, so this is safe even if a rollover
        # swaps the dict mid-call.
        meta = INSTRUMENT_METADATA.get(key) or {}
        entry_data = instrument_data.get(key) or {}
        side = (
            str(meta.get("option_type") or entry_data.get("option_type") or "CE")
            .strip().upper() or "CE"
        )
        for setup_type in _AI_SETUP_TYPES:
            desired[(key, setup_type)] = (side, ltp)

    # ── Apply the desired engine set under the lock ───────────────────────────
    with _ai_engines_lock:
        # Drop engines for keys/setups no longer tracked (ATM strike roll).
        for k in list(_ai_engines.keys()):
            if k not in desired:
                del _ai_engines[k]
                _ai_ref_premium.pop(k, None)

        for (key, setup_type), (side, ltp) in desired.items():
            # Anchor the reference premium once; keep it until the key changes.
            ref = _ai_ref_premium.get((key, setup_type))
            if ref is None:
                ref = ltp
                _ai_ref_premium[(key, setup_type)] = ref
            entry = round(ref * _ai_entry_factor(setup_type), 2)

            eng = _ai_engines.get((key, setup_type))
            if eng is not None and eng.option_side == side:
                # Same engine: refresh entry in place so rolling history survives.
                # Bug #7 fix: _route_ai_option_candle() also holds _ai_engines_lock
                # during on_candle(), so this update_entry() can't race with a
                # concurrent candle evaluation on the CVD worker thread.
                eng.update_entry(entry, option_side=side)
                continue
            _ai_engines[(key, setup_type)] = CandleConfirmationEngine(
                setup_type, entry, symbol=key, option_side=side, dispatch=True
            )

    # ── Keep the option-candle feed allowlist in sync ──────────────────────────
    if ai_option_cvd is not None:
        ai_option_cvd.set_symbol_allowlist({k for (k, _s) in desired})


def _route_ai_option_candle(candle: dict) -> None:
    """Route a closed option candle (from the option-CVD worker thread) to the
    CandleConfirmationEngine instances tracking that instrument key."""
    if not isinstance(candle, dict):
        return
    key = candle.get("symbol")
    if not key:
        return
    with _ai_engines_lock:
        engines = [eng for (k, _setup), eng in _ai_engines.items() if k == key]
    for eng in engines:
        try:
            eng.on_candle(candle)
        except Exception:
            logger.exception("AI setup engine on_candle failed for %s", key)


def _ai_current_atr() -> dict:
    """Return the latest ATR per side from the AI candle-confirmation engines.

    Engines are keyed by (instrument_key, setup_type); both setups for a given
    instrument share the same 1-minute candle history, so the first non-null ATR
    per side is representative. Returns {"ce": float|None, "pe": float|None}.
    """
    ce_atr = pe_atr = None
    with _ai_engines_lock:
        for (_key, _setup), eng in _ai_engines.items():
            atr = getattr(eng, "latest_atr", None)
            if not isinstance(atr, (int, float)) or atr != atr or atr <= 0:
                continue
            side = (getattr(eng, "option_side", "") or "").upper()
            if side == "CE" and ce_atr is None:
                ce_atr = float(atr)
            elif side == "PE" and pe_atr is None:
                pe_atr = float(atr)
    return {"ce": ce_atr, "pe": pe_atr}


def _ai_market_no_trade_probe() -> bool:
    """Live no-trade gate for the AI candle-confirmation engine.

    Reads the SAME authoritative sentiment the dashboard consumes (score /
    momentum / whole-chain Net-OI bias) and mirrors the frontend tri-state, so a
    candle BUY_NOW trigger is suppressed whenever the AI card would render
    "🚫 NO TRADE ZONE".
    """
    return is_no_trade_zone(
        market_state.get("score"),
        market_state.get("momentum"),
        market_state.get("net_oi_bias"),
    )


# ── Directional Fibonacci Bounce (index/spot 3-minute synthesis) ─────────────────
# Independent from the option-premium CandleConfirmationEngine. Fed ONLY the live
# Index/Spot tick (not option premiums). The engine owns the 09:15 anchor capture,
# directional Fib math and 3-minute candle synthesis, and reuses the shared
# trade-trigger dispatcher (same ai:trade_trigger channel).
FIB_BOUNCE_SETUP = "fib_bounce"
_fib_bounce_engine: FibBounceEngine | None = None


def _extract_index_tick_timestamp(raw: object) -> datetime | None:
    """Epoch-ms (``ltt``) of an index tick, or None when absent."""
    if not isinstance(raw, dict):
        return None
    for path in (
        ("fullFeed", "indexFF", "ltpc", "ltt"),
        ("fullFeed", "marketFF", "ltpc", "ltt"),
        ("ltpc", "ltt"),
    ):
        value = _nested(raw, path)
        if isinstance(value, (int, float)) and value > 0:
            if value > 1e12:  # milliseconds -> seconds
                value /= 1000.0
            return datetime.fromtimestamp(value, tz=IST)
    return None


def _fib_premium_provider(side: str) -> float | None:
    """Live ATM option premium for the fib engine's STRICT 15% dip entry (x 0.85)."""
    spot = live_state.get("spot_price")
    if isinstance(spot, bool):
        spot = None
    try:
        spot_f = float(spot) if spot is not None else None
    except (TypeError, ValueError):
        spot_f = None
    instrument_data = live_state.get("instrument_data") or {}
    ce_key, pe_key = _resolve_atm_option_keys(spot_f, instrument_data)
    key = ce_key if str(side or "").upper() == "CE" else pe_key
    return _option_ltp(key, instrument_data) if key else None


def _fib_gamma_side_provider() -> str | None:
    """Authoritative CE/PE direction from the live NIFTY DAY GAMMA (Day Bias).

    Reads ``net_gex_ratio`` (net GEX / gross GEX) from the market state and maps it
    with a relaxed ±0.10 neutral band (fib_bounce_engine.GAMMA_DAY_NEUTRAL_FRACTION):
      * ratio <= -0.10 -> PE (bearish, draw Fib top->bottom)
      * ratio >= +0.10 -> CE (bullish, draw Fib bottom->top)
      * otherwise      -> None (🟡 Range-bound: Choppy / NO_TRADE — suspend Fib)
    """
    return resolve_gamma_side(market_state.get("net_gex_ratio"))


def _route_fib_index_tick(ltp: float, raw: object = None) -> None:
    """Feed a live index LTP into the fib engine (called from update_index_spot)."""
    engine = _fib_bounce_engine
    if engine is None:
        return
    try:
        engine.on_index_tick(ltp, _extract_index_tick_timestamp(raw))
    except Exception:
        logger.exception("Fib bounce engine on_index_tick failed")


def _compute_macro_metrics(instrument_data=None) -> dict:
    """Whole-chain macro metrics for the left "TOTAL OPTION CHAIN (MACRO)" panel.

    Net OI Bias and Total Vol Skew are bipolar percentages (negative = call-heavy
    = bearish, positive = put-heavy = bullish), while Total PCR is unipolar with a
    1.0 neutral centre. Volume arrives from the V3 feed as a numeric string
    (``vtt``), so it is coerced via ``_first_numeric`` rather than a strict
    int/float guard — the old guard silently dropped those string volumes and left
    ``total_vol_skew`` frozen at 0. Every field degrades to its neutral default
    (0.0 / 0.0 / 1.0) when the chain is empty, so a disconnected feed renders as a
    neutral zero-state instead of raising.
    """
    call_oi = put_oi = 0.0
    call_vol = put_vol = 0.0

    for item in (instrument_data or {}).values():
        if not isinstance(item, dict):
            continue
        side = (item.get("option_type") or "").upper()
        oi = _first_numeric(item.get("oi")) or 0.0
        volume = _first_numeric(item.get("volume")) or 0.0

        if side == "CE":
            call_oi += oi
            call_vol += volume
        elif side == "PE":
            put_oi += oi
            put_vol += volume

    total_oi = call_oi + put_oi
    total_vol = call_vol + put_vol

    net_oi_bias = ((put_oi - call_oi) / total_oi) * 100.0 if total_oi > 0 else 0.0
    total_vol_skew = ((put_vol - call_vol) / total_vol) * 100.0 if total_vol > 0 else 0.0
    total_pcr = (put_oi / call_oi) if call_oi > 0 else 1.0

    return {
        "net_oi_bias": net_oi_bias,
        "total_vol_skew": total_vol_skew,
        "total_pcr": total_pcr,
    }


def _refresh_market_state(instrument_data=None) -> None:
    """Fold the latest CVD + live sentiment peaks into the in-memory market state."""
    global _score_peak_updated_at

    sentiment = _compute_live_sentiment_full(instrument_data)
    bias, momentum, score = sentiment["bias"], sentiment["momentum"], sentiment["score"]

    ex = _sentiment_extremes
    ex["bias_max"] = max(ex["bias_max"], bias)
    ex["bias_min"] = min(ex["bias_min"], bias)
    ex["mom_max"] = max(ex["mom_max"], momentum)
    ex["mom_min"] = min(ex["mom_min"], momentum)
    # Score HWM now decays toward the neutral centre (50) with a 5-minute half-life
    # instead of holding the session extreme forever. Each tick re-derives the peak as
    # max(current_score, decayed_peak) on the bullish side and min(...) on the bearish
    # side, so a pull-back in the live score drains the stale peak over ~300 s.
    now = time.monotonic()
    elapsed = 0.0 if _score_peak_updated_at is None else max(0.0, now - _score_peak_updated_at)
    decay_factor = math.exp(-SCORE_PEAK_DECAY_RATE * elapsed / SCORE_PEAK_HALF_LIFE_SECONDS)
    ex["score_max"] = max(score, 50.0 + (ex["score_max"] - 50.0) * decay_factor)
    ex["score_min"] = min(score, 50.0 - (50.0 - ex["score_min"]) * decay_factor)
    _score_peak_updated_at = now

    market_state["cumulative_cvd"] = _latest_cumulative_cvd
    market_state["peak_bias"] = ex["bias_max"] if bias >= 0 else ex["bias_min"]
    market_state["peak_momentum"] = ex["mom_max"] if momentum >= 0 else ex["mom_min"]
    market_state["peak_score"] = ex["score_max"] if score >= 50 else ex["score_min"]
    # Advanced-quant diagnostics, persisted to Redis so views.py can serve them at 1 Hz.
    market_state["delta_wt_impact"] = sentiment["delta_wt_impact"]
    market_state["gamma_boost_val"] = sentiment["gamma_boost_val"]
    market_state["iv_skew_est"] = sentiment["iv_skew_est"]
    market_state["acceleration"] = sentiment["acceleration"]
    # Authoritative current sentiment — the single source of truth the dashboard renders.
    market_state["bias"] = bias
    market_state["momentum"] = momentum
    market_state["score"] = score
    market_state["net_gex"] = sentiment["net_gex"]
    market_state["net_gex_ratio"] = sentiment["net_gex_ratio"]
    # Advanced Greek Blend inputs (Vanna / Charm / Gamma Flip level) — the same raw
    # values driving the score's gamma boost. A missing flip is persisted as 0.0 (the
    # JSON pipeline already renders None as 0 via _json_safe_value).
    market_state["net_vanna"] = sentiment["net_vanna"]
    market_state["net_charm"] = sentiment["net_charm"]
    market_state["gamma_flip_level"] = (
        sentiment["gamma_flip_level"] if sentiment["gamma_flip_level"] is not None else 0.0
    )
    # Real-time ATM time decay, also mirrored onto live_state so it rides the same
    # Redis snapshot (live_options_data) that the Gamma engine and dashboard consume.
    market_state["theta_decay_per_hour"] = sentiment["theta_decay_per_hour"]
    live_state["theta_decay_per_hour"] = sentiment["theta_decay_per_hour"]

    # Whole-chain macro metrics (Net OI Bias / Total Vol Skew / Total PCR) + persistent
    # session peaks. Bipolar peaks keep the SIGNED extreme (greatest |value| from 0);
    # the PCR peak keeps the value furthest from the 1.0 neutral centre.
    macro = _compute_macro_metrics(instrument_data)
    market_state["net_oi_bias"] = macro["net_oi_bias"]
    market_state["total_vol_skew"] = macro["total_vol_skew"]
    market_state["total_pcr"] = macro["total_pcr"]

    # AI Trade Setups: expose the live ATR entry-zone buffer (₹) so the frontend
    # can render PRICE as [entry - ATR] - [entry + ATR]. CE/PE premiums differ,
    # so both sides are published plus a single active-side value.
    ai_atr = _ai_current_atr()
    market_state["current_atr_ce"] = ai_atr["ce"] if ai_atr["ce"] is not None else 0.0
    market_state["current_atr_pe"] = ai_atr["pe"] if ai_atr["pe"] is not None else 0.0
    _bullish = score > 55
    _bearish = (score < 45) or (macro["net_oi_bias"] < 0 and score < 50)
    if not _bullish and not _bearish:
        _bullish = momentum > 20
        _bearish = momentum < -20
    if _bearish:
        market_state["current_atr"] = market_state["current_atr_pe"]
    elif _bullish:
        market_state["current_atr"] = market_state["current_atr_ce"]
    else:
        market_state["current_atr"] = 0.0

    if abs(macro["net_oi_bias"]) > abs(market_state.get("peak_net_oi_bias", 0.0)):
        market_state["peak_net_oi_bias"] = macro["net_oi_bias"]
    if abs(macro["total_vol_skew"]) > abs(market_state.get("peak_total_vol_skew", 0.0)):
        market_state["peak_total_vol_skew"] = macro["total_vol_skew"]
    if abs(macro["total_pcr"] - 1.0) > abs(market_state.get("peak_total_pcr", 1.0) - 1.0):
        market_state["peak_total_pcr"] = macro["total_pcr"]


def _persist_market_state() -> None:
    """Write the in-memory market state to its date-scoped Redis key (24h TTL)."""
    try:
        if not save_market_state(redis_client, market_state):
            logger.warning("⚠️ Market state was not persisted (Redis unavailable).")
    except Exception as exc:
        logger.warning("⚠️ Failed to persist market state to Redis: %s", exc)


def _load_market_state() -> None:
    """Restore CVD + peak sentiment from today's Redis key on startup."""
    global _latest_cumulative_cvd

    restored = load_market_state(redis_client)
    market_state.update(restored)
    _latest_cumulative_cvd = float(restored.get("cumulative_cvd", 0.0))

    # Seed the running extremes from the persisted current-side peaks. Only one
    # number per metric was persisted, so the opposite-side extreme restarts from
    # the neutral baseline (0 for bias/momentum, 50 for score).
    ex = _sentiment_extremes
    ex["bias_max"] = restored["peak_bias"] if restored["peak_bias"] >= 0 else 0.0
    ex["bias_min"] = restored["peak_bias"] if restored["peak_bias"] < 0 else 0.0
    ex["mom_max"] = restored["peak_momentum"] if restored["peak_momentum"] >= 0 else 0.0
    ex["mom_min"] = restored["peak_momentum"] if restored["peak_momentum"] < 0 else 0.0
    ex["score_max"] = restored["peak_score"] if restored["peak_score"] >= 50 else 50.0
    ex["score_min"] = restored["peak_score"] if restored["peak_score"] < 50 else 50.0

    logger.info("♻️ Market state restored from %s: %s", market_state_key(), restored)


async def publish_live_state(force: bool = False) -> None:
    """Push ``live_state`` to Redis and notify the Gamma engine, throttled.

    The Gamma engine is driven by the PUBLISH, and it re-derives Black-Scholes greeks
    for the entire chain on every notification. Firing that on each tick burned CPU
    for updates nothing ever read, so the whole Redis hop (serialize + SET + PUBLISH)
    is rate-limited to ``REDIS_PUBLISH_MIN_INTERVAL``.

    The SET is deliberately kept together with the PUBLISH: the engine's
    ``_process_message`` does a ``GET`` on the data key when the notification is not
    itself JSON (which is our case, we publish the literal "updated"), so the key must
    be refreshed in the same beat as the notification.

    What is serialized is the SANITISED snapshot (single expiry only), not ``live_state``
    verbatim - see snapshot_for_publish().

    Pass ``force=True`` to bypass the throttle - used by the periodic dump so the
    engine still receives a final update if ticks stop arriving.

    BUG #4 FIX: The original called the synchronous redis-py pipeline +
    _refresh_market_state() (which runs the full Black-Scholes gamma engine for
    ~774 strikes) directly on the event loop, blocking it for ~30-85ms per
    publish beat. Now the blocking work runs in loop.run_in_executor() so the
    event loop can process ticks concurrently with the Redis I/O + gamma
    computation. The snapshot + json.dumps() stay on the event loop (safe:
    no concurrent mutation from other coroutines); the immutable serialized
    string is passed to the executor.
    """
    global _last_redis_publish_ts

    now = time.monotonic()
    if not force and (now - _last_redis_publish_ts) < REDIS_PUBLISH_MIN_INTERVAL:
        return

    try:
        # Stamp the snapshot as it goes out. `updated_at` used to be written only by the
        # 5-second dump_live_state(), so whenever that task fell behind, the dashboard's
        # freshness readout froze (observed 10 minutes stale) even though ticks were
        # flowing. Stamping here makes `updated_at` mean "when this payload was
        # produced", which is what every consumer actually wants.
        live_state["updated_at"] = datetime.now(IST).isoformat()
        refresh_liveness_fields()
        # Snapshot + serialize on the event loop (safe: no concurrent mutation from
        # other coroutines while we're running). The serialized string is immutable,
        # so the executor can safely use it for Redis I/O without a race.
        payload, active_expiry = snapshot_for_publish()
        serialized_state = json.dumps(payload, default=str, separators=(",", ":"))
        # The instrument_data in the payload is a shallow-copied outer dict from
        # sanitized_instrument_data(); its key set is frozen. Individual entry reads
        # are GIL-atomic, so the executor can safely iterate it for the sentiment
        # computation without seeing a partially-mutated dict.
        instrument_data = payload.get("instrument_data") or {}

        loop = asyncio.get_running_loop()
        await loop.run_in_executor(
            None,
            _publish_and_persist_sync,
            serialized_state,
            active_expiry,
            instrument_data,
            now,
        )
    except redis.RedisError as exc:
        logger.warning(f"⚠️ Failed to publish live state to Redis: {exc}")
    except Exception as exc:
        # A non-Redis failure (e.g. a NameError from a missing helper inside
        # _refresh_market_state) must not silently kill the whole publish/persist
        # beat. Log the full traceback and keep the worker alive for the next tick.
        logger.exception(f"⚠️ Non-Redis failure while publishing/persisting live state: {exc}")


def _publish_and_persist_sync(
    serialized_state: str,
    active_expiry: str | None,
    instrument_data: dict,
    now: float,
) -> None:
    """Synchronous Redis publish + market-state refresh. Runs in an executor.

    BUG #4 FIX: The original publish_live_state() did all of this on the event
    loop: json.dumps (~5ms) + Redis pipeline (~10ms) + _refresh_market_state
    (gamma engine, ~30ms) + _persist_market_state (~5ms) = ~50ms of blocking
    per 1Hz publish beat. Moving it to an executor frees the event loop to
    process ticks concurrently with the Redis I/O + gamma computation.
    """
    global _last_redis_publish_ts
    try:
        # Batch the Redis hop into ONE pipeline round-trip (mirror SET + primary SET +
        # PUBLISH). The opt-in expiry-namespaced mirror is queued FIRST so the primary
        # key stays the last write a consumer or a test observes.
        pipe = redis_client.pipeline(transaction=False)
        if REDIS_EXPIRY_NAMESPACE and active_expiry:
            pipe.set(REDIS_DATA_KEY_TEMPLATE.format(expiry=active_expiry), serialized_state)
        pipe.set(REDIS_DATA_KEY, serialized_state)
        pipe.publish(REDIS_CHANNEL, "updated")
        pipe.execute()
        _last_redis_publish_ts = now
        # Persist the cumulative / peak market metrics on every published update so
        # they survive a restart and stay fresh for views.py's initial render. Use
        # the same sanitised (active-expiry) chain the dashboard renders.
        _refresh_market_state(instrument_data)
        _persist_market_state()
    except redis.RedisError as exc:
        logger.warning(f"⚠️ Failed to publish live state to Redis: {exc}")
    except Exception as exc:
        logger.exception(f"⚠️ Non-Redis failure while publishing/persisting live state: {exc}")


async def write_live_state(force_publish: bool = False) -> None:
    """Push the current state to Redis. Deliberately does NO disk I/O.

    This runs on EVERY tick, so it has to stay cheap. It used to call
    atomic_write_json() first, which meant a full ~500 KB json.dump + fsync + rename of
    live_options_data.json per tick: ~28ms of synchronous work inside the event loop
    (measured ~32 rewrites/sec, ~91% of a CPU core) and — because the Django dashboard
    held a read handle on that same file at 1Hz — a steady stream of
    "[WinError 5] Access is denied: ...json.tmp -> ...json" failures, each of which
    silently discarded the newest snapshot.

    Disk persistence now belongs solely to dump_live_state() (every 5s, cold backup).
    The function name is kept because the tick path and the test-suite already refer
    to it.
    """
    await publish_live_state(force=force_publish)


async def update_instrument_state(instrument: str, values: dict) -> None:
    ltp = values.get("ltp")
    cp = values.get("cp")
    oi = values.get("oi")
    tick_poi = values.get("poi")
    iv = values.get("iv")
    # Upstox streams OI in contracts (lots); convert to shares so Live_OI, POI and
    # COI are all on the same NSE share-based scale before they're stored/compared.
    if isinstance(oi, (int, float)):
        oi = _scale_oi_to_internal(oi)
    if isinstance(tick_poi, (int, float)):
        tick_poi = _scale_oi_to_internal(tick_poi)
    volume = values.get("volume")

    # Volume Velocity (shares/sec) over the rolling window. `vtt` is cumulative
    # volume-traded-today, so the rate is derived from consecutive samples.
    velocity = None
    if isinstance(volume, (int, float)):
        velocity = compute_volume_velocity(instrument, volume)

    # Illiquid options may not have traded yet this session; fall back to the close
    # price for display only so the chain stays populated. The underlying index is
    # handled separately in update_index_spot and never falls back to the close price.
    display_ltp = ltp if isinstance(ltp, (int, float)) and ltp > 0 else cp

    async with state_lock:
        meta = INSTRUMENT_METADATA.get(instrument, {})
        entry = live_state["instrument_data"].setdefault(instrument, {
            'strike': meta.get('strike'),
            'option_type': meta.get('option_type'),
            'tradingsymbol': meta.get('tradingsymbol'),
            # The expiry is stamped on EVERY entry so consumers can key rows by
            # (expiry_date, strike_price) without having to parse the tradingsymbol,
            # and so a foreign chain is identifiable even after a rollover.
            'expiry': normalize_expiry(meta.get('expiry')),
        })
        if isinstance(display_ltp, (int, float)):
            entry["ltp"] = display_ltp
        if isinstance(oi, (int, float)):
            entry["oi"] = oi
            # Previous OI: prefer the value carried on the tick itself (future-proof),
            # otherwise use the single-call option-chain `prev_oi` snapshot.
            if isinstance(tick_poi, (int, float)) and tick_poi >= 0:
                poi = tick_poi
                entry["poi_source"] = "tick"
            else:
                poi = get_previous_oi(instrument)
                if poi is not None:
                    entry["poi_source"] = "option_chain"
                else:
                    # POI base persistence: never DOWNGRADE an already-known previous-day
                    # OI to None just because previous_oi_map is momentarily empty (feed
                    # restart, failed refresh, a strike listed after the last refresh).
                    # prev_oi is a static previous-day figure, so the value we already
                    # hold on the entry is still the correct base for COI.
                    retained = entry.get("poi")
                    if isinstance(retained, (int, float)) and not isinstance(retained, bool) and retained > 0:
                        poi = float(retained)
                        entry["poi_source"] = "retained"
                    else:
                        entry["poi_source"] = None
            entry["poi"] = poi

            # COI is computed dynamically on every tick: COI = live OI - previous OI.
            if isinstance(poi, (int, float)) and poi != 0:
                coi = oi - poi
                entry["coi"] = coi
                entry["coi_pct"] = round((coi / poi) * 100, 2)
            else:
                entry["coi"] = None
                entry["coi_pct"] = None
                if poi is None:
                    logger.debug(
                        "No previous OI for %s yet; COI left as None instead of guessing.",
                        instrument,
                    )
        if isinstance(volume, (int, float)):
            entry["volume"] = volume
        # Implied volatility, converted FRACTION -> PERCENT here so it lines up with the
        # Gamma engine, which does `strike.call_iv / 100` and whose missing-IV fallback
        # is 15.0 (i.e. it expects percent). Without this the engine silently ran the
        # whole surface on that flat 15% default.
        if isinstance(iv, (int, float)) and iv > 0:
            entry["iv"] = iv * IV_FRACTION_TO_PERCENT
        if velocity is not None:
            # Expose the velocity under a neutral key plus a side-specific key so the
            # dashboard/API can read `call_velocity` / `put_velocity` directly.
            entry["volume_velocity"] = velocity
            side = (entry.get("option_type") or meta.get("option_type") or "").upper()
            if side == "CE":
                entry["call_velocity"] = velocity
            elif side == "PE":
                entry["put_velocity"] = velocity
        if 'strike' not in entry and meta.get('strike') is not None:
            entry['strike'] = meta['strike']
        if 'option_type' not in entry and meta.get('option_type') is not None:
            entry['option_type'] = meta['option_type']
        if 'tradingsymbol' not in entry and meta.get('tradingsymbol'):
            entry['tradingsymbol'] = meta['tradingsymbol']
        if not entry.get('expiry') and meta.get('expiry') is not None:
            # Backfill for entries restored from an older snapshot, which predate the
            # explicit expiry stamp and only carry it inside the tradingsymbol.
            entry['expiry'] = normalize_expiry(meta.get('expiry'))
        entry["latest_tick_at"] = datetime.now(IST).isoformat()
        entry["raw_sample"] = values["raw"]

    # Publish OUTSIDE the lock, matching update_index_spot(). state_lock exists to
    # protect live_state mutations; it never protected the publish itself (json.dumps
    # contains no await, so a single event loop cannot interleave two of them), and
    # holding it across the Redis round-trip only stalls other ticks.
    await write_live_state()

    logger.debug("Published live state for %s to Redis.", instrument)


async def update_index_spot(values: dict) -> None:
    """Store a fresh underlying index spot tick as the canonical live spot.

    Only the live last-traded price (``ltpc.ltp``) is accepted — never the close
    price (``cp``) and never a ``marketOHLC`` candle field — so the dashboard can
    never latch onto a stale pre-open snapshot or a minute-candle open.
    """
    raw = values.get("raw")
    ltp = extract_index_spot(raw)
    if ltp is None:
        # Fall back to the generic extraction only if it produced a live LTP.
        candidate = values.get("ltp")
        if isinstance(candidate, (int, float)) and candidate > 0:
            ltp = float(candidate)
    if ltp is None:
        logger.debug("Skipping index spot update: no live LTP in tick (raw=%s)", raw)
        return

    async with state_lock:
        previous = live_state.get("spot_price")
        live_state["spot_price"] = ltp
        # Aliases kept so every downstream consumer (Gamma engine's _find_spot_price,
        # dashboards) resolves the same canonical number.
        live_state["underlying_ltp"] = ltp
        live_state["underlying_spot"] = ltp
        live_state["index_ltp"] = ltp
        live_state["nifty_ltp"] = ltp
        live_state["spot_instrument_key"] = INDEX_INSTRUMENT_KEY
        live_state["spot_source"] = "index_tick_ltp"
        live_state["spot_updated_at"] = datetime.now(IST).isoformat()
        # Keep the last index tick in the published snapshot. Without this there is no
        # way to tell a frozen spot apart from a frozen feed after the fact.
        live_state["index_raw_sample"] = raw

    # Directional Fibonacci Bounce: consume the live index LTP (owns the 09:15 anchor,
    # directional Fib grid + 3-minute synthesis). Routed OUTSIDE the lock because a
    # rare BUY_NOW dispatch performs a blocking Redis publish.
    _route_fib_index_tick(ltp, raw)
    live_state["fib_bounce"] = _fib_bounce_engine.snapshot() if _fib_bounce_engine is not None else {}

    await write_live_state()

    # Log only on change: an index ticks many times a second, and a silent log is
    # itself the signal that the spot has stopped moving.
    if previous != ltp:
        logger.info("📈 Index spot updated: %s -> %s", previous, ltp)


def _is_index_key(instrument: object) -> bool:
    """True when a feed key is the underlying index rather than an option contract.

    Compares case-insensitively and ignores stray whitespace, and also treats any
    ``NSE_INDEX|...`` key as an index so a renamed/extra index subscription is never
    mistaken for an option and shoved into ``instrument_data``.
    """
    if not isinstance(instrument, str):
        return False
    key = instrument.strip()
    return key.casefold() == INDEX_INSTRUMENT_KEY.casefold() or key.upper().startswith("NSE_INDEX|")


async def update_live_state_from_message(message: dict) -> None:
    if not isinstance(message, dict):
        logger.debug("Received unexpected message type from Upstox feed.")
        return

    # Freshness is measured from real ticks, never from wall-clock timers, so a
    # disconnected socket can never masquerade as a live feed.
    mark_tick_received()

    feeds = None
    if isinstance(message.get("data"), dict) and isinstance(message["data"].get("feeds"), dict):
        feeds = message["data"]["feeds"]
    elif isinstance(message.get("feeds"), dict):
        feeds = message["feeds"]

    if feeds:
        for instrument, feed_payload in feeds.items():
            # Per-instrument guard: one malformed tick must not drop the rest of the
            # batch (all 774 options arrive in one envelope, so a single bad payload
            # would otherwise discard every other update in this message).
            try:
                values = extract_live_values(feed_payload, instrument_key=instrument)
                if _is_index_key(instrument):
                    await update_index_spot(values)
                    continue
                await update_instrument_state(instrument, values)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.warning(
                    "Skipping a malformed tick for %s (feed continues).",
                    instrument, exc_info=True,
                )
        return

    payload = message.get("data") if isinstance(message.get("data"), (dict, list)) else message
    values = extract_live_values(payload)
    instrument = values.get("instrument_key")

    if not instrument:
        logger.debug("Received message without instrument_key.")
        return

    if _is_index_key(instrument):
        await update_index_spot(values)
        return

    await update_instrument_state(instrument, values)


async def dump_live_state() -> None:
    """Periodic cold backup — the ONLY code path that writes live_options_data.json.

    Every 5 seconds, not every tick. This is the whole point of the Redis migration:
    the disk file is a restart/offline convenience (load_persisted_live_state() reads it,
    and the Django view falls back to it when Redis is down), not the live IPC channel.

    Dropping from ~32 rewrites/sec to 0.2 cuts the window in which any reader can block
    the rename by ~160x, and the retry inside atomic_write_json() absorbs what is left.

    Staleness guard: this loop used to call ``publish_live_state(force=True)``
    unconditionally, which re-stamped ``updated_at`` and re-SET the payload every 5s even
    when the websocket was disconnected or silent. A dead feed therefore looked perfectly
    live to the dashboard and the Gamma engine forever - they were reading a fresh
    timestamp on frozen prices. Now a stale/disconnected snapshot is NOT force-published:
    Redis keeps the last genuinely live payload (and its older `updated_at`), and the
    published `stale` / `feed_status` / `seconds_since_last_tick` fields say why.
    """
    while True:
        await asyncio.sleep(5)
        # BUG #1 FIX: Only hold state_lock for the snapshot extraction (a few
        # microseconds). The original held it across atomic_write_json() +
        # publish_live_state(force=True) — blocking ALL tick processing for
        # ~50-85ms every 5 seconds (Redis I/O + full gamma engine computation).
        # Now the I/O and publish happen OUTSIDE the lock, matching the tick
        # path's design (update_instrument_state also publishes outside the
        # lock: "state_lock exists to protect live_state mutations; it never
        # protected the publish itself").
        async with state_lock:
            live, age = refresh_liveness_fields()
            if live:
                live_state["updated_at"] = datetime.now(IST).isoformat()
            payload, _ = snapshot_for_publish()
            # `dumped_at` records when the FILE was written, which is deliberately not the
            # same claim as `updated_at` ("when live data was produced").
            payload["dumped_at"] = datetime.now(IST).isoformat()

        # OUTSIDE the lock — file I/O and Redis publish don't need exclusive
        # access to live_state. The payload is already a shallow-copied dict
        # from snapshot_for_publish(), so mutations to live_state by other
        # coroutines won't affect what we write/publish here.
        try:
            atomic_write_json(JSON_PATH, payload)
        except Exception as exc:
            # Non-fatal by design: Redis already carries the live snapshot, so a
            # failed cold backup costs nothing until the next restart.
            logger.warning(f"⚠️ Failed to write the cold backup file: {exc}")

        if live:
            # Guaranteed floor for the throttled Redis hop: if ticks go quiet for a
            # moment (illiquid strike, brief lull) the Gamma engine still gets a
            # snapshot instead of being starved by the throttle.
            await publish_live_state(force=True)
            continue

        # Publish once at startup so consumers are not stuck on an empty key while
        # waiting for the first tick, then stay silent until the feed is live again.
        if _last_redis_publish_ts == 0.0:
            logger.info(
                "Seeding Redis with the startup snapshot (feed_status=%s); it is flagged stale.",
                live_state.get("feed_status"),
            )
            await publish_live_state(force=True)
            continue

        feed_health["stale_publish_skips"] += 1
        if feed_health["stale_publish_skips"] % 12 == 1:  # ~once a minute
            logger.warning(
                "⏸️ Skipping the forced Redis publish: feed_status=%s, last tick %s ago "
                "(threshold %.0fs). Consumers keep the last live snapshot instead of a "
                "restamped stale one.",
                live_state.get("feed_status"),
                "never" if age is None else f"{age:.1f}s",
                STALE_FEED_SECONDS,
            )

def _parse_cvd_symbols():
    """Parse CVD_SYMBOLS (comma-separated) into a symbol allowlist.

    Empty/unset => None (track every instrument the feed delivers).
    """
    raw = os.getenv("CVD_SYMBOLS", "")
    if not raw.strip():
        return None
    return {s.strip() for s in raw.split(",") if s.strip()}


def build_streamer(token: str, instrument_keys: list[str], expiry_date: date, loop: asyncio.AbstractEventLoop, cvd_engine=None, future_key=None, ai_option_cvd=None) -> MarketDataStreamerV3:
    config = Configuration()
    config.access_token = token
    api_client = ApiClient(configuration=config)

    # The index spot MUST be part of the subscription list itself, not bolted on
    # afterwards: MarketDataStreamerV3.handle_open() calls subscribe_to_initial_keys(),
    # which replays only the keys it already knows about. Anything missing from this
    # list therefore vanishes silently after the first auto-reconnect.
    subscription_keys = list(dict.fromkeys([*instrument_keys, INDEX_INSTRUMENT_KEY, *([future_key] if future_key else [])]))

    streamer = MarketDataStreamerV3(
        api_client=api_client,
        instrumentKeys=subscription_keys,
        mode=MarketDataStreamerV3.Mode["FULL"],
    )

    def on_open(*args):
        logger.info("✅ Upstox feed socket opened.")
        mark_socket_connected()
        # Verified against the live V3 feed: an index subscribed in `full` mode does
        # return fullFeed.indexFF.ltpc.ltp, so options and the index share a single
        # mode and a single subscription set. That keeps the reconnect path honest.
        # `instrument_keys` is read fresh on every open (it is mutated in place by an
        # expiry rollover), so a reconnect after 15:30 re-subscribes the NEW chain
        # rather than replaying the expired one.
        try:
            keys = list(dict.fromkeys([*instrument_keys, INDEX_INSTRUMENT_KEY, *([future_key] if future_key else [])]))
            streamer.subscribe(keys, mode=MarketDataStreamerV3.Mode["FULL"])
            logger.info(
                "📡 Subscribed to %d instruments on full mode (%d options + index spot '%s').",
                len(keys), len(instrument_keys), INDEX_INSTRUMENT_KEY,
            )
            logger.info(f"📡 Sample instrument keys: {keys[:5]} ... {keys[-1]!r}")
        except Exception as exc:
            logger.error(f"❌ Failed to subscribe on open: {exc}")

    def on_message(*args):
        message = args[-1] if args else None
        # Was an unconditional print() of the full decoded tick. With 774 options plus
        # the index that is a synchronous, blocking write of hundreds of MB per session
        # — enable DEBUG logging when you actually need to inspect raw ticks.
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug("Raw tick received: %s", message)
        if message is None:
            logger.warning("⚠️ Upstox feed MESSAGE received with no payload.")
            return
        try:
            # CVD: parse the same envelope and enqueue ticks (non-blocking, WS-thread safe).
            if cvd_engine is not None:
                cvd_engine.ingest_upstox_message(message)
            # AI Trade Setups: feed the SAME envelope into the option-candle engine so the
            # ATM CE/PE premiums are bucketed into 1-minute candles for confirmation.
            if ai_option_cvd is not None:
                ai_option_cvd.ingest_upstox_message(message)
            asyncio.run_coroutine_threadsafe(update_live_state_from_message(message), loop)
        except Exception:
            # A malformed tick must never propagate out of the WebSocket callback thread:
            # an unhandled raise here would tear down the connection (or the whole worker).
            logger.exception("⚠️ Dropping a malformed tick in the feed callback.")

    def on_error(*args):
        error = args[-1] if args else None
        logger.error(f"❌ Upstox feed error: {error}")

    def on_close(*args):
        code = args[0] if len(args) > 0 else None
        msg = args[1] if len(args) > 1 else None
        # The socket is DOWN from here on. dump_live_state() consults this before it
        # force-publishes, so a dropped connection stops refreshing `updated_at` instead
        # of making the frozen chain look live.
        mark_socket_disconnected(str(code) if code is not None else "closed")
        logger.warning(f"🔌 Upstox feed closed: {code} {msg}")

    streamer.on(streamer.Event["OPEN"], on_open)
    streamer.on(streamer.Event["MESSAGE"], on_message)
    streamer.on(streamer.Event["ERROR"], on_error)
    streamer.on(streamer.Event["CLOSE"], on_close)
    streamer.auto_reconnect(True, interval=5, retry_count=1)
    return streamer


async def previous_oi_refresh_loop(active: dict) -> None:
    """Periodically re-pull the option-chain ``prev_oi`` snapshot for the ACTIVE expiry.

    ``active`` is the mutable holder owned by ``run_streamer``, so after an expiry
    rollover this loop automatically starts refreshing the new chain.
    """
    loop = asyncio.get_running_loop()
    while True:
        await asyncio.sleep(POI_REFRESH_SECONDS)
        expiry = active.get("expiry")
        if expiry is None:
            continue
        try:
            # Blocking `requests` call — keep it off the event loop.
            await loop.run_in_executor(None, refresh_previous_oi, expiry)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.error("Periodic previous-OI refresh failed.", exc_info=True)


async def apply_expiry_rollover(
    streamer,
    active: dict,
    instrument_keys: list,
    new_expiry: date,
    new_keys: list,
    loop: asyncio.AbstractEventLoop,
) -> None:
    """Swap the live subscription from an expired chain to ``new_expiry``.

    Order matters: the old keys are released BEFORE the new ones are added, because
    774 old + 774 new + the index would breach the 1500-key combined 'full'
    subscription limit mid-swap and Upstox would reject the request.
    """
    old_keys = list(instrument_keys)
    try:
        if old_keys:
            streamer.unsubscribe(old_keys)
            logger.info("🔻 Unsubscribed %d expired contracts.", len(old_keys))
    except Exception as exc:
        logger.warning("Could not unsubscribe the expired chain (%s); continuing.", exc)

    # Point every rollover-aware consumer at the new expiry. The in-place slice
    # assignment matters: build_streamer's on_open closes over this exact list object.
    instrument_keys[:] = list(new_keys)
    active["expiry"] = new_expiry

    async with state_lock:
        # The expired chain's OI/COI/velocity are meaningless for the new contracts.
        live_state["instrument_data"] = {}
        live_state["expiry_date"] = new_expiry.isoformat()
        live_state["restored_from_disk"] = False
        live_state["restored_instruments"] = 0
        live_state["restored_snapshot_updated_at"] = None
    volume_history.clear()
    previous_oi_map.clear()

    # Fresh previous-day OI for the new expiry before the first ticks land.
    await loop.run_in_executor(None, refresh_previous_oi, new_expiry)

    try:
        future_key = active.get("nifty_future_key")
        keys = list(dict.fromkeys([*new_keys, INDEX_INSTRUMENT_KEY, *([future_key] if future_key else [])]))
        streamer.subscribe(keys, mode=MarketDataStreamerV3.Mode["FULL"])
        logger.info(
            "🔁 Rolled over to expiry %s (%d options + index spot).",
            new_expiry.isoformat(), len(new_keys),
        )
    except Exception as exc:
        logger.error("❌ Failed to subscribe the rolled-over chain: %s", exc)

    await publish_live_state(force=True)


async def run_streamer(instrument_keys: list[str], expiry_date: date) -> None:
    token = read_access_token()
    if not token:
        return

    # Mutable holder shared with the POI refresh loop so an expiry rollover is picked
    # up without tearing down and recreating background tasks.
    active = {"expiry": expiry_date}
    # Own a mutable copy: apply_expiry_rollover() rewrites it in place and
    # build_streamer's on_open closes over this exact object.
    instrument_keys = list(instrument_keys)

    live_state["expiry_date"] = expiry_date.isoformat()
    # Do NOT blank the chain on startup. Reuse the last on-disk snapshot for the same
    # expiry so the dashboard has data immediately (essential for testing outside
    # market hours); every entry is replaced by the first live tick for that strike.
    volume_history.clear()
    previous_oi_map.clear()
    restored = load_persisted_live_state(expiry_date)
    if restored:
        logger.info(
            "♻️ Reused %d instruments from the previous snapshot (updated_at=%s) "
            "so the option chain is populated before the first tick.",
            restored, live_state.get("restored_snapshot_updated_at"),
        )
    else:
        live_state["instrument_data"] = {}
        live_state["restored_from_disk"] = False
        live_state["restored_instruments"] = 0
        live_state["restored_snapshot_updated_at"] = None

    # refresh_previous_oi() makes ONE blocking `requests` call; run it in an executor
    # so the asyncio event loop is never frozen during startup.
    loop = asyncio.get_running_loop()
    # POI base persistence: re-hydrate the previous-day OI baseline from disk BEFORE the
    # first tick can land, so `coi` is computed from tick #1 instead of being published as
    # None (rendered as 0 by the dashboard, read as "no OI change" by the Gamma engine)
    # for the whole duration of the REST call - or for the whole session if it fails.
    hydrate_previous_oi(expiry_date)
    await loop.run_in_executor(None, refresh_previous_oi, expiry_date)

    # Restore the cumulative CVD + peak sentiment persisted under market_state:{date}
    # so a worker restart continues the trading day instead of resetting to 0.
    _load_market_state()

    poi_task = asyncio.create_task(previous_oi_refresh_loop(active))

    # ── NIFTY future auto-selection (no manual CVD_SYMBOLS needed) ────────────
    # Resolve the nearest (current-month) NIFTY index future from the Upstox master
    # and feed ONLY that instrument into the CVD engine. Falls back to CVD_SYMBOLS
    # (or "track all") if the master file can't be fetched.
    nifty_future_key, nifty_future_expiry = await loop.run_in_executor(
        None, find_nearest_nifty_future_key
    )
    active["nifty_future_key"] = nifty_future_key
    active["nifty_future_expiry"] = nifty_future_expiry

    if nifty_future_key:
        cvd_allowlist = {nifty_future_key}
    else:
        cvd_allowlist = _parse_cvd_symbols()

    # ── CVD engine: live Upstox ticks -> 2m candles -> Redis pub/sub ──────────
    cvd_engine = CVDEngine(symbol_allowlist=cvd_allowlist, on_candle=_on_cvd_candle)
    # Continue the running CVD from the value persisted under market_state:{date}
    # so a worker restart does not reset the day's cumulative volume delta to 0.
    if nifty_future_key and _latest_cumulative_cvd:
        cvd_engine.restore_cumulative_cvd(nifty_future_key, _latest_cumulative_cvd)
        logger.info(
            "♻️ Seeded CVD for %s with restored cumulative CVD %.2f.",
            nifty_future_key, _latest_cumulative_cvd,
        )
    cvd_engine.start()
    if nifty_future_key:
        logger.info(
            "📊 CVD engine started — tracking NIFTY future %s (expiry %s).",
            nifty_future_key, nifty_future_expiry.isoformat(),
        )
    elif cvd_allowlist:
        logger.info(
            "📊 CVD engine started — tracking %d symbol(s): %s",
            len(cvd_allowlist), ", ".join(sorted(cvd_allowlist)),
        )
    else:
        logger.info("📊 CVD engine started — tracking ALL instruments.")

    # ── AI Trade Setup option-candle confirmation engine ───────────────────────
    # A second CVDEngine instance dedicated to the ATM CE/PE option premiums. It
    # reuses the proven worker-thread + 1-minute Pandas resampling, but publishes
    # nothing to Redis (publish=False) and routes each closed option candle to the
    # CandleConfirmationEngine instances instead of recording cumulative CVD.
    ai_option_cvd = CVDEngine(
        candle_seconds=AI_SETUP_CANDLE_SECONDS,
        trend_periods=3,
        symbol_allowlist=set(),
        on_candle=_route_ai_option_candle,
        publish=False,
    )
    # Wire the live no-trade gate so the candle-confirmation engine suppresses
    # BUY_NOW triggers whenever the dashboard's tri-state is in a NO TRADE ZONE.
    set_market_no_trade_probe(_ai_market_no_trade_probe)
    ai_option_cvd.start()
    _refresh_ai_engines(ai_option_cvd)
    logger.info("🤖 AI Trade Setup confirmation engine started (1m option candles).")

    # ── Directional Fibonacci Bounce (index/spot 3-minute synthesis) ───────────
    global _fib_bounce_engine
    _fib_bounce_engine = FibBounceEngine(
        FIB_BOUNCE_SETUP,
        entry_factor=MOMENTUM_PULLBACK_DIP,  # 0.85 — STRICT 15% dip, unchanged
        premium_provider=_fib_premium_provider,
        gamma_side_provider=_fib_gamma_side_provider,
        no_trade_probe=_ai_market_no_trade_probe,
        dispatch=True,
    )
    logger.info("🎯 Directional Fibonacci Bounce engine started (index 3m synthesis).")

    streamer = build_streamer(token, instrument_keys, expiry_date, loop, cvd_engine=cvd_engine, ai_option_cvd=ai_option_cvd, future_key=nifty_future_key)
    logger.info(f"🔌 Connecting to Upstox live feed for {len(instrument_keys) + 1} instruments (options + index spot)...")
    streamer.connect()

    last_rollover_check = time.monotonic()
    last_future_check = time.monotonic()
    try:
        while not is_shutdown_requested():
            await asyncio.sleep(1)

            # Keep the ATM CE/PE engines + option-candle allowlist in sync as the
            # spot drifts (cheap + idempotent; engines retain their candle history).
            _refresh_ai_engines(ai_option_cvd)

            # --- NIFTY future rollover watch (monthly contract swap) ----------
            if time.monotonic() - last_future_check >= FUTURE_ROLLOVER_CHECK_SECONDS:
                last_future_check = time.monotonic()
                try:
                    new_future_key, new_future_expiry = await loop.run_in_executor(
                        None, find_nearest_nifty_future_key
                    )
                except Exception:
                    logger.error("NIFTY future rollover lookup failed; will retry.", exc_info=True)
                    new_future_key = None
                if new_future_key and new_future_key != active.get("nifty_future_key"):
                    await _rollover_nifty_future(streamer, active, new_future_key, new_future_expiry, cvd_engine)

            # --- expiry rollover watch -------------------------------------
            # Checked while running, not just at startup: a process launched at 09:00
            # on expiry day would otherwise stay bound to the dead chain from 15:30
            # until it happened to be restarted.
            if time.monotonic() - last_rollover_check < EXPIRY_ROLLOVER_CHECK_SECONDS:
                continue
            last_rollover_check = time.monotonic()
            if active["expiry"] >= earliest_tradable_expiry():
                continue

            logger.info(
                "⏭️ Expiry %s stopped trading at %02d:%02d IST; resolving the next chain...",
                active["expiry"].isoformat(), MARKET_CLOSE_HOUR, MARKET_CLOSE_MINUTE,
            )
            try:
                new_expiry, new_keys = await loop.run_in_executor(
                    None, find_nifty_weekly_option_keys
                )
            except Exception:
                logger.error("Expiry rollover lookup failed; will retry.", exc_info=True)
                continue
            if not new_keys or new_expiry is None or new_expiry == active["expiry"]:
                logger.warning(
                    "Rollover could not resolve a newer expiry (got %s); will retry.", new_expiry
                )
                continue
            await apply_expiry_rollover(
                streamer, active, instrument_keys, new_expiry, new_keys, loop
            )
    finally:
        poi_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await poi_task
        if hasattr(streamer, "disconnect"):
            try:
                streamer.disconnect()
            except Exception:
                pass
        elif hasattr(streamer, "close"):
            try:
                streamer.close()
            except Exception:
                pass
        cvd_engine.stop()
        ai_option_cvd.stop()


async def main() -> None:
    setup_signal_handlers()
    # Idempotent: returns immediately when __main__ already owns the lock. Repeated here
    # so an alternative entry point (an orchestrator importing and awaiting main()) is
    # covered by the same single-instance rule.
    if not acquire_single_instance_lock():
        return
    expiry_date, instrument_keys = find_nifty_weekly_option_keys()
    if not instrument_keys or expiry_date is None:
        logger.error("No NIFTY option instrument keys found for the next valid expiry.")
        return

    dump_task = asyncio.create_task(dump_live_state())
    try:
        await run_streamer(instrument_keys, expiry_date)
    finally:
        dump_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await dump_task
        mark_socket_disconnected("shutdown")


if __name__ == "__main__":
    setup_signal_handlers()
    # Single-instance guard FIRST: never open a second websocket, never let a second
    # process SET the shared Redis key. Deliberately here and not at import time so the
    # test-suite (and any tooling that imports this module) is unaffected.
    if not acquire_single_instance_lock():
        sys.exit(1)
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("🛑 Upstox live feed stopped by user.")
    finally:
        release_single_instance_lock()

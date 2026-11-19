from django.conf import settings
from django.http import HttpResponse, JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.shortcuts import redirect, render
from pathlib import Path
import json
import logging
import math
import os
import threading
import pandas as pd
import requests
import psycopg2
import time as std_time  # aliased to avoid clashing with `datetime.time` below
from datetime import date, datetime, time, timedelta, timezone
from Nse_Project.Swing_ML.views import get_swing_ml_context
# Shared (expiry_date, strike_price) keying. The feed, this dashboard and the Gamma
# engine MUST resolve an instrument's expiry identically, otherwise one of them merges
# two chains into a single row while the others do not.
from Nse_Project.expiry_utils import (
    belongs_to_expiry,
    expiry_group_key,
    normalize_expiry,
    resolve_row_expiry,
)
from Nse_Project.Live_dashboard.market_state import load_market_state, NIFTY_2M_SEED_CANDLES_KEY

try:
    import redis
except ImportError:  # Keeps the dashboard importable until redis-py is installed.
    redis = None

logger = logging.getLogger(__name__)

# Shared India (IST) timezone, reused by the feed-timestamp parser and the pre-open
# context builder instead of reconstructing a tz object on every call.
IST = timezone(timedelta(hours=5, minutes=30))

# Pre-open wall strikes are derived from the previous session close +/- an offset and
# rounded to the nearest NIFTY strike. The PRE-OPEN GAMMA TRAP is judged against these
# morning / previous-EOD wall levels and must NOT drift with intraday OI, so they are
# decoupled from RedisGammaBot.last_profile (its call_wall/put_wall come from the live
# option chain's max OI and would shift during the session).
GAMMA_WALL_OFFSET = 100          # points around the reference (previous close / Final IEP)
GAMMA_WALL_STRIKE_STEP = 50      # round walls to the nearest 50-point NIFTY strike

# ---------------------------------------------------------------------------
# Live options IPC — Redis first, cold JSON only as a fallback
# ---------------------------------------------------------------------------
# upstox_live_feed.py SETs the entire live_state under this key on every throttled
# publish and notifies ``gamma:live_options_updated``. Reading Redis here replaces the
# old per-poll open() on live_options_data.json, which was the direct cause of
# "[WinError 5] Access is denied: ...json.tmp -> ...json" in the feed: a plain Windows
# open() does not grant FILE_SHARE_DELETE, so this view's ~3.5 ms read handle made the
# feed's os.replace() fail with ERROR_ACCESS_DENIED. Redis has no such semantics, so
# the failure mode is structurally impossible now, not merely less likely.
#
# Deliberately a RAW redis client and not django.core.cache: settings.CACHES is
# LocMemCache (per-process, invisible to the feed), and even under django-redis the
# cache backend key-prefixes (":1:") and pickles its values, so it could never read the
# plain string key the feed writes. The env var names match GAMMA/gamma_blast_engine.py
# so one setting moves the feed, the engine and the dashboard together.
REDIS_URL = os.getenv('REDIS_URL', 'redis://127.0.0.1:6379/0')
LIVE_OPTIONS_REDIS_KEY = os.getenv("GAMMA_REDIS_DATA_KEY", "live_options_data")
# Fail fast: a dead Redis must never hold a 1 Hz dashboard poll open.
LIVE_OPTIONS_REDIS_TIMEOUT = float(os.getenv("LIVE_OPTIONS_REDIS_TIMEOUT", "0.5"))
# Circuit breaker. After a failed Redis attempt, skip Redis entirely for this long and
# serve the cold backup directly. Without it every single 1 Hz poll would pay the full
# connect timeout above for as long as Redis stayed down (measured 514 ms per poll),
# which would make the dashboard crawl and pile up Django worker threads.
LIVE_OPTIONS_REDIS_COOLDOWN = float(os.getenv("LIVE_OPTIONS_REDIS_COOLDOWN", "5.0"))
# Cold backup written by the feed's dump_live_state() every 5 s. Anchored on
# settings.BASE_DIR because that is where the feed's JSON_PATH points; the previous
# constant here resolved to Nse_Project/live_options_data.json — a stale 114-byte
# orphan with an empty option chain.
LIVE_OPTIONS_COLD_BACKUP = Path(settings.BASE_DIR) / "live_options_data.json"

_live_options_redis = None
_live_options_redis_lock = threading.Lock()
_redis_outage_logged = False
_redis_retry_after = 0.0

LOT_SIZE = 65  # NIFTY option lot size (used to convert OI/COI shares -> lots)

# Hard ceiling on the NSE round-trips behind get_active_strike_bounds(). These run inside
# a request that the dashboard polls at ~1 Hz, and requests without a timeout block the
# worker thread indefinitely - a slow/hanging nseindia.com would pile up Django threads
# until the server stopped answering at all.
NSE_REQUEST_TIMEOUT = float(os.getenv("NSE_REQUEST_TIMEOUT", "5.0"))

# Dynamic strike boundaries derived from the most-active NIFTY contracts.
_ACTIVE_STRIKE_BOUNDS: tuple | None = None
_ACTIVE_STRIKE_BOUNDS_FETCHED_AT: float | None = None
# Timestamp of the last ATTEMPT (success or failure). Without this a persistently failing
# NSE endpoint was re-tried on every single poll, because only successes were ever cached.
_ACTIVE_STRIKE_BOUNDS_ATTEMPTED_AT: float | None = None
_ACTIVE_STRIKE_BOUNDS_TTL_SECONDS = 300  # refresh at most once every 5 minutes


def _extract_active_strike_bounds(result) -> tuple | None:
    """Return (lowest_strike, highest_strike) from most_active_index_contracts() output."""
    try:
        df, _ = result
        strike_col = None
        for col in df.columns:
            label = col[-1] if isinstance(col, tuple) else col
            if str(label).strip().lower() in ("strike price", "strikeprice"):
                strike_col = col
                break
        if strike_col is None:
            return None
        strikes = pd.to_numeric(df[strike_col], errors="coerce").dropna()
        if strikes.empty:
            return None
        return (float(strikes.min()), float(strikes.max()))
    except Exception:
        return None


def _cache_active_strike_bounds(result) -> tuple | None:
    global _ACTIVE_STRIKE_BOUNDS, _ACTIVE_STRIKE_BOUNDS_FETCHED_AT
    bounds = _extract_active_strike_bounds(result)
    if bounds is not None:
        _ACTIVE_STRIKE_BOUNDS = bounds
        _ACTIVE_STRIKE_BOUNDS_FETCHED_AT = std_time.monotonic()
    return _ACTIVE_STRIKE_BOUNDS


def get_active_strike_bounds(force_refresh: bool = False) -> tuple | None:
    """Return cached (lowest, highest) active strike bounds, refreshing when stale.

    Both successes AND failures are rate-limited to one NSE attempt per TTL window. The
    caller is a ~1 Hz dashboard poll, so retrying a broken/slow NSE endpoint on every call
    would add a network round-trip to every poll; build_option_chain_rows() already falls
    back to a safe NIFTY strike band when the bounds are unavailable.
    """
    global _ACTIVE_STRIKE_BOUNDS_ATTEMPTED_AT

    if (
        not force_refresh
        and _ACTIVE_STRIKE_BOUNDS is not None
        and _ACTIVE_STRIKE_BOUNDS_FETCHED_AT is not None
        and (std_time.monotonic() - _ACTIVE_STRIKE_BOUNDS_FETCHED_AT) < _ACTIVE_STRIKE_BOUNDS_TTL_SECONDS
    ):
        return _ACTIVE_STRIKE_BOUNDS

    # Negative cache: hold off after a failed attempt too.
    if (
        not force_refresh
        and _ACTIVE_STRIKE_BOUNDS_ATTEMPTED_AT is not None
        and (std_time.monotonic() - _ACTIVE_STRIKE_BOUNDS_ATTEMPTED_AT) < _ACTIVE_STRIKE_BOUNDS_TTL_SECONDS
    ):
        return _ACTIVE_STRIKE_BOUNDS

    _ACTIVE_STRIKE_BOUNDS_ATTEMPTED_AT = std_time.monotonic()
    result = most_active_index_contracts()
    if result is not None:
        _cache_active_strike_bounds(result)
    else:
        logger.warning(
            "Could not refresh the active strike bounds from NSE; keeping %s and retrying in "
            "at most %ds.", _ACTIVE_STRIKE_BOUNDS, _ACTIVE_STRIKE_BOUNDS_TTL_SECONDS,
        )

    return _ACTIVE_STRIKE_BOUNDS


# 1. Data-fetch function
def most_active_index_contracts():
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Referer': 'https://www.nseindia.com/'
    }
    session = requests.Session()
    
    try:
        ref_url = 'https://www.nseindia.com/market-data/most-active-contracts'
        ref = session.get(ref_url, headers=headers, timeout=NSE_REQUEST_TIMEOUT)
        cookies = ref.cookies.get_dict()

        calls_url = 'https://www.nseindia.com/api/snapshot-derivatives-equity?index=calls-index-vol'
        puts_url = 'https://www.nseindia.com/api/snapshot-derivatives-equity?index=puts-index-vol'

        res_calls = session.get(calls_url, headers=headers, cookies=cookies, timeout=NSE_REQUEST_TIMEOUT)
        res_puts = session.get(puts_url, headers=headers, cookies=cookies, timeout=NSE_REQUEST_TIMEOUT)

        # ISSUE #7 FIX: Check HTTP status before .json() — a 403/429/500 from NSE
        # returns an HTML error page that causes JSONDecodeError, which was silently
        # swallowed by the broad except below. Now we log the specific HTTP status.
        if res_calls.status_code != 200:
            logger.warning("NSE calls API returned HTTP %s", res_calls.status_code)
            return None
        if res_puts.status_code != 200:
            logger.warning("NSE puts API returned HTTP %s", res_puts.status_code)
            return None

        calls_list = res_calls.json().get('OPTIDX', {}).get('data', [])
        puts_list = res_puts.json().get('OPTIDX', {}).get('data', [])

        df_calls = pd.DataFrame(calls_list)
        df_puts = pd.DataFrame(puts_list)

        if df_calls.empty and df_puts.empty:
            return None

        if not df_calls.empty:
            df_calls['expiryDate'] = pd.to_datetime(df_calls['expiryDate'], errors='coerce')
            df_calls = df_calls.rename(columns={
                'openInterest': 'oi',
                'numberOfContractsTraded': 'volume',
                'lastPrice': 'ltp'
            })

        if not df_puts.empty:
            df_puts['expiryDate'] = pd.to_datetime(df_puts['expiryDate'], errors='coerce')
            df_puts = df_puts.rename(columns={
                'openInterest': 'oi',
                'numberOfContractsTraded': 'volume',
                'lastPrice': 'ltp'
            })

        expiry_candidates = []
        if not df_calls.empty:
            expiry_candidates.append(df_calls['expiryDate'].min())
        if not df_puts.empty:
            expiry_candidates.append(df_puts['expiryDate'].min())

        if not expiry_candidates:
            return None

        current_week_expiry = min(expiry_candidates)
        current_week_expiry = pd.to_datetime(current_week_expiry, errors='coerce')
        if pd.isna(current_week_expiry):
            return None

        if not df_calls.empty:
            df_calls = df_calls[df_calls['expiryDate'] == current_week_expiry]
        if not df_puts.empty:
            df_puts = df_puts[df_puts['expiryDate'] == current_week_expiry]

        if df_calls.empty and df_puts.empty:
            return None

        if not df_calls.empty:
            df_calls = df_calls.add_prefix('call_')
            df_calls['strikePrice'] = df_calls['call_strikePrice']
            df_calls['underlying'] = df_calls['call_underlying']
            for col in ['oi%', 'coi', 'coi%']:
                df_calls[f'call_{col}'] = '-'

        if not df_puts.empty:
            df_puts = df_puts.add_prefix('put_')
            df_puts['strikePrice'] = df_puts['put_strikePrice']
            df_puts['underlying'] = df_puts['put_underlying']
            for col in ['oi%', 'coi', 'coi%']:
                df_puts[f'put_{col}'] = '-'

        if not df_calls.empty and not df_puts.empty:
            merged_df = pd.merge(df_calls, df_puts, on=['underlying', 'strikePrice'], how='outer')
        elif not df_calls.empty:
            merged_df = df_calls
        else:
            merged_df = df_puts

        if 'underlying' in merged_df.columns:
            merged_df = merged_df.drop(columns=['underlying'])

        ordered_cols = [
            'call_oi%', 'call_oi', 'call_volume', 'call_ltp', 'call_coi', 'call_coi%',
            'strikePrice',
            'put_coi%', 'put_coi', 'put_ltp', 'put_volume', 'put_oi', 'put_oi%'
        ]
        final_cols = [col for col in ordered_cols if col in merged_df.columns]
        merged_df = merged_df[final_cols].sort_values(by=['strikePrice']).reset_index(drop=True)

        top_headers = []
        bottom_headers = []
        for col in merged_df.columns:
            if col == 'strikePrice':
                top_headers.append('')
                bottom_headers.append('Strike Price')
            elif col.startswith('call_'):
                top_headers.append('CALL')
                bottom_headers.append(col.replace('call_', '').upper())
            elif col.startswith('put_'):
                top_headers.append('PUT')
                bottom_headers.append(col.replace('put_', '').upper())
            else:
                top_headers.append('')
                bottom_headers.append(col.upper())

        merged_df.columns = pd.MultiIndex.from_arrays([top_headers, bottom_headers])

        return merged_df, current_week_expiry.strftime('%d-%b-%Y')

    except Exception:
        return None

def dashboard_view(request):
    result = most_active_index_contracts()

    if result is not None:
        df, current_expiry = result
        # Warm the active strike-bound cache used by the live option chain filter.
        _cache_active_strike_bounds(result)
    else:
        df, current_expiry = None, None

    context = {'current_expiry': current_expiry}
    top_20 = []
    top_stocks_csv = (
        Path(__file__).resolve().parent.parent
        / "Swing_stocks"
        / "nifty200_smc_precision_screen.csv"
    )
    if top_stocks_csv.exists():
        try:
            stocks = pd.read_csv(top_stocks_csv).where(pd.notnull, None)
            if "score" in stocks.columns:
                stocks = stocks.sort_values(by="score", ascending=False)
            top_20 = stocks.head(20).to_dict(orient="records")
        except Exception as exc:
            logger.warning("Could not load top swing stocks for dashboard: %s", exc)
    context["top_20"] = top_20
    context.update(get_swing_ml_context())
    return render(request, "dashboard.html", context)


# --- Real Tick-By-Tick (TBT) Pre-Open data loader ---------------------------

# Directory containing the daily pre-open TBT CSV/TXT files.
# Anchored to the repository root (the folder that holds manage.py) via
# Path(__file__).resolve() so it resolves to <repo_root>/preopen on both Windows
# (local) and Linux (Render), regardless of the process working directory.
PREOPEN_TBT_DIR = os.getenv(
    "PREOPEN_TBT_DIR",
    str(Path(__file__).resolve().parents[2] / "preopen"),
)
PREOPEN_TBT_FILE_EXTENSIONS = (".csv", ".txt")
PREOPEN_TBT_ENCODINGS = ("utf-8-sig", "utf-8", "latin1")
PREOPEN_TBT_START = time(9, 0, 0)   # pre-open window start
PREOPEN_TBT_END = time(9, 10, 0)    # pre-open window end (IEP settlement)

# Deployment mode switch. On Render the pre-open ticks live in Supabase PostgreSQL
# (the collector bulk-inserts them into preopen_ticks), not in local CSV files — the
# container filesystem is ephemeral and shared-nothing, so the old os.listdir scan
# returned "file not found" after any restart/deploy. Locally the CSV scan is kept.
DEPLOY_ENV = os.getenv("DEPLOY_ENV", "LOCAL").strip().upper()
PREOPEN_DB_URL = os.getenv("DATABASE_URL", "").strip()
PREOPEN_DB_TIMEOUT = float(os.getenv("PREOPEN_DB_TIMEOUT", "3.0"))

_DATETIME_COLUMN_HINTS = ("datetime", "date and time", "timestamp", "date", "time")
_PRICE_COLUMN_HINTS = (
    "preopen_nifty 50", "preopen_nifty50", "nifty 50", "nifty50",
    "iep", "ltp", "last price", "last_price", "price", "close",
)

# Pre-open Redis IPC. The 09:10 publisher SETs this snapshot key and PUBLISHes to the
# matching channel; the Channels consumer bridge then fans the payload out to browsers.
PREOPEN_REDIS_DATA_KEY = os.getenv("PREOPEN_REDIS_DATA_KEY", "preopen_trap_data")
PREOPEN_REDIS_CHANNEL = os.getenv("PREOPEN_REDIS_CHANNEL", "preopen:trap_updated")
PREOPEN_REDIS_TIMEOUT = float(os.getenv("PREOPEN_REDIS_TIMEOUT", "2.0"))

_preopen_redis = None
_preopen_redis_lock = threading.Lock()


def _ist_today() -> date:
    """Return the current date in the India (IST) timezone."""
    return datetime.now(IST).date()


def _find_preopen_column(columns, hints) -> str | None:
    """Match a DataFrame column against a list of normalized hints."""
    normalized = {col: str(col).strip().lower().replace("_", " ") for col in columns}
    for hint in hints:
        for col, clean in normalized.items():
            if clean == hint:
                return col
    for hint in hints:
        for col, clean in normalized.items():
            if hint in clean:
                return col
    return None


def _read_preopen_file(file_path: str) -> pd.DataFrame | None:
    """Read a TBT file trying several common encodings; returns None if unreadable."""
    for encoding in PREOPEN_TBT_ENCODINGS:
        try:
            return pd.read_csv(file_path, encoding=encoding, sep=None, engine="python")
        except Exception:
            continue
    return None


def _fetch_preopen_from_supabase(target_date: date) -> dict | None:
    """Return Open/Max/Min/Final IEP from Supabase ``preopen_ticks`` (RENDER mode).

    One SQL aggregate over today's 09:00:00-09:10:00 ticks replaces the old filesystem
    scan. Any failure (DATABASE_URL unset, connection error, empty table) returns None
    so the dashboard degrades gracefully exactly like the old missing-CSV path did.
    """
    if not PREOPEN_DB_URL:
        logger.warning("DATABASE_URL is not set; cannot read pre-open ticks from Supabase.")
        return None

    query = """
        SELECT
            min(price) FILTER (WHERE rn = 1)     AS open_iep,
            max(price)                           AS max_iep,
            min(price)                           AS min_iep,
            min(price) FILTER (WHERE rn = total) AS final_iep
        FROM (
            SELECT price,
                   row_number() OVER (ORDER BY tick_time) AS rn,
                   count(*) OVER ()                       AS total
            FROM preopen_ticks
            WHERE trading_date = %s
              AND tick_time BETWEEN TIME '09:00:00' AND TIME '09:10:00'
        ) sub
    """

    try:
        import psycopg2
        from psycopg2.extras import RealDictCursor
    except ImportError:
        logger.warning("psycopg2 is not installed; cannot read pre-open ticks from Supabase.")
        return None

    conn = None
    try:
        conn = psycopg2.connect(
            PREOPEN_DB_URL,
            connect_timeout=int(PREOPEN_DB_TIMEOUT),
            cursor_factory=RealDictCursor,
        )
        with conn.cursor() as cur:
            cur.execute(query, (target_date,))
            row = cur.fetchone()
    except Exception as exc:
        logger.warning("Supabase pre-open query failed for %s: %s", target_date, exc)
        return None
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass

    if not row or row["open_iep"] is None or row["final_iep"] is None:
        logger.info("No pre-open ticks in Supabase for %s", target_date)
        return None

    result = {
        "Open_IEP": round(float(row["open_iep"]), 2),
        "Max_IEP": round(float(row["max_iep"]), 2),
        "Min_IEP": round(float(row["min_iep"]), 2),
        "Final_IEP": round(float(row["final_iep"]), 2),
    }
    logger.info("Computed pre-open trap data from Supabase for %s: %s", target_date, result)
    return result


def fetch_real_preopen_trap_data(target_date: date | None = None) -> dict | None:
    """Compute Max/Min/Final IEP from today's real pre-open TBT ticks.

    Two backends, selected by ``DEPLOY_ENV``:

      * ``RENDER`` — query Supabase ``preopen_ticks`` directly (durable, shared across
        services, no local filesystem involved).
      * ``LOCAL``  — the original scan of ``PREOPEN_TBT_DIR`` for today's tick file(s),
        keeping only ticks within the 09:00:00 - 09:10:00 pre-open window.

    Returns ``{'Open_IEP', 'Max_IEP', 'Min_IEP', 'Final_IEP'}``. Returns ``None`` when
    today's data is missing or unreadable so the dashboard can show a graceful fallback.
    """
    target_date = target_date or _ist_today()

    if DEPLOY_ENV == "RENDER":
        return _fetch_preopen_from_supabase(target_date)

    # ---- LOCAL: original filesystem scan (unchanged) ----
    if not os.path.isdir(PREOPEN_TBT_DIR):
        logger.warning("PREOPEN_TBT_DIR does not exist: %s", PREOPEN_TBT_DIR)
        return None
    try:
        file_names = sorted(os.listdir(PREOPEN_TBT_DIR))
    except OSError as exc:
        logger.warning("Cannot list PREOPEN_TBT_DIR %s: %s", PREOPEN_TBT_DIR, exc)
        return None

    collected = []
    for file_name in file_names:
        file_path = os.path.join(PREOPEN_TBT_DIR, file_name)
        if not os.path.isfile(file_path):
            continue
        if os.path.splitext(file_name)[1].lower() not in PREOPEN_TBT_FILE_EXTENSIONS:
            continue

        df = _read_preopen_file(file_path)
        if df is None or df.empty:
            continue

        datetime_col = _find_preopen_column(df.columns, _DATETIME_COLUMN_HINTS)
        price_col = _find_preopen_column(df.columns, _PRICE_COLUMN_HINTS)
        if datetime_col is None or price_col is None:
            continue

        ticks = pd.DataFrame({
            "ts": pd.to_datetime(df[datetime_col], errors="coerce"),
            "price": pd.to_numeric(df[price_col], errors="coerce"),
        }).dropna()

        window = ticks[
            (ticks["ts"].dt.date == target_date)
            & (ticks["ts"].dt.time >= PREOPEN_TBT_START)
            & (ticks["ts"].dt.time <= PREOPEN_TBT_END)
        ]
        if not window.empty:
            collected.append(window)

    if not collected:
        logger.info(
            "No pre-open TBT ticks found for %s in %s",
            target_date,
            PREOPEN_TBT_DIR,
        )
        return None

    all_ticks = pd.concat(collected, ignore_index=True).sort_values("ts")
    if all_ticks.empty:
        logger.info("Pre-open TBT data for %s was empty", target_date)
        return None

    result = {
        "Open_IEP": round(float(all_ticks["price"].iloc[0]), 2),
        "Max_IEP": round(float(all_ticks["price"].max()), 2),
        "Min_IEP": round(float(all_ticks["price"].min()), 2),
        "Final_IEP": round(float(all_ticks["price"].iloc[-1]), 2),
    }
    logger.info("Computed pre-open trap data for %s: %s", target_date, result)
    return result


def _round_to_strike(price: float, step: int = GAMMA_WALL_STRIKE_STEP) -> int:
    """Round ``price`` to the nearest strike on a ``step``-point ladder (round-half-up)."""
    return int(math.floor(price / step + 0.5) * step)


def _parse_feed_timestamp(value) -> date | None:
    """Parse a feed timestamp (ISO-8601 string or epoch milliseconds) into an IST date."""
    if value is None:
        return None

    ist = IST

    if isinstance(value, (int, float)):
        try:
            # Upstox ``ltt`` arrives as epoch *milliseconds*; guard the rare seconds case.
            seconds = value / 1000.0 if value > 10_000_000_000 else value
            return datetime.fromtimestamp(seconds, tz=ist).date()
        except (OverflowError, OSError, ValueError):
            return None

    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError:
            # Last resort: a bare "YYYY-MM-DD" prefix.
            try:
                return datetime.strptime(text[:10], "%Y-%m-%d").date()
            except ValueError:
                return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=ist)
        return parsed.astimezone(ist).date()

    return None


def _snapshot_is_fresh_today(payload: dict) -> bool:
    """True when the snapshot carries a data-production timestamp from today (IST).

    Uses only data-production timestamps: ``updated_at`` (when live data was produced),
    ``spot_updated_at`` (when the index tick that carries ``cp`` arrived) and the index
    tick's own ``ltt``. Deliberately ignores ``dumped_at`` (the file-write time) and
    ``restored_snapshot_updated_at`` (provenance of a stale restore), neither of which
    proves the data is from today.
    """
    today = _ist_today()

    for key in ("updated_at", "spot_updated_at"):
        if _parse_feed_timestamp(payload.get(key)) == today:
            return True

    ltt = (
        payload.get("index_raw_sample", {})
        .get("fullFeed", {})
        .get("indexFF", {})
        .get("ltpc", {})
        .get("ltt")
    )
    return _parse_feed_timestamp(ltt) == today


def _read_previous_close() -> float | None:
    """Return the NIFTY previous-session close from the live feed snapshot, if fresh.

    The feed keeps the last raw index tick under ``index_raw_sample``; the previous
    close is the ``cp`` field of ``fullFeed.indexFF.ltpc``. The value is only trusted
    when the snapshot is from today (IST) — otherwise (feed not yet initialized, or a
    stale cold JSON from an earlier session) this returns ``None`` so ``get_gamma_walls``
    derives the walls from the Final IEP instead.
    """
    try:
        payload = read_live_options_data()
        if not isinstance(payload, dict) or not _snapshot_is_fresh_today(payload):
            logger.debug("Ignoring previous close: live snapshot is not from today.")
            return None

        ltpc = (
            payload.get("index_raw_sample", {})
            .get("fullFeed", {})
            .get("indexFF", {})
            .get("ltpc", {})
        )
        cp = ltpc.get("cp") if isinstance(ltpc, dict) else None
        if isinstance(cp, (int, float)) and cp > 0:
            return float(cp)
    except Exception:
        logger.debug("Could not read the previous close from the live snapshot.", exc_info=True)
    return None


def get_gamma_walls(reference_price: float | None = None) -> tuple[int | None, int | None]:
    """Return dynamic pre-open wall strikes ``(call_wall, put_wall)``.

    The reference is the actual previous session close when the live feed provides it,
    otherwise the caller-supplied ``reference_price`` (the Final IEP). Walls are the
    reference +/- ``GAMMA_WALL_OFFSET`` rounded to the nearest NIFTY strike so they stay
    decoupled from the live intraday option chain. Returns ``(None, None)`` when no
    usable reference exists.
    """
    base = _read_previous_close()
    if base is None or base <= 0:
        base = reference_price
    if base is None or base <= 0:
        return None, None

    call_wall = _round_to_strike(base + GAMMA_WALL_OFFSET)
    put_wall = _round_to_strike(base - GAMMA_WALL_OFFSET)
    return call_wall, put_wall


# ---------------------------------------------------------------------------
# EOD FNO Participant OI -> Pre-Open Trap Decision Matrix
# ---------------------------------------------------------------------------
# The 09:00-09:08 auction has no option flow of its own, so the actionable
# trap read merges (a) the EOD participant positioning stored by
# fno_swing_engine.py in fno_swing.db with (b) the live projected open versus
# yesterday's close. The matrix yields CONFIRMED_SQUEEZE / CONFIRMED_PANIC /
# INVALIDATED / FLAT_OPEN, plus a neutral fallback when data is missing.

def _read_latest_eod_participant_bias() -> dict | None:
    """Return the latest EOD FNO participant setup from fno_swing.db, or None.

    Imported lazily so the dashboard still boots when the FNO engine (pandas,
    the ``D:`` drive database, etc.) is unavailable, and every failure degrades
    to ``None`` so the trap matrix can serve a neutral read instead of a 500.
    """
    try:
        from Nse_Project.FNO.fno_swing_engine import get_eod_participant_bias
    except Exception as exc:
        logger.warning(
            "EOD participant bias unavailable (fno_swing_engine import failed): %s",
            exc,
        )
        return None
    try:
        return get_eod_participant_bias()
    except Exception as exc:
        logger.warning("Could not read EOD participant bias from fno_swing.db: %s", exc)
        return None


def _derive_eod_bias(action_type, tomorrow_bias) -> str:
    """Classify the EOD FNO setup as bullish / bearish / neutral."""
    if action_type in {"SHORT COVERING", "LONG BUILD-UP"}:
        return "bullish"
    if action_type in {"LONG UNWINDING", "ADDING SHORTS"}:
        return "bearish"
    bias_text = (tomorrow_bias or "").upper()
    if "BULLISH" in bias_text:
        return "bullish"
    if "BEARISH" in bias_text:
        return "bearish"
    return "neutral"


def _compute_trap_decision(eod: dict | None, projected_open_pct) -> dict:
    """Merge EOD participant positioning with the projected open.

    Evaluated in the order the trade logic demands: squeeze / panic first (the
    tradeable traps), then the contradiction guard, then the flat-open guard.
    """
    if projected_open_pct is None:
        return {
            "trap_status": "WAITING",
            "trap_tone": "neutral",
            "actionable_signal": "⏳ AWAITING PRE-OPEN DATA",
            "trade_advice": "Pre-open auction + projected open resolve by ~09:08 IST.",
        }
    if eod is None:
        return {
            "trap_status": "NO_EOD_SETUP",
            "trap_tone": "neutral",
            "actionable_signal": "🟡 NO EOD POSITIONING",
            "trade_advice": "EOD FNO participant data unavailable. No trap edge computed.",
        }

    fii_net_oi = eod.get("fii_net_oi") or 0
    fii_net_vol = eod.get("fii_net_vol") or 0
    retail_trap = bool(eod.get("retail_trap"))
    eod_bias = _derive_eod_bias(eod.get("action_type"), eod.get("tomorrow_bias"))
    pct = float(projected_open_pct)

    # CASE A — FII SHORT SQUEEZE (bullish confirmation).
    if fii_net_oi < 0 and fii_net_vol > 0 and pct >= 0.3:
        return {
            "trap_status": "CONFIRMED_SQUEEZE",
            "trap_tone": "bullish",
            "actionable_signal": "🔥 FII SHORT SQUEEZE CONFIRMED",
            "trade_advice": "Pre-open gap-up traps FII Shorts! High probability CE momentum at 9:15 AM.",
        }

    # CASE B — FII LONG TRAP (bearish panic). "Retail Long with FII Heavy
    # Short" is the engine's ``retail_trap`` flag.
    if (fii_net_oi > 0 or retail_trap) and pct <= -0.3:
        return {
            "trap_status": "CONFIRMED_PANIC",
            "trap_tone": "bearish",
            "actionable_signal": "⚠️ BEARISH BREAKDOWN CONFIRMED",
            "trade_advice": "Pre-open gap-down forces long unwinding! High probability PE momentum at 9:15 AM.",
        }

    # CASE C — SETUP INVALIDATED (contradiction guard).
    if (eod_bias == "bullish" and pct <= -0.4) or (eod_bias == "bearish" and pct >= 0.4):
        return {
            "trap_status": "INVALIDATED",
            "trap_tone": "neutral",
            "actionable_signal": "🚫 TRAP SETUP INVALIDATED",
            "trade_advice": "Pre-open gap contradicts EOD positioning. Stand down / No trade zone.",
        }

    # CASE D — FLAT OPEN (no squeeze).
    if -0.2 < pct < 0.2:
        return {
            "trap_status": "FLAT_OPEN",
            "trap_tone": "flat",
            "actionable_signal": "⚖️ FLAT PRE-OPEN",
            "trade_advice": "No pre-open squeeze. Wait for initial 9:15 AM range breakout.",
        }

    # Fallback — a directional gap without a matching trap setup.
    return {
        "trap_status": "NO_SETUP",
        "trap_tone": "neutral",
        "actionable_signal": "🟡 NO TRAP EDGE",
        "trade_advice": "Pre-open move lacks a high-probability trap. Wait for 9:15 AM confirmation.",
    }


def _build_preopen_trap_fields(projected_open_pct) -> dict:
    """Return the trap matrix fields plus the raw EOD context for the payload."""
    eod = _read_latest_eod_participant_bias()
    fields = _compute_trap_decision(eod, projected_open_pct)
    fields["eod_participant"] = eod
    return fields


def _build_preopen_context() -> dict:
    """Build the PRE-OPEN card context (MAX / MIN / Final IEP + projected open).

    Options do not trade during the 09:00-09:08 pre-open session, so any Call/Put OI
    or wall read here would be built on stale, invalid data. The card is therefore
    reduced to the auction's own spot levels plus the projected open versus
    yesterday's close. Shared by the server-rendered page, the JSON poll endpoint
    and the Redis publisher so the three can never drift apart.
    """
    trap_data = fetch_real_preopen_trap_data()
    ist_now = datetime.now(IST).isoformat()

    # Yesterday's close comes from the live feed's index snapshot (cp field), and is
    # only trusted when it is from today; otherwise it stays None and the projected
    # open percentage is shown as unavailable.
    previous_close = _read_previous_close()

    def _projected_open(pct):
        if pct is None:
            return "flat", "—"
        if pct > 0:
            return "up", f"+{pct:.2f}%"
        if pct < 0:
            return "down", f"{pct:.2f}%"
        return "flat", "0.00%"

    if trap_data is None:
        return {
            'preopen_max': None,
            'preopen_min': None,
            'preopen_final': None,
            'preopen_open': None,
            'previous_close': previous_close,
            'projected_open_pct': None,
            'projected_open_display': '—',
            'projected_open_tone': 'flat',
            'updated_at': ist_now,
            **_build_preopen_trap_fields(None),
        }

    max_iep = trap_data['Max_IEP']
    min_iep = trap_data['Min_IEP']
    final_iep = trap_data['Final_IEP']
    open_iep = trap_data.get('Open_IEP', min_iep)

    projected_open_pct = None
    if previous_close and previous_close > 0:
        projected_open_pct = round(((final_iep - previous_close) / previous_close) * 100.0, 2)

    projected_open_tone, projected_open_display = _projected_open(projected_open_pct)

    return {
        'preopen_max': max_iep,
        'preopen_min': min_iep,
        'preopen_final': final_iep,
        'preopen_open': open_iep,
        'previous_close': previous_close,
        'projected_open_pct': projected_open_pct,
        'projected_open_display': projected_open_display,
        'projected_open_tone': projected_open_tone,
        'updated_at': ist_now,
        **_build_preopen_trap_fields(projected_open_pct),
    }


def _read_market_state_for_today() -> dict:
    """Return today's persisted CVD + peak sentiment (safe defaults on failure).

    The feed worker writes these under ``market_state:YYYY-MM-DD``; reading them
    here means a hard refresh renders the accumulated values immediately instead of
    starting at 0 until the next WebSocket tick arrives.
    """
    return load_market_state(get_live_options_redis())


def _deduplicate_2m_candles(candles: list) -> list:
    """Collapse duplicate 2m candles by integer ``ts`` into strictly ascending bars.

    When the same timestamp appears more than once the entry with the greatest
    ``volume`` wins (zero-volume duplicates never shadow a valid bar). The result is
    sorted ascending and holds no duplicate ``ts`` values, so ``bars[-2]`` and
    ``bars[-1]`` are always two distinct consecutive 2m candles.
    """
    if not candles:
        return []
    best: dict = {}
    for candle in candles:
        if not isinstance(candle, dict):
            continue
        ts = candle.get("ts")
        if isinstance(ts, bool) or not isinstance(ts, (int, float)):
            continue
        key = int(ts)
        try:
            vol = float(candle.get("volume"))
        except (TypeError, ValueError):
            vol = 0.0
        existing = best.get(key)
        if existing is None:
            best[key] = candle
            continue
        try:
            existing_vol = float(existing.get("volume"))
        except (TypeError, ValueError):
            existing_vol = 0.0
        if vol > existing_vol:
            best[key] = candle
    return [best[k] for k in sorted(best)]


def _read_seed_candles_2m() -> list:
    """Return the last 20 completed 2m candles (OHLC + CVD) for frontend hydration.

    Falls back cleanly to ``[]`` when Redis is unavailable, the key is absent
    (e.g. before the 09:15 open / market closed), or the payload is malformed.
    """
    client = get_live_options_redis()
    if client is None:
        return []
    try:
        raw = client.get(NIFTY_2M_SEED_CANDLES_KEY)
        if not raw:
            return []
        data = json.loads(raw)
        return _deduplicate_2m_candles(data) if isinstance(data, list) else []
    except Exception:
        return []


def live_signals_view(request):
    context = _build_preopen_context()
    # Persisted live market metrics (CVD + peak Bias/Momentum/Score). These are
    # read from the date-scoped Redis key written by the feed worker so a hard
    # refresh shows the accumulated values instead of starting at 0.
    context.update(_read_market_state_for_today())
    # Last 20 completed 2m candles (OHLC + CVD) so the frontend can rehydrate
    # closedCandles on page load and skip the 30-minute ATR/divergence warm-up.
    context["seed_candles_2m"] = _read_seed_candles_2m()
    return render(request, "live_signals.html", context)


def _get_preopen_redis():
    """Return the process-wide Redis client for the pre-open publisher, or None.

    BUG #1 FIX: Race condition — in Django's threaded request handler, two threads
    could both see _preopen_redis is None and both create clients, orphaning one
    (socket-pool leak). Now guarded by a Lock + double-check pattern.
    """
    global _preopen_redis

    if _preopen_redis is not None:
        return _preopen_redis
    if redis is None:
        return None
    with _preopen_redis_lock:
        if _preopen_redis is not None:  # double-check after acquiring the lock
            return _preopen_redis
        try:
            _preopen_redis = redis.Redis.from_url(
                REDIS_URL,
                decode_responses=True,
                socket_connect_timeout=PREOPEN_REDIS_TIMEOUT,
                socket_timeout=PREOPEN_REDIS_TIMEOUT,
                health_check_interval=30,
            )
        except Exception as exc:
            logger.warning("Could not build the pre-open Redis client: %s", exc)
            return None
    return _preopen_redis


def publish_preopen_trap_to_redis(payload: dict | None = None) -> dict:
    """SET + PUBLISH the final pre-open snapshot so the dashboard bridge fans it out.

    Builds the payload from today's TBT file when none is supplied (this is what the
    09:10 scheduler calls), so the Windows task and the poll endpoint always ship the
    same shape. Also dispatched synchronously into the Channels group when running
    inside the ASGI process, so a browser already on the page gets it without polling.
    """
    payload = payload or _build_preopen_context()

    client = _get_preopen_redis()
    if client is not None:
        try:
            raw = json.dumps(payload, default=str)
            pipe = client.pipeline(transaction=False)
            pipe.set(PREOPEN_REDIS_DATA_KEY, raw)
            pipe.publish(PREOPEN_REDIS_CHANNEL, raw)
            pipe.execute()
            logger.info("Published pre-open trap snapshot to Redis (%s).", PREOPEN_REDIS_DATA_KEY)
        except Exception as exc:
            logger.warning("Failed to publish pre-open snapshot to Redis: %s", exc)
    else:
        logger.warning("Pre-open Redis unavailable; snapshot was not published.")

    # Best-effort in-process fan-out (the consumer bridge handles the raw Redis
    # channel above; this covers the case where the scheduler runs under ASGI).
    try:
        from channels.layers import get_channel_layer
        from asgiref.sync import async_to_sync
        layer = get_channel_layer()
        if layer is not None:
            async_to_sync(layer.group_send)(
                "preopen_updates", {"type": "preopen.update", "payload": payload}
            )
    except Exception:
        logger.debug("Channels group fan-out skipped.", exc_info=True)

    return payload


def read_preopen_from_redis() -> dict | None:
    """Return the published pre-open snapshot, or None if Redis cannot serve it."""
    client = _get_preopen_redis()
    if client is None:
        return None
    try:
        raw = client.get(PREOPEN_REDIS_DATA_KEY)
    except Exception as exc:
        logger.debug("Pre-open snapshot read failed: %s", exc)
        return None
    if not raw:
        return None
    try:
        payload = json.loads(raw)
    except (TypeError, ValueError):
        return None
    return payload if isinstance(payload, dict) else None


def preopen_trap_api(request):
    """JSON endpoint polled by the dashboard to clear the WAITING state live.

    Reads the published Redis snapshot first so the poll and the WebSocket always
    agree, then falls back to a fresh on-disk computation while Redis is down or the
    snapshot has not been published yet. Any unexpected failure is returned as a JSON
    body (never an HTML 500), so the frontend poll can degrade gracefully.

    ISSUE #9 FIX: Added Cache-Control: no-store.
    """
    try:
        cached = read_preopen_from_redis()
        response = JsonResponse(cached if cached is not None else _build_preopen_context())
    except Exception as exc:
        logger.error("preopen_trap_api failed; serving a neutral fallback.", exc_info=True)
        response = JsonResponse({
            "preopen_max": None,
            "preopen_min": None,
            "preopen_final": None,
            "preopen_open": None,
            "previous_close": None,
            "projected_open_pct": None,
            "projected_open_display": "—",
            "projected_open_tone": "flat",
            "updated_at": None,
            "trap_status": "WAITING",
            "trap_tone": "neutral",
            "actionable_signal": "⏳ AWAITING PRE-OPEN DATA",
            "trade_advice": "Pre-open trap unavailable.",
            "eod_participant": None,
            "error": f"{type(exc).__name__}: {exc}",
        })
    response["Cache-Control"] = "no-store"
    return response


def get_live_options_redis():
    """Return the process-wide Redis client for the live feed, or None if unusable.

    Lazily created and then reused: building a client per request would add a TCP
    connect to every 1 Hz dashboard poll. redis-py pools connections internally and is
    thread-safe, so one module-level client is correct for Django's threaded server.

    BUG #1 FIX: Race condition — two concurrent request threads could both see
    _live_options_redis is None and both create clients, orphaning one (socket
    leak). Now guarded by a Lock + double-check.
    """
    global _live_options_redis

    if _live_options_redis is not None:
        return _live_options_redis
    if redis is None:
        return None
    with _live_options_redis_lock:
        if _live_options_redis is not None:  # double-check after acquiring the lock
            return _live_options_redis
        try:
            _live_options_redis = redis.Redis.from_url(
                REDIS_URL,
                decode_responses=True,
                socket_connect_timeout=LIVE_OPTIONS_REDIS_TIMEOUT,
                socket_timeout=LIVE_OPTIONS_REDIS_TIMEOUT,
                health_check_interval=30,
            )
        except Exception as exc:
            logger.warning("Could not build the live-options Redis client: %s", exc)
            return None
    return _live_options_redis


def _read_live_options_from_redis() -> dict | None:
    """Return the live snapshot published by the feed, or None if Redis cannot serve it.

    None means "fall back to disk" and is returned for every failure mode: redis-py
    missing, server down, key absent (feed not started yet) or an unparseable payload.
    An empty-but-valid snapshot is NOT None — that is a real answer from a live feed.
    """
    global _redis_outage_logged, _redis_retry_after

    client = get_live_options_redis()
    if client is None:
        return None

    # Circuit breaker: a recent failure means don't even try — go straight to disk.
    if std_time.monotonic() < _redis_retry_after:
        return None

    try:
        raw = client.get(LIVE_OPTIONS_REDIS_KEY)
    except Exception as exc:  # RedisError, socket timeout, DNS failure, ...
        _redis_retry_after = std_time.monotonic() + LIVE_OPTIONS_REDIS_COOLDOWN
        # Log the first outage only: this runs on every dashboard poll and would
        # otherwise flood the console at 1 Hz for as long as Redis stays down.
        if not _redis_outage_logged:
            logger.warning(
                "Redis unavailable at %s (%s); serving the cold JSON backup instead.",
                REDIS_URL, exc,
            )
            _redis_outage_logged = True
        return None

    _redis_retry_after = 0.0
    if _redis_outage_logged:
        logger.info("Redis is back; serving live option data from %s again.", LIVE_OPTIONS_REDIS_KEY)
        _redis_outage_logged = False

    if not raw:
        # Key missing/empty: the feed has not published yet this session.
        return None

    try:
        payload = json.loads(raw)
    except (TypeError, ValueError) as exc:
        logger.warning("Redis key %s held unparseable JSON (%s).", LIVE_OPTIONS_REDIS_KEY, exc)
        return None

    if not isinstance(payload, dict):
        logger.warning("Redis key %s did not hold a JSON object.", LIVE_OPTIONS_REDIS_KEY)
        return None

    payload["data_source"] = "redis"
    return payload


def _read_live_options_from_disk() -> dict:
    """Cold fallback: the 5-second snapshot the feed's dump_live_state() leaves on disk.

    Only reached while Redis is down. The feed now touches this file once every 5 s
    instead of ~32 times a second, so even this path is ~160x less likely to collide
    with the feed's os.replace() than the old per-tick behaviour was.
    """
    try:
        if not LIVE_OPTIONS_COLD_BACKUP.exists():
            logger.warning("No cold backup at %s; returning an empty snapshot.", LIVE_OPTIONS_COLD_BACKUP)
            return {}
        with LIVE_OPTIONS_COLD_BACKUP.open("r", encoding="utf-8") as fh:
            payload = json.load(fh)
    except (OSError, ValueError) as exc:
        # ValueError covers json.JSONDecodeError, which is expected here: the file can
        # be caught mid-rewrite while the feed is dumping it.
        logger.warning("Cold backup %s unreadable (%s).", LIVE_OPTIONS_COLD_BACKUP, exc)
        return {}

    if not isinstance(payload, dict):
        return {}

    payload["data_source"] = "cold_json"
    return payload


def read_live_options_data() -> dict:
    """Return the latest live option-chain snapshot.

    Redis is the live IPC channel (the feed publishes there on every tick, throttled);
    the on-disk JSON is only a cold backup for when Redis is unavailable. The returned
    dict carries a ``data_source`` key ("redis" / "cold_json") so the dashboard and the
    API response can show which path served the data.
    """
    payload = _read_live_options_from_redis()
    if payload is not None:
        return payload
    return _read_live_options_from_disk()


def get_nested(raw, path):
    if not isinstance(raw, dict):
        return None
    current = raw
    for key in path:
        if isinstance(current, dict):
            current = current.get(key)
        elif isinstance(current, list) and isinstance(key, int) and 0 <= key < len(current):
            current = current[key]
        else:
            return None
    return current


def _to_number(value):
    """Coerce a value to float, returning None for missing/unparseable/NaN/inf.

    ISSUE #3 FIX: The original returned float('nan') for NaN inputs, which
    propagated through calculations (NaN + x = NaN) and silently corrupted
    percentages and totals. Now NaN/inf are rejected at the source.
    """
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        f = float(value)
        return f if math.isfinite(f) else None
    if isinstance(value, str):
        try:
            f = float(value.replace(',', '').strip())
            return f if math.isfinite(f) else None
        except ValueError:
            return None
    return None


def _has_activity(value) -> bool:
    """True when value is a non-zero number (used to filter inactive strikes).

    ISSUE #8 FIX: Moved from inside build_option_chain_rows() to module level
    so it is not re-created on every 1Hz call.
    """
    return isinstance(value, (int, float)) and value != 0


def _json_safe_value(value):
    if value is None:
        return 0
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return 0
        return value
    if isinstance(value, int):
        return value
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value
    try:
        parsed = float(value)
        if math.isnan(parsed) or math.isinf(parsed):
            return 0
        return parsed
    except Exception:
        return 0


def build_option_chain_rows(
    instrument_data: dict,
    target_symbol: str = "NIFTY",
    strike_bounds=None,
    expiry_date=None,
) -> list:
    """Fold the feed's flat ``instrument_data`` map into option-chain rows.

    Rows are keyed by the COMPOSITE ``(expiry_date, strike_price)`` and never by the bare
    strike. With a strike-only key, an ``instrument_data`` map that carried two expiries
    (a second feed instance publishing to the same Redis key, a snapshot restored across a
    rollover, a half-finished subscription swap) merged them: the CE leg of expiry A and
    the PE leg of expiry B landed in the SAME row, so ``call_coi``/``put_coi`` were taken
    from two unrelated chains and the resulting totals were arbitrary.

    ``expiry_date`` is the snapshot's active expiry (``live_options_data.expiry_date``).
    When supplied, any instrument that PROVABLY belongs to another expiry is dropped and
    logged; entries whose expiry cannot be determined are kept and folded into the active
    bucket, so an unknown symbol format can never blank the dashboard.
    """
    if not isinstance(instrument_data, dict):
        return []

    rows: dict[tuple, dict] = {}
    excluded_symbols = ("BANKNIFTY", "FINNIFTY", "SENSEX", "MIDCPNIFTY")
    target_prefix = target_symbol.upper().replace(" ", "")
    active_expiry = normalize_expiry(expiry_date)
    foreign_expiries: dict[str, int] = {}

    # Dynamic strike window from most-active contracts; fall back to a safe NIFTY band.
    if strike_bounds and len(strike_bounds) == 2:
        lowest_strike, highest_strike = float(strike_bounds[0]), float(strike_bounds[1])
    else:
        lowest_strike, highest_strike = 20000.0, 30000.0

    for instrument_key, item in instrument_data.items():
        if not isinstance(item, dict):
            continue

        # Lock 1: Value-based symbol filter (inspect item values, not the instrument_token key)
        symbol = item.get("tradingsymbol") or item.get("underlying_name")
        normalized_symbol = symbol.strip().upper().replace(" ", "") if isinstance(symbol, str) else ""
        if not normalized_symbol or not normalized_symbol.startswith(target_prefix):
            continue
        if any(excluded in normalized_symbol for excluded in excluded_symbols):
            continue

        # Lock 2: Strict strike boundary (active min/max; fallback 20000..30000)
        strike = item.get("strike_price", item.get("strike"))
        strike = _to_number(strike)
        if strike is None or not (lowest_strike <= strike <= highest_strike):
            continue

        # Lock 3: Expiry isolation. Only instruments that provably belong to another
        # expiry are rejected, so this can never empty a chain whose entries simply do
        # not carry an expiry marker yet.
        if active_expiry and not belongs_to_expiry(item, active_expiry):
            foreign = resolve_row_expiry(item) or "unknown"
            foreign_expiries[foreign] = foreign_expiries.get(foreign, 0) + 1
            continue

        side = (item.get('option_type') or '').upper()
        raw_sample = item.get('raw_sample', {})
        raw = raw_sample if isinstance(raw_sample, dict) else {}
        if side not in {"CE", "PE"}:
            continue

        strike_key = str(int(strike) if strike.is_integer() else strike)
        # THE fix: (expiry_date, strike_price), not strike_price alone.
        row_key = (expiry_group_key(item, active_expiry), strike)
        row = rows.setdefault(row_key, {
            'strike': strike_key,

            'call_oi_pct': '-',
            'call_oi': '-',
            'call_volume': '-',
            'call_velocity': '-',
            'call_ltp': '-',
            'call_delta': '-',
            'call_coi': '-',
            'call_coi_pct': '-',
            'call_control_pct': '-',
            'call_coi_rel_pct': '-',
            'put_coi_pct': '-',
            'put_control_pct': '-',
            'put_coi_rel_pct': '-',
            'put_coi': '-',
            'put_ltp': '-',
            'put_delta': '-',
            'put_velocity': '-',
            'put_volume': '-',
            'put_oi': '-',
            'put_oi_pct': '-',
        })

        target = 'call' if side == 'CE' else 'put'
        ltp = _to_number(item.get('ltp'))
        if ltp is None:
            ltp = _to_number(get_nested(raw, ['fullFeed', 'marketFF', 'ltpc', 'ltp']))
        oi = _to_number(item.get('oi'))
        if oi is None:
            oi = _to_number(get_nested(raw, ['fullFeed', 'marketFF', 'oi']))
        volume = _to_number(item.get('volume'))
        if volume is None:
            volume = _to_number(get_nested(raw, ['fullFeed', 'marketFF', 'vtt']))
            if volume is None:
                volume = _to_number(get_nested(raw, ['fullFeed', 'marketFF', 'marketOHLC', 'ohlc', 0, 'vol']))

        coi = _to_number(item.get('coi'))
        coi_pct = _to_number(item.get('coi_pct'))
        if coi is None and isinstance(oi, (int, float)):
            # `poi` is the previous-day OI written by the live feed; `previous_oi` is
            # kept as a legacy alias for older payloads.
            prev_oi = _to_number(item.get('poi'))
            if prev_oi is None:
                prev_oi = _to_number(item.get('previous_oi'))
            # Match upstox_live_feed.py: a missing or zero previous OI yields no COI
            # (None) instead of a fabricated "0% change" or a divide-by-zero.
            if isinstance(prev_oi, (int, float)) and prev_oi != 0:
                coi = oi - prev_oi
                coi_pct = round((coi / prev_oi) * 100, 2)
            else:
                coi = None
                coi_pct = None

        # Volume Velocity (shares/sec) from the feed's rolling window tracker.
        velocity = _to_number(item.get('volume_velocity'))
        if velocity is None:
            velocity = _to_number(item.get(f'{target}_velocity'))

        row[f'{target}_ltp'] = ltp if ltp is not None else '-'
        # Live option Delta (Upstox optionGreeks) so the AI card can translate a
        # spot-level ATR stop into an option-premium SL (ATM fallback handled client-side).
        delta = _to_number(get_nested(raw, ['fullFeed', 'marketFF', 'optionGreeks', 'delta']))
        row[f'{target}_delta'] = delta if delta is not None else '-'
        row[f'{target}_oi'] = oi if oi is not None else '-'
        row[f'{target}_volume'] = volume if volume is not None else '-'
        row[f'{target}_velocity'] = velocity if velocity is not None else 0
        row[f'{target}_coi'] = coi if coi is not None else 0
        row[f'{target}_coi_pct'] = coi_pct if coi_pct is not None else 0

        call_oi = _to_number(row.get('call_oi'))
        put_oi = _to_number(row.get('put_oi'))

        if call_oi is not None and put_oi is not None:
            oi_total = call_oi + put_oi
            row['call_oi_pct'] = round((call_oi / oi_total) * 100, 1) if oi_total != 0 else 0
            row['put_oi_pct'] = round((put_oi / oi_total) * 100, 1) if oi_total != 0 else 0

        # Relative COI dominance per strike (Call vs Put). Absolute-value math keeps a
        # negative COI on either leg from ever producing negative or >100% figures.
        call_coi_val = _to_number(row.get('call_coi'))
        put_coi_val = _to_number(row.get('put_coi'))

        if call_coi_val is not None and put_coi_val is not None:
            ce_oich_abs = abs(call_coi_val)
            pe_oich_abs = abs(put_coi_val)
            coi_total = ce_oich_abs + pe_oich_abs

            row['call_coi_rel_pct'] = round((ce_oich_abs / coi_total) * 100, 1) if coi_total != 0 else 0.0
            row['put_coi_rel_pct'] = round((pe_oich_abs / coi_total) * 100, 1) if coi_total != 0 else 0.0

            # Bull vs Bear directional-flow control % (dual-metric display beneath the
            # daily COI% in the compact chain). call_coi > 0 = call OI building (bearish
            # flow); put_coi > 0 = put OI building (bullish flow); a negative COI on
            # either leg is unwinding and counts toward the OPPOSITE flow.
            ce_coi = call_coi_val or 0
            pe_coi = put_coi_val or 0
            bull_flow = max(0, pe_coi) + abs(min(0, ce_coi))
            bear_flow = max(0, ce_coi) + abs(min(0, pe_coi))
            total_flow = bull_flow + bear_flow
            if total_flow > 0:
                row['put_control_pct'] = round((bull_flow / total_flow) * 100, 1)
                row['call_control_pct'] = round((bear_flow / total_flow) * 100, 1)
            else:
                row['put_control_pct'] = 0
                row['call_control_pct'] = 0

    # ISSUE #8 FIX: Moved to module level (was re-created on every 1Hz call).
    # has_activity is now _has_activity, defined at the top of the file.

    if foreign_expiries:
        # This is the smoking gun for a second feed instance or a stale rollover: log it
        # loudly instead of silently folding the foreign chain into the active rows.
        logger.warning(
            "Dropped %d option-chain rows from non-active expiries %s (active expiry %s).",
            sum(foreign_expiries.values()), sorted(foreign_expiries), active_expiry,
        )

    # Sorted by the composite key so the output is deterministic even when more than one
    # expiry survives (possible only when the snapshot carries no active expiry at all).
    filtered = [
        row for _, row in sorted(rows.items(), key=lambda item: (item[0][0], item[0][1]))
        if _has_activity(row.get('call_oi')) or _has_activity(row.get('call_volume')) or
           _has_activity(row.get('put_oi')) or _has_activity(row.get('put_volume'))
    ]

    # Convert OI / COI share quantities into lots (NIFTY lot size = 65).
    for row in filtered:
        for key in ('call_oi', 'call_coi', 'put_oi', 'put_coi'):
            value = row.get(key)
            if isinstance(value, (int, float)):
                row[key] = round(value / LOT_SIZE)

    return filtered


def detect_deadly_combo_signals(
    options_chain_data: list,
    spot_price: float | None = None,
) -> list:
    """Scans Compact Options Chain rows for 'Deadly Combo' intraday scalping setups.

    Only strikes within the ATM proximity zone (ATM +/- 200 pts, ~4 strikes) are
    evaluated, and only massive OI additions / fast unwinding trigger a signal.
    """
    signals = []

    if not options_chain_data or not isinstance(options_chain_data, list):
        return signals

    # Find the closest ATM strike if spot_price is available.
    atm_strike = None
    if spot_price:
        try:
            # ISSUE #4 FIX: Use _round_to_strike (round-half-up) instead of Python's
            # round() (banker's rounding, round-half-to-even). The gamma walls use
            # _round_to_strike, so the ATM filter must too — otherwise the combo scan
            # could miss signals at the adjacent strike.
            atm_strike = _round_to_strike(float(spot_price), GAMMA_WALL_STRIKE_STEP)
        except (ValueError, TypeError):
            atm_strike = None

    # Scalping-optimized thresholds.
    WALL_STRENGTH = 65.0      # ATM walls are still forming, so 65% is enough
    STRONG_ADDITION = 80.0    # capture only massive OI spikes
    PANIC_UNWINDING = -25.0   # captures fast short-covering
    SCALPER_WINDOW = 200      # ATM +/- 200 points (approx 4 strikes)

    for row in options_chain_data:
        strike = _to_number(row.get('strike')) or 0

        # Apply ATM filter: ignore far OTM / deep ITM strikes.
        if atm_strike and abs(strike - atm_strike) > SCALPER_WINDOW:
            continue

        call_rel = _to_number(row.get('call_coi_rel_pct')) or 0
        call_chg = _to_number(row.get('call_coi_pct')) or 0
        put_rel = _to_number(row.get('put_coi_rel_pct')) or 0
        put_chg = _to_number(row.get('put_coi_pct')) or 0

        # Rule 1: CE JACKPOT BREAKOUT (Call Resistance Breaking)
        if call_rel >= WALL_STRENGTH and call_chg <= PANIC_UNWINDING:
            signals.append({"type": "CE_JACKPOT", "strike": strike, "msg": f"🚀 [CE BREAKOUT] {strike}"})

        # Rule 2: PE REVERSAL (Call Resistance Holding)
        elif call_rel >= WALL_STRENGTH and call_chg >= STRONG_ADDITION:
            signals.append({"type": "PE_REVERSAL", "strike": strike, "msg": f"🔻 [PE REVERSAL] {strike}"})

        # Rule 3: PE JACKPOT BREAKOUT (Put Support Breaking)
        if put_rel >= WALL_STRENGTH and put_chg <= PANIC_UNWINDING:
            signals.append({"type": "PE_JACKPOT", "strike": strike, "msg": f"🩸 [PE BREAKOUT] {strike}"})

        # Rule 4: CE REVERSAL (Put Support Holding)
        elif put_rel >= WALL_STRENGTH and put_chg >= STRONG_ADDITION:
            signals.append({"type": "CE_REVERSAL", "strike": strike, "msg": f"🟢 [CE REVERSAL] {strike}"})

    return signals


def _json_safe_row(row: dict) -> dict:
    return {key: _json_safe_value(value) for key, value in row.items()}


@csrf_exempt
def log_sniper_api(request):
    """Log a VWAP Sniper C2-A trigger forwarded from the browser (POST JSON)."""
    if request.method != "POST":
        return JsonResponse({"ok": False, "error": "POST required"}, status=405)
    try:
        payload = json.loads(request.body or b"{}")
    except (ValueError, TypeError):
        return JsonResponse({"ok": False, "error": "invalid JSON"}, status=400)
    if not isinstance(payload, dict):
        return JsonResponse({"ok": False, "error": "payload must be an object"}, status=400)
    payload["source"] = "LIVE_UI_TRIGGER"
    try:
        from Nse_Project.Live_dashboard.sniper_logger import log_sniper_c2a_trigger
        logged = bool(log_sniper_c2a_trigger(payload))
    except Exception as exc:
        logger.error("log_sniper_api failed: %s", exc, exc_info=True)
        return JsonResponse({"ok": False, "error": f"{type(exc).__name__}: {exc}"}, status=500)
    return JsonResponse({"ok": True, "logged": logged})


@csrf_exempt
def log_sniper_ce_api(request):
    """Log a VWAP Reclaim Sniper (CE · C2-PB) trigger forwarded from the browser."""
    if request.method != "POST":
        return JsonResponse({"ok": False, "error": "POST required"}, status=405)
    try:
        payload = json.loads(request.body or b"{}")
    except (ValueError, TypeError):
        return JsonResponse({"ok": False, "error": "invalid JSON"}, status=400)
    if not isinstance(payload, dict):
        return JsonResponse({"ok": False, "error": "payload must be an object"}, status=400)
    payload["source"] = "LIVE_UI_TRIGGER"
    try:
        from Nse_Project.Live_dashboard.sniper_logger import log_sniper_ce_pb_trigger
        logged = bool(log_sniper_ce_pb_trigger(payload))
    except Exception as exc:
        logger.error("log_sniper_ce_api failed: %s", exc, exc_info=True)
        return JsonResponse({"ok": False, "error": f"{type(exc).__name__}: {exc}"}, status=500)
    return JsonResponse({"ok": True, "logged": logged})


def live_option_chain_api(request):
    """Serve the live option chain, and NEVER answer a 1 Hz poll with an HTML 500.

    dashboard.html does ``if (!response.ok) throw`` and then renders
    "Unable to load live option data." - so an unhandled exception here costs the whole
    chain AND leaves no diagnostic anywhere. Any failure is therefore logged with a full
    traceback and returned as a normal JSON body carrying an ``error`` field, with every
    key the frontend expects still present.

    ISSUE #9 FIX: Added Cache-Control: no-store so a CDN/proxy between the browser
    and Django can never serve a stale 1Hz-poll response.
    """
    try:
        response = JsonResponse(_build_live_option_chain_payload())
        response["Cache-Control"] = "no-store"
        return response
    except Exception as exc:
        logger.error(
            "live_option_chain_api failed; serving an empty chain with the error attached.",
            exc_info=True,
        )
        # BUG #2 FIX: String fields use "" (not 0) so the frontend doesn't crash
        # on type mismatches (e.g., data.updated_at.substring(0, 10) on an int).
        response = JsonResponse({
            "data_source": "error",
            "updated_at": "",
            "expiry_date": "",
            "spot_price": 0,
            "spot_source": "",
            "spot_updated_at": "",
            "poi_source": "",
            "poi_updated_at": "",
            "poi_instruments": 0,
            "feed_connected": False,
            "feed_status": "api_error",
            "stale": True,
            "seconds_since_last_tick": 0,
            "bias": 0,
            "momentum": 0,
            "score": 50,
            "cvd": 0,
            "ofi_imbalance": 0,
            "rel_vol_ratio": 1.0,
            "cvd_change_3": 0,
            "current_atr": 0,
            "current_atr_ce": 0,
            "current_atr_pe": 0,
            "delta_wt_impact": 0,
            "gamma_boost_val": 0,
            "iv_skew_est": 0,
            "acceleration": 0,
            "net_gex": 0,
            "net_gex_ratio": 0,
            "net_vanna": 0,
            "net_charm": 0,
            "gamma_flip_level": 0,
            "theta_decay_per_hour": 0,
            "net_oi_bias": 0,
            "total_vol_skew": 0,
            "total_pcr": 1.0,
            "vwap": 0,
            "future_ltp": 0,
            "strike_bounds": None,
            "option_chain": [],
            "deadly_combo_signals": [],
            "error": f"{type(exc).__name__}: {exc}",
        })
        response["Cache-Control"] = "no-store"
        return response


def _build_live_option_chain_payload() -> dict:
    data = read_live_options_data()
    strike_bounds = get_active_strike_bounds()
    # The snapshot's own expiry is passed in so rows are keyed by
    # (expiry_date, strike_price) and any instrument belonging to a DIFFERENT expiry is
    # dropped instead of being merged into an active-expiry row.
    active_expiry = data.get("expiry_date")
    chain = build_option_chain_rows(
        data.get("instrument_data", {}),
        target_symbol="NIFTY",
        strike_bounds=strike_bounds,
        expiry_date=active_expiry,
    )

    # Live index spot. This is what the dashboard polls for the SPOT readout, and it
    # anchors the Deadly Combo ATM-proximity filter. Previously it was written to
    # live_options_data.json by the feed but never forwarded here, which left the UI
    # depending on the much slower /gamma/status/ round-trip and looking frozen.
    spot_price = None
    for key in ("spot_price", "underlying_spot", "underlying_ltp", "index_ltp", "nifty_ltp"):
        candidate = _to_number(data.get(key))
        if isinstance(candidate, (int, float)) and candidate > 0:
            spot_price = candidate
            break

    # "Deadly Combo" anomaly scan — captured on the finalized compact rows, filtered
    # to the ATM +/- 4-strike zone, and surfaced to the frontend under
    # `deadly_combo_signals` (root-level, additive).
    combo_signals = detect_deadly_combo_signals(chain, spot_price=spot_price)

    chain = [_json_safe_row(row) for row in chain]

    # Advanced-quant diagnostics + whole-chain macro metrics, persisted by the feed
    # worker to the date-scoped Redis market_state key and read here so they refresh
    # at the same 1 Hz cadence as the option chain itself.
    market_state = _read_market_state_for_today()

    return {
        # Which IPC path served this response: "redis" (live) or "cold_json" (fallback).
        "data_source": _json_safe_value(data.get("data_source")),
        "updated_at": _json_safe_value(data.get("updated_at")),
        "expiry_date": _json_safe_value(data.get("expiry_date")),
        "spot_price": _json_safe_value(spot_price),
        "spot_source": _json_safe_value(data.get("spot_source")),
        "spot_updated_at": _json_safe_value(data.get("spot_updated_at")),
        # Previous-OI provenance (replaces the retired historical-baseline fields).
        "poi_source": _json_safe_value(data.get("poi_source")),
        "poi_updated_at": _json_safe_value(data.get("poi_updated_at")),
        "poi_instruments": _json_safe_value(data.get("poi_instruments")),
        # Feed liveness, published by the producer. `stale` is True when the websocket is
        # down or has not delivered a tick recently, which is exactly when `updated_at`
        # must NOT be read as "this data is current".
        "feed_connected": bool(data.get("feed_connected")),
        "feed_status": _json_safe_value(data.get("feed_status")),
        "stale": bool(data.get("stale", False)),
        "seconds_since_last_tick": _json_safe_value(data.get("seconds_since_last_tick")),
        # Advanced-quant diagnostics (Step 2) — refreshed on every 1 Hz poll.
        "delta_wt_impact": _json_safe_value(market_state.get("delta_wt_impact")),
        "gamma_boost_val": _json_safe_value(market_state.get("gamma_boost_val")),
        "iv_skew_est": _json_safe_value(market_state.get("iv_skew_est")),
        "acceleration": _json_safe_value(market_state.get("acceleration")),
        # Authoritative God-Mode sentiment — the backend is the single source of truth;
        # the frontend must NOT recompute bias/momentum/score locally.
        "bias": _json_safe_value(market_state.get("bias")),
        "momentum": _json_safe_value(market_state.get("momentum")),
        "score": _json_safe_value(market_state.get("score")),
        # Live cumulative CVD (index-level), bridged from the CVD engine so the AI
        # card's macro bearish confirmation reads it straight from this 1 Hz payload.
        "cvd": _json_safe_value(market_state.get("cumulative_cvd")),
        # VWAP BREAKDOWN SNIPER (PE · C2-A): scale-free order-flow / volume / 3-bar
        # CVD divergence telemetry, computed by the CVD engine on each 2m candle.
        "ofi_imbalance": _json_safe_value(market_state.get("ofi_imbalance")),
        "rel_vol_ratio": _json_safe_value(market_state.get("rel_vol_ratio")),
        "cvd_change_3": _json_safe_value(market_state.get("cvd_change_3")),
        # AI Trade Setups: live 1-minute ATR entry-zone buffer (₹). The PRICE field
        # renders [entry - ATR] - [entry + ATR] from these.
        "current_atr": _json_safe_value(market_state.get("current_atr")),
        "current_atr_ce": _json_safe_value(market_state.get("current_atr_ce")),
        "current_atr_pe": _json_safe_value(market_state.get("current_atr_pe")),
        "net_gex": _json_safe_value(market_state.get("net_gex")),
        "net_gex_ratio": _json_safe_value(market_state.get("net_gex_ratio")),
        # Advanced Greek Blend inputs (Vanna / Charm / Gamma Flip level) driving the score.
        "net_vanna": _json_safe_value(market_state.get("net_vanna")),
        "net_charm": _json_safe_value(market_state.get("net_charm")),
        "gamma_flip_level": _json_safe_value(market_state.get("gamma_flip_level")),
        "theta_decay_per_hour": _json_safe_value(market_state.get("theta_decay_per_hour")),
        # Whole-chain macro metrics for the left "TOTAL OPTION CHAIN (MACRO)" panel —
        # computed and persisted by the feed worker (see upstox_live_feed).
        "net_oi_bias": _json_safe_value(market_state.get("net_oi_bias")),
        "total_vol_skew": _json_safe_value(market_state.get("total_vol_skew")),
        "total_pcr": _json_safe_value(market_state.get("total_pcr")),
        # Real volume-weighted VWAP (₹) from the NIFTY index future (see market_state).
        # cards/vwap_signal.html reads `data.vwap` and switches to FEED mode automatically.
        "vwap": _json_safe_value(market_state.get("vwap")),
        # Live future LTP pairs with `vwap` so the VWAP card compares future price
        # against future VWAP (not the cash index against the futures VWAP).
        "future_ltp": _json_safe_value(market_state.get("future_ltp")),
        "strike_bounds": list(strike_bounds) if strike_bounds else None,
        "option_chain": chain,
        # "Deadly Combo" anomaly signals for the live dashboard (root-level, additive).
        "deadly_combo_signals": combo_signals,
        "error": None,
    }


# ---------------------------------------------------------------------------
# Upstox OAuth login flow (browser-based, Render-friendly)
# ---------------------------------------------------------------------------
# Fully independent of the existing local/manual Upstox token scripts. It reads
# credentials from environment variables so nothing is hard-coded in the repo.
def upstox_login(request):
    """Redirect the browser to Upstox's OAuth authorization dialog."""
    api_key = os.getenv("UPSTOX_API_KEY", "")
    redirect_uri = os.getenv("UPSTOX_REDIRECT_URI", "")
    auth_url = (
        "https://api.upstox.com/v2/login/authorization/dialog"
        f"?response_type=code&client_id={api_key}&redirect_uri={redirect_uri}"
    )
    return redirect(auth_url)


def upstox_callback(request):
    """Exchange the OAuth authorization ``code`` for an Upstox access token.

    For now this only surfaces the first 15 characters of the token as a simple
    success message; token persistence/reuse can be layered on later without
    touching the existing manual token scripts.
    """
    code = request.GET.get("code")
    if not code:
        return HttpResponse(
            "Upstox OAuth error: no authorization code was returned.", status=400
        )

    api_key = os.getenv("UPSTOX_API_KEY", "")
    api_secret = os.getenv("UPSTOX_API_SECRET", "")
    redirect_uri = os.getenv("UPSTOX_REDIRECT_URI", "")

    token_url = "https://api.upstox.com/v2/login/authorization/token"
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Accept": "application/json",
    }
    data = {
        "code": code,
        "client_id": api_key,
        "client_secret": api_secret,
        "redirect_uri": redirect_uri,
        "grant_type": "authorization_code",
    }

    try:
        response = requests.post(token_url, headers=headers, data=data, timeout=30)
        response.raise_for_status()
        payload = response.json()
    except Exception as exc:
        return HttpResponse(f"Upstox token exchange failed: {exc}", status=502)

    access_token = payload.get("access_token")
    if not access_token:
        detail = (
            payload.get("error_description")
            or payload.get("error")
            or response.text
        )
        return HttpResponse(f"Upstox token exchange failed: {detail}", status=502)

    token_preview = access_token[:15]
    return HttpResponse(
        f"✅ Upstox OAuth succeeded. Access token preview (first 15 chars): {token_preview}"
    )

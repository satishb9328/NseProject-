"""
NSE PRE-OPEN data fetcher (direct API, no headless browser).

The NSE pre-open chart API returns the tick-by-tick indicative price series for
NIFTY 50 in the ``grapthData`` field (a list of ``[epoch_ms, price]`` pairs).
This script:

  1. warms up a ``curl_cffi.requests.Session(impersonate="chrome")`` against the
     NSE homepage so the API call spoofs the browser TLS/JA3 fingerprint and carries
     the same cookies NSE issues to a real browser (bypasses the bot wall),
  2. calls the pre-open chart endpoint,
  3. converts the series into a ``datetime,pre open nifty 50`` CSV that both
     ``import_preopen_to_db.py`` and ``Live_dashboard.views`` can consume.

It raises on ANY failure and returns non-zero from the CLI — it never silently
"SUCCEEDS" when the download fails.
"""

import csv
import os
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

try:
    from curl_cffi import requests
except ImportError as exc:  # pragma: no cover - only hit if curl_cffi isn't installed
    raise ImportError(
        "curl_cffi is required to bypass NSE's bot protection. "
        "Install it with: pip install curl_cffi"
    ) from exc


# Keep this in sync with Live_dashboard.views.PREOPEN_TBT_DIR and
# import_preopen_to_db.PREOPEN_CSV_DIR so the backend always reads where we write.
# Anchored to the repository root via __file__ so it resolves to <repo_root>/preopen
# on Windows (local) and Linux (Render), regardless of the working directory.
SAVE_DIR = os.getenv("PREOPEN_TBT_DIR", str(Path(__file__).resolve().parents[2] / "preopen"))
CSV_NAME = "nifty50_graph_data.csv"
# Dated history copies are kept in this subfolder so the main folder stays clean
# (only the active file) while every day's data is retained.
ARCHIVE_SUBDIR = "archive"

IST = timezone(timedelta(hours=5, minutes=30))

# Endpoints tried in order. The first is NSE's dedicated pre-open chart URL; the
# second is the generic index-chart URL with the pre-open flag as a fallback.
API_URLS = [
    "https://www.nseindia.com/api/chart-databyindex-preopen",
    "https://www.nseindia.com/api/chart-databyindex?index=NIFTY%2050&preopen=true",
]

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "DNT": "1",
    "Upgrade-Insecure-Requests": "1",
    "Cache-Control": "max-age=0",
}

# Network resilience: per-endpoint attempts with linear backoff, and HTTP timeout.
MAX_ATTEMPTS = 3
RETRY_BACKOFF_SECONDS = 2.0
HTTP_TIMEOUT = 25


def _build_session() -> requests.Session:
    """Create a browser-impersonating session and prime it with NSE's cookies."""
    # Spoof the Chrome TLS/JA3 + HTTP/2 fingerprint so NSE's WAF serves JSON
    # instead of an HTML challenge/block page.
    session = requests.Session(impersonate="chrome")
    session.headers.update(HEADERS)
    # NSE issues anti-bot cookies (nsit, nseappid, ak_bmsc, ...) on the homepage.
    # Hit it first and keep the cookie jar so the API call looks like the SPA.
    # The warm-up is best-effort: if it fails we still attempt the API call and
    # let _get_json's retry/error handling surface any real problem.
    try:
        session.get("https://www.nseindia.com", timeout=HTTP_TIMEOUT)
    except requests.exceptions.RequestException as exc:
        print(f"WARNING: NSE homepage warm-up failed (continuing anyway): {exc}")
    return session


# Process-wide persistent session (created once, reused for every request) so
# curl_cffi's connection pool and warm cookie jar survive across calls instead of
# re-doing the TCP/TLS handshake each time.
_SESSION = None


def _get_session() -> requests.Session:
    """Return the persistent Session, creating it lazily on first use."""
    global _SESSION
    if _SESSION is None:
        _SESSION = _build_session()
    return _SESSION


def _get_json(session, url):
    """GET ``url`` and return the parsed JSON payload.

    Retries transient failures (connection errors, timeouts, 5xx) with linear
    backoff. 4xx client errors are treated as permanent and re-raised immediately.
    A WAF block page raises ``RuntimeError`` so the caller can try the next URL.
    """
    last_exc = None
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            session.headers["Referer"] = "https://www.nseindia.com/"
            resp = session.get(url, timeout=HTTP_TIMEOUT)
            resp.raise_for_status()  # 4xx/5xx -> requests.HTTPError

            # NSE's WAF sometimes answers with an HTML block page (HTTP 200).
            # Detect it explicitly so the failure message is actionable.
            ctype = (resp.headers.get("Content-Type") or "").lower()
            if "json" not in ctype and resp.text.lstrip().startswith("<"):
                raise RuntimeError(
                    f"blocked by WAF (HTML page, HTTP {resp.status_code}, "
                    f"first 120 chars: {resp.text[:120]!r})"
                )

            return resp.json()
        except requests.exceptions.HTTPError as exc:
            status = getattr(getattr(exc, "response", None), "status_code", None)
            if status is not None and 400 <= status < 500:
                raise  # permanent client error — retrying won't help
            last_exc = exc
        except (requests.exceptions.RequestException, ValueError) as exc:
            # ConnectionError / Timeout / JSON decode failure, etc.
            last_exc = exc

        if attempt < MAX_ATTEMPTS:
            time.sleep(RETRY_BACKOFF_SECONDS * attempt)

    raise RuntimeError(f"request failed after {MAX_ATTEMPTS} attempts: {last_exc}")


def _fetch_graph():
    """Return the tick list from the first endpoint that returns valid data."""
    session = _get_session()
    last_error = None

    for url in API_URLS:
        try:
            payload = _get_json(session, url)
            graph = payload.get("grapthData") or payload.get("graphData")
            if isinstance(graph, list) and graph:
                return graph
            last_error = ValueError("empty/missing grapthData in API response")
        except Exception as exc:  # noqa: BLE001 - try every endpoint before failing
            last_error = exc
            continue

    raise RuntimeError(f"All NSE pre-open API endpoints failed: {last_error}")


def _iter_csv_rows(graph):
    """Yield ``(datetime_str, price)`` CSV rows, skipping malformed ticks.

    A generator so the tick series is streamed straight to disk without
    materialising a large intermediate list in memory.
    """
    for item in graph:
        try:
            ts, price = item[0], item[1]
        except (IndexError, TypeError):
            continue
        try:
            ts = float(ts)
            price = float(price)
        except (TypeError, ValueError):
            continue
        # NSE encodes the IST wall-clock time as if it were a UTC epoch timestamp
        # (09:00 IST is stored as the epoch ms for "09:00 UTC"). Read it as UTC and
        # then re-label the wall clock as IST, so the CSV carries the correct
        # 09:00-09:10 IST window that Live_dashboard.views filters on.
        dt_ist = datetime.fromtimestamp(ts / 1000.0, timezone.utc).replace(tzinfo=IST)
        yield dt_ist.strftime("%Y-%m-%d %H:%M:%S"), round(price, 2)


def _write_csv(path, rows):
    """Write ``rows`` (an iterable of ``(datetime_str, price)`` tuples) to ``path``."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(["datetime", "pre open nifty 50"])
        writer.writerows(rows)


def download_graph_csv():
    """Download the pre-open tick series and persist it without data loss.

    Two files are written so a later run never destroys an earlier day's data:

      * ``nifty50_graph_data.csv``                    — ACTIVE file the importer and
        dashboard read (overwritten daily with today's ticks).
      * ``archive/nifty50_graph_data_YYYY-MM-DD.csv`` — immutable, dated history copy.

    Returns the active CSV path on success. Raises on failure so the caller halts
    immediately instead of reporting a false SUCCESS.
    """
    os.makedirs(SAVE_DIR, exist_ok=True)
    archive_dir = os.path.join(SAVE_DIR, ARCHIVE_SUBDIR)
    os.makedirs(archive_dir, exist_ok=True)

    print("Fetching NSE pre-open chart data via API...")
    graph = _fetch_graph()

    # First pass: derive metadata (count + first/last datetime) without buffering.
    first_dt_str = last_dt_str = None
    count = 0
    for dt_str, _price in _iter_csv_rows(graph):
        if first_dt_str is None:
            first_dt_str = dt_str
        last_dt_str = dt_str
        count += 1

    if count == 0:
        raise RuntimeError("Pre-open API returned no usable ticks")

    # The first tick's IST date is the authoritative trading date for this snapshot.
    date_str = first_dt_str[:10]  # YYYY-MM-DD

    active_path = os.path.join(SAVE_DIR, CSV_NAME)
    archive_path = os.path.join(archive_dir, f"nifty50_graph_data_{date_str}.csv")

    # Archive first so history is preserved even if the active-file write fails.
    # Each write streams from a fresh generator — no large in-memory list.
    _write_csv(archive_path, _iter_csv_rows(graph))
    _write_csv(active_path, _iter_csv_rows(graph))

    print(f"Success! Active file saved at: {active_path} ({count} ticks)")
    print(f"         Archive copy saved at: {archive_path}")
    print(f"  date range: {first_dt_str}  ->  {last_dt_str}")
    return active_path


if __name__ == "__main__":
    try:
        download_graph_csv()
    except Exception as exc:
        print(f"FAILED: {exc}")
        raise SystemExit(1)

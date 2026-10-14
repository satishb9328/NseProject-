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

from datetime import datetime, timezone, timedelta

try:
    from curl_cffi import requests
except ImportError as exc:  # pragma: no cover - only hit if curl_cffi isn't installed
    raise ImportError(
        "curl_cffi is required to bypass NSE's bot protection. "
        "Install it with: pip install curl_cffi"
    ) from exc


# Keep this in sync with Live_dashboard.views.PREOPEN_TBT_DIR and
# import_preopen_to_db.PREOPEN_CSV_DIR so the backend always reads where we write.
SAVE_DIR = os.getenv("PREOPEN_TBT_DIR", os.path.join(os.getcwd(), "preopen"))
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


def _build_session() -> requests.Session:
    """Create a browser-impersonating session and prime it with NSE's cookies."""
    # Spoof the Chrome TLS/JA3 + HTTP/2 fingerprint so NSE's WAF serves JSON
    # instead of an HTML challenge/block page.
    session = requests.Session(impersonate="chrome")
    session.headers.update(HEADERS)
    # NSE issues anti-bot cookies (nsit, nseappid, ak_bmsc, ...) on the homepage.
    # Hit it first and keep the cookie jar so the API call looks like the SPA.
    session.get("https://www.nseindia.com", timeout=15)
    return session


def _fetch_graph():
    """Return ``(payload, graph)`` from the first endpoint that returns valid data."""
    session = _build_session()
    last_error = None

    for url in API_URLS:
        try:
            session.headers["Referer"] = "https://www.nseindia.com/"
            resp = session.get(url, timeout=25)
            resp.raise_for_status()

            # NSE's WAF sometimes answers with an HTML block page (HTTP 200).
            # Detect it explicitly so the failure message is actionable.
            ctype = (resp.headers.get("Content-Type") or "").lower()
            if "json" not in ctype and resp.text.lstrip().startswith("<"):
                last_error = RuntimeError(
                    f"blocked by WAF (HTML page, HTTP {resp.status_code}, "
                    f"first 120 chars: {resp.text[:120]!r})"
                )
                continue

            payload = resp.json()
            graph = payload.get("grapthData") or payload.get("graphData")
            if isinstance(graph, list) and graph:
                return payload, graph
            last_error = ValueError("empty/missing grapthData in API response")
        except Exception as exc:  # noqa: BLE001 - try every endpoint before failing
            last_error = exc
            continue

    raise RuntimeError(f"All NSE pre-open API endpoints failed: {last_error}")


def _write_csv(path, rows):
    """Write ``rows`` (a list of ``(datetime_str, price)`` tuples) to ``path``."""
    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(["datetime", "pre open nifty 50"])
        writer.writerows(rows)


def download_graph_csv():
    """Download the pre-open tick series and persist it without data loss.

    Two files are written so a later run never destroys an earlier day's data:

      * ``nifty50_graph_data.csv``                   — ACTIVE file the importer and
        dashboard read (overwritten daily with today's ticks).
      * ``archive/nifty50_graph_data_YYYY-MM-DD.csv`` — immutable, dated history copy.

    Returns the active CSV path on success. Raises on failure so the caller halts
    immediately instead of reporting a false SUCCESS.
    """
    os.makedirs(SAVE_DIR, exist_ok=True)
    archive_dir = os.path.join(SAVE_DIR, ARCHIVE_SUBDIR)
    os.makedirs(archive_dir, exist_ok=True)

    print("Fetching NSE pre-open chart data via API...")
    payload, graph = _fetch_graph()

    rows = []
    for item in graph:
        try:
            ts, price = item[0], item[1]
        except (IndexError, TypeError):
            continue
        # NSE encodes the IST wall-clock time as if it were a UTC epoch timestamp
        # (09:00 IST is stored as the epoch ms for "09:00 UTC"). Read it as UTC and
        # then re-label the wall clock as IST, so the CSV carries the correct
        # 09:00-09:10 IST window that Live_dashboard.views filters on.
        dt_ist = datetime.fromtimestamp(ts / 1000.0, timezone.utc).replace(tzinfo=IST)
        rows.append((dt_ist.strftime("%Y-%m-%d %H:%M:%S"), round(float(price), 2)))

    if not rows:
        raise RuntimeError("Pre-open API returned no usable ticks")

    # The first tick's IST date is the authoritative trading date for this snapshot.
    date_str = rows[0][0][:10]  # YYYY-MM-DD

    active_path = os.path.join(SAVE_DIR, CSV_NAME)
    archive_path = os.path.join(archive_dir, f"nifty50_graph_data_{date_str}.csv")

    # Archive first so history is preserved even if the active-file write fails.
    _write_csv(archive_path, rows)
    _write_csv(active_path, rows)

    print(f"Success! Active file saved at: {active_path} ({len(rows)} ticks)")
    print(f"         Archive copy saved at: {archive_path}")
    print(f"  date range: {rows[0][0]}  ->  {rows[-1][0]}")
    return active_path


if __name__ == "__main__":
    try:
        download_graph_csv()
    except Exception as exc:
        print(f"FAILED: {exc}")
        raise SystemExit(1)
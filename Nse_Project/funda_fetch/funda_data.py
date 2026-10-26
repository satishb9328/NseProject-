"""
funda_data.py v2 (A+) — Stage 1 of the swing pipeline: Screener.in fundamentals.

FUNNEL  : 500 stocks -> quality/growth screen -> nifty500_swing_candidates.csv
SECTORS : Financial Services gated on ROE+Profit (D/E meaningless for banks);
          everything else on Sales+Profit+ROE+D/E — explicit, in CONFIG.
CACHE   : fundamentals_cache/<symbol>.json (TTL 20d) — quarterly data doesn't
          change daily; reruns cost ~0 requests and resume after crashes.
OUTPUT  : nifty500_swing_candidates.csv  (validator-compatible columns)
          fetch_failures.csv             (every failed symbol + reason)
          candidate_pool_history.csv     (date, universe, fetched, matched — append)

RUN   : python funda_data.py
        python funda_data.py --symbols RELIANCE,TCS,HDFCBANK --refresh
        python funda_data.py --limit 20
NEEDS : pip install requests beautifulsoup4 pandas
"""
import argparse
import json
import logging
import os
import random
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, asdict, field
from datetime import date
from typing import Any, Dict, List, Optional
from urllib.parse import quote

logger = logging.getLogger(__name__)

import pandas as pd
import requests
from bs4 import BeautifulSoup

pd.set_option("display.max_columns", None)
pd.set_option("display.width", 1000)

# ═══════════════════════════════ CONFIG ════════════════════════════════════════
SCREENER_BASE = "https://www.screener.in/company"
HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                   "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    # ISSUE #5 FIX: screener.in may 403 requests without a Referer.
    "Referer": "https://www.screener.in/",
}

UNIVERSE_CSV   = "ind_nifty500list.csv"
OUT_CSV        = "nifty500_swing_candidates.csv"      # validator contract — keep name
PARTIAL_CSV    = "nifty500_swing_candidates_PARTIAL.csv"
FAILURES_CSV   = "fetch_failures.csv"
HISTORY_CSV    = "candidate_pool_history.csv"
CACHE_DIR      = "fundamentals_cache"
CACHE_TTL_DAYS = float(os.getenv("FUND_CACHE_TTL_DAYS", "20"))         # fundamentals are quarterly

# Screening gates (fail-safe defaults: missing data => reject, never accept).
# ISSUE #10 FIX: all thresholds are now env-overridable for tuning without code changes.
SALES_GROWTH_MIN  = float(os.getenv("FUND_SALES_GROWTH_MIN", "10.0"))
PROFIT_GROWTH_MIN = float(os.getenv("FUND_PROFIT_GROWTH_MIN", "15.0"))
ROE_MIN           = float(os.getenv("FUND_ROE_MIN", "10.0"))
DE_MAX            = float(os.getenv("FUND_DE_MAX", "1.0"))
# ISSUE #4 FIX: Turnaround sentinel is now 999.0 (kept for JSON serialization
# compatibility — float('inf') is not valid JSON). The downstream validator
# should check the `turnaround` flag (True/False) before interpreting this value.
TURNAROUND_SENTINEL = 999.0                           # loss -> profit marker

FINANCIAL_INDUSTRY = "Financial Services"

REQUEST_TIMEOUT  = float(os.getenv("FUND_REQUEST_TIMEOUT", "15"))
MAX_RETRIES      = int(os.getenv("FUND_MAX_RETRIES", "3"))
BACKOFF_429      = [20, 45, 90]                       # escalating cooldowns
SLEEP_RANGE      = (1.2, 2.5)                         # politeness jitter before a fetch
MAX_WORKERS      = int(os.getenv("FUND_MAX_WORKERS", "10"))               # concurrent fetch threads
INCREMENT_EVERY  = int(os.getenv("FUND_INCREMENT_EVERY", "25"))            # crash-safe partial saves


@dataclass
class StockProfile:
    symbol: str
    name: str
    industry: str = ""
    current_price: float = 0.0
    market_cap: float = 0.0
    pe_ratio: float = 0.0
    pb_ratio: float = 0.0
    roe: float = 0.0
    roce: float = 0.0
    debt_to_equity: float = float("nan")              # NaN = unknown => FAILS gate
    eps_ttm: float = 0.0
    book_value: float = 0.0
    qtr_sales_growth: float = 0.0
    qtr_profit_growth: float = 0.0
    opm: float = 0.0
    opm_trend: float = float("nan")                   # OPM now minus OPM year-ago
    sales_accel: float = float("nan")                 # latest YoY minus prev-Q YoY
    peg_ratio: float = 0.0                            # PE / YoY-qtr-profit (proxy)
    turnaround: bool = False
    financial: bool = False
    data_date: str = field(default_factory=lambda: date.today().isoformat())


def _num(text: str) -> Optional[float]:
    """Coerce a scraped metric to float, tolerating N/A, commas, NBSP and
    accounting-style parentheses for negatives."""
    if not text:
        return None
    text = text.replace("\xa0", "").replace(",", "")
    negative = text.strip().startswith("(") and text.strip().endswith(")")
    m = re.search(r"-?\d+(?:\.\d+)?", text)
    if not m:
        return None
    try:
        value = float(m.group())
    except (TypeError, ValueError):
        return None
    return -value if negative else value


def _match_num(value: str) -> Optional[float]:
    return _num(value.replace("%", ""))


class IndianStockScreener:
    def __init__(self, refresh: bool = False):
        self.refresh = refresh
        self._local = threading.local()                # per-thread persistent Session
        os.makedirs(CACHE_DIR, exist_ok=True)

    def _session(self) -> requests.Session:
        """Persistent Session per worker thread (requests.Session is not thread-safe)."""
        sess = getattr(self._local, "session", None)
        if sess is None:
            sess = requests.Session()
            sess.headers.update(HEADERS)
            self._local.session = sess
        return sess

    # ── transport with escalating 429 handling ─────────────────────────────────
    def _get(self, url: str) -> Optional[requests.Response]:
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                r = self._session().get(url, timeout=REQUEST_TIMEOUT)
            except requests.RequestException as e:
                print(f"    network error: {e}")
                return None
            if r.status_code == 429:
                wait = BACKOFF_429[min(attempt - 1, len(BACKOFF_429) - 1)]
                wait = random.uniform(wait * 0.9, wait * 1.3)
                print(f"    ⚠️ 429 — cooling {wait:.0f}s (attempt {attempt}/{MAX_RETRIES})")
                time.sleep(wait)
                continue
            return r
        return None

    def _candidate_urls(self, symbol: str) -> List[str]:
        s = quote(symbol)
        return [f"{SCREENER_BASE}/{s}/consolidated/", f"{SCREENER_BASE}/{s}/"]

    # ── page parsing ───────────────────────────────────────────────────────────
    def _parse_page(self, html: str, symbol: str) -> Optional[Dict[str, Any]]:
        soup = BeautifulSoup(html, "html.parser")
        ratios: Dict[str, Any] = {}

        # 1) top ratios strip
        wanted = {"Current Price": "current_price", "Market Cap": "market_cap",
                  "Stock P/E": "pe", "Book Value": "book_value",
                  "ROCE": "roce", "ROE": "roe"}
        for item in soup.select("#top-ratios li"):
            name_el, val_el = item.select_one(".name"), item.select_one(".number")
            if not (name_el and val_el):
                continue
            key = wanted.get(name_el.get_text(" ", strip=True))
            if key:
                v = _num(val_el.get_text(" ", strip=True))
                if v is not None:
                    ratios[key] = v

        # 2) balance sheet -> D/E (fail-safe NaN when equity missing/<=0)
        bs = soup.select_one("#balance-sheet table")
        if bs:
            vals = {}
            for row in bs.select("tbody tr"):
                cells = row.find_all(["th", "td"])
                if len(cells) < 2:
                    continue
                label = cells[0].get_text(" ", strip=True).rstrip("+").strip()
                v = _num(cells[-1].get_text(" ", strip=True))
                if v is not None:
                    vals[label] = v
            equity = vals.get("Equity Capital", 0) + vals.get("Reserves", 0)
            if equity > 0:
                ratios["debt_to_equity"] = round(vals.get("Borrowings", 0) / equity, 2)

        # 3) quarters table — YoY via HEADER LABELS (same month, year-1)
        qt = soup.select_one("#quarters table")
        if qt:
            try:
                thead = [th.get_text(strip=True) for th in qt.select("thead th")]
                if len(thead) >= 6:
                    series: Dict[str, Dict[int, float]] = {}
                    for row in qt.select("tbody tr"):
                        cells = row.find_all(["th", "td"])
                        if len(cells) != len(thead):        # alignment guard
                            continue
                        label = cells[0].get_text(strip=True).rstrip("+").strip()
                        for i in range(1, len(cells)):
                            v = _match_num(cells[i].get_text(strip=True))
                            if v is not None:
                                series.setdefault(label, {})[i] = v

                    def parse_hdr(s: str):
                        m = re.match(r"([A-Za-z]{3})[\s\-]*(\d{4})", s)
                        return (m.group(1).lower(), int(m.group(2))) if m else None

                    pos = {i: parse_hdr(thead[i]) for i in range(1, len(thead))}
                    latest = max([i for i in pos if pos[i]], default=None)
                    if latest is not None:
                        mth, yr = pos[latest]
                        yago = next((i for i in sorted(pos)
                                     if pos[i] == (mth, yr - 1)), None)
                        prevq = next((i for i in sorted(pos)
                                      if pos[i] == prev_quarter(mth, yr)), None)

                        # ISSUE #7 FIX: Use `or` for short-circuit evaluation
                        # (the original eagerly evaluated series.get("Revenue", {})
                        # even when "Sales" existed).
                        top = series.get("Sales") or series.get("Revenue") or {}
                        prof = series.get("Net Profit", {})
                        opm = series.get("OPM %", {})

                        if yago is not None:
                            if yago in top and top.get(yago, 0) > 0 and latest in top:
                                ratios["qtr_sales_growth"] = round(
                                    (top[latest] - top[yago]) / top[yago] * 100, 2)
                            if yago in prof:
                                p_now, p_then = prof.get(latest, 0), prof[yago]
                                if p_then > 0:
                                    ratios["qtr_profit_growth"] = round(
                                        (p_now - p_then) / p_then * 100, 2)
                                elif p_now > 0:
                                    ratios["qtr_profit_growth"] = TURNAROUND_SENTINEL
                                    ratios["turnaround"] = True
                            if yago in opm and latest in opm:
                                ratios["opm_trend"] = round(opm[latest] - opm[yago], 2)
                        if latest in opm:
                            ratios["opm"] = opm[latest]
                        # acceleration: latest YoY minus previous-quarter YoY (sales)
                        # BUG #2 FIX: prev_quarter() can now return None on
                        # unknown month abbreviations; guard against it so the
                        # generator doesn't match pos[i] == None (which would
                        # match unparseable headers and produce wrong results).
                        if prevq is not None and yago is not None:
                            pq_result = prev_quarter(*pos[prevq])
                            p_yago = next((i for i in sorted(pos)
                                           if pq_result is not None and pos[i] == pq_result), None) if pq_result else None
                            if None not in (prevq, p_yago) and \
                               prevq in top and p_yago in top and top[p_yago] > 0 and \
                               latest in top and yago in top and top[yago] > 0:
                                y1 = (top[latest] - top[yago]) / top[yago] * 100
                                y2 = (top[prevq] - top[p_yago]) / top[p_yago] * 100
                                ratios["sales_accel"] = round(y1 - y2, 2)
            # ISSUE #3 FIX: Log the exception instead of silently swallowing.
            # The original `except Exception: pass` lost the entire quarters
            # table silently — the operator had no idea why a previously-passing
            # stock suddenly failed.
            except Exception as exc:
                logger.warning("Quarters parse failed for %s: %s", symbol, exc)

        # 4) derived ratios (only when denominator valid)
        price = ratios.get("current_price", 0)
        if ratios.get("book_value"):
            ratios["pb"] = round(price / ratios["book_value"], 2)
        if ratios.get("pe"):
            ratios["eps_ttm"] = round(price / ratios["pe"], 2)
            g = ratios.get("qtr_profit_growth", 0)
            if 0 < g < TURNAROUND_SENTINEL:
                ratios["peg_ratio"] = round(ratios["pe"] / g, 2)   # YoY-qtr proxy

        if not ratios:                                        # page existed but junk
            return None
        title = soup.select_one("h1")
        name = title.get_text(" ", strip=True) if title else symbol
        name = re.sub(r"\s+share price\s*$", "", name, flags=re.I)
        return {"symbol": symbol, "name": name, "ratios": ratios}

    # ── fetch + disk cache ─────────────────────────────────────────────────────
    def _cache_hit(self, symbol: str) -> bool:
        cache_path = os.path.join(CACHE_DIR, f"{symbol}.json")
        if self.refresh or not os.path.exists(cache_path):
            return False
        return (time.time() - os.path.getmtime(cache_path)) / 86400 < CACHE_TTL_DAYS

    def get_fundamental_data(self, symbol: str) -> Optional[Dict[str, Any]]:
        cache_path = os.path.join(CACHE_DIR, f"{symbol}.json")
        if self._cache_hit(symbol):
            try:
                with open(cache_path, encoding="utf-8") as fh:
                    return json.load(fh)
            except (OSError, json.JSONDecodeError):
                pass                                        # stale/corrupt -> re-fetch

        data = None
        try:
            for url in self._candidate_urls(symbol):        # consolidated, then plain
                r = self._get(url)
                if r is None:
                    break                                   # network dead — stop early
                if r.status_code == 200:
                    data = self._parse_page(r.text, symbol)
                    if data:
                        break
                # 404 -> try next variant
        except Exception as exc:                            # HTML changed / BS4 blew up
            print(f"    parse error for {symbol}: {exc}")
            data = None

        if data:
            try:
                with open(cache_path, "w", encoding="utf-8") as fh:
                    json.dump(data, fh)
            except OSError:
                pass                                        # cache write is best-effort
        return data

    def parse_ratios(self, data: Dict) -> StockProfile:
        p = StockProfile(symbol=data.get("symbol", ""), name=data.get("name", ""))
        for key, value in data.get("ratios", {}).items():
            if hasattr(p, key):
                setattr(p, key, value)
            elif key == "pe":
                p.pe_ratio = value
            elif key == "pb":
                p.pb_ratio = value
        return p

    # ── the screen ─────────────────────────────────────────────────────────────
    def passes(self, p: StockProfile) -> bool:
        """Sector-aware gates. Missing data fails safely (NaN D/E, 0 growth...)."""
        # BUG #1 FIX: Reject missing price (0.0) in addition to the price cap.
        # The original `>= 2000` check passed 0.0 (missing data) through the
        # price gate — a stock with no price data could pass if other gates
        # happened to be met (unlikely but not fail-safe).
        if p.current_price <= 0 or p.current_price >= 2000:  # swing-friendly price cap
            return False
        if p.qtr_profit_growth < PROFIT_GROWTH_MIN:          # sentinel 999 passes
            return False
        if p.roe < ROE_MIN:
            return False
        if p.financial:                                      # banks: D/E & Sales N/A
            return True
        if p.qtr_sales_growth < SALES_GROWTH_MIN:
            return False
        if not (p.debt_to_equity < DE_MAX):                  # NaN fails here too
            return False
        return True

    def _process_symbol(self, symbol: str, industry: str) -> Dict[str, Any]:
        """Fetch/parse one symbol and apply the sector-aware gates.

        Touches only its own cache file and a thread-local Session, so it is
        safe to run concurrently across many worker threads.
        """
        cached = self._cache_hit(symbol)
        if not cached:
            time.sleep(random.uniform(*SLEEP_RANGE))        # politeness jitter
        data = self.get_fundamental_data(symbol)
        if not data:
            return {"symbol": symbol, "cached": cached, "ok": False,
                    "failed": True, "reason": "fetch_or_parse_failed"}

        p = self.parse_ratios(data)
        p.industry = industry
        p.financial = (industry == FINANCIAL_INDUSTRY)
        ok = self.passes(p)
        return {
            "symbol": symbol, "cached": cached, "ok": ok, "failed": False,
            "financial": p.financial, "profile": asdict(p) if ok else None,
            "pe": p.pe_ratio, "roe": p.roe,
            "sales_g": p.qtr_sales_growth, "profit_g": p.qtr_profit_growth,
        }

    def screen(self, symbols: List[str], industry_map: Dict[str, str]) -> pd.DataFrame:
        # ISSUE #8 FIX: Clean up stale partial CSV at the START (was at the end
        # of main()). If the script crashed mid-screen, the stale partial file
        # from the previous run would persist on disk.
        if os.path.exists(PARTIAL_CSV):
            os.remove(PARTIAL_CSV)

        results, failed = [], []
        n_fin = 0
        total = len(symbols)
        print(f"\n🔍 Screening {total} stocks "
              f"(cache hits are instant; new fetches up to {MAX_WORKERS} concurrent)")

        def run_one(symbol: str) -> Dict[str, Any]:
            try:
                return self._process_symbol(symbol, industry_map.get(symbol, ""))
            except Exception as exc:                        # never kill the batch
                return {"symbol": symbol, "cached": False, "ok": False,
                        "failed": True, "reason": f"unhandled: {exc}"}

        done = 0
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
            futures = [pool.submit(run_one, s) for s in symbols]
            for fut in as_completed(futures):
                out = fut.result()
                done += 1
                if out["ok"]:
                    results.append(out["profile"])
                    n_fin += int(out["financial"])
                    print(f"[{done:3d}/{total}] {out['symbol']:12s} "
                          f"{'(cache)' if out['cached'] else 'fetch '}"
                          f"  PE {out['pe']:6.1f} ROE {out['roe']:5.1f} "
                          f"salesG {out['sales_g']:6.1f} profG {out['profit_g']:6.1f} ✅ MATCH")
                elif out["failed"]:
                    failed.append({"symbol": out["symbol"], "reason": out["reason"]})
                    print(f"[{done:3d}/{total}] {out['symbol']:12s} "
                          f"{'(cache)' if out['cached'] else 'fetch '}  ❌ FAILED")
                else:
                    print(f"[{done:3d}/{total}] {out['symbol']:12s} "
                          f"{'(cache)' if out['cached'] else 'fetch '}"
                          f"  PE {out['pe']:6.1f} ROE {out['roe']:5.1f} "
                          f"salesG {out['sales_g']:6.1f} profG {out['profit_g']:6.1f}")

                if done % INCREMENT_EVERY == 0 and results:  # crash-safe partial save
                    pd.DataFrame(results).to_csv(PARTIAL_CSV, index=False)

        # coverage report — the "can I trust this pool?" answer
        print("\n" + "─" * 74)
        print(f"  COVERAGE: universe={len(symbols)}  fetched_ok={len(symbols)-len(failed)}"
              f"  failed={len(failed)}  matched={len(results)} "
              f"(financials matched: {n_fin})")
        print(f"  match rate on fetched: "
              f"{len(results)/max(1, len(symbols)-len(failed)):.1%}")
        if failed:
            pd.DataFrame(failed).to_csv(FAILURES_CSV, index=False)
            print(f"  ⚠️ {len(failed)} failures -> {FAILURES_CSV} "
                  f"(fix slugs or ignore if known-delisted)")
        else:
            if os.path.exists(FAILURES_CSV):
                os.remove(FAILURES_CSV)

        # pool history (append) — pairs with the validator's funnel logs
        hist = pd.DataFrame([{
            "date": date.today().isoformat(), "universe": len(symbols),
            "fetched": len(symbols) - len(failed), "failed": len(failed),
            "matched": len(results),
            "matched_pct": round(100 * len(results) / max(1, len(symbols) - len(failed)), 1),
        }])
        hist.to_csv(HISTORY_CSV, mode="a",
                    header=not os.path.exists(HISTORY_CSV), index=False)

        if results:
            return (pd.DataFrame(results)
                    .sort_values("qtr_profit_growth", ascending=False)
                    .reset_index(drop=True))
        return pd.DataFrame()


def prev_quarter(mon: str, yr: int):
    """('sep', 2024) -> ('jun', 2024). Returns None on unknown month."""
    order = ["jan", "feb", "mar", "apr", "may", "jun",
             "jul", "aug", "sep", "oct", "nov", "dec"]
    # BUG #2 FIX: list.index() raises ValueError on unknown months (e.g. "Sept"
    # instead of "Sep"). The original crash was caught by the broad except in
    # _parse_page, silently losing the ENTIRE quarters table. Now returns None
    # so the caller can skip just the acceleration calculation.
    try:
        i = order.index(mon.lower()[:3])
    except ValueError:
        return None
    return (order[i - 3], yr - 1) if i < 3 else (order[i - 3], yr)


def main():
    ap = argparse.ArgumentParser(description="Stage-1 fundamentals screener")
    ap.add_argument("--symbols", type=str, default="",
                    help="comma-separated subset, e.g. RELIANCE,TCS")
    ap.add_argument("--limit", type=int, default=0, help="only first N symbols")
    ap.add_argument("--refresh", action="store_true", help="ignore disk cache")
    args = ap.parse_args()

    print(f"📊 Streaming {UNIVERSE_CSV} ...")
    symbols: List[str] = []
    industry_map: Dict[str, str] = {}
    ind_col = None
    for chunk in pd.read_csv(UNIVERSE_CSV, chunksize=1000):
        chunk.columns = [c.strip() for c in chunk.columns]
        if ind_col is None:
            ind_col = next((c for c in chunk.columns if c.lower() == "industry"), None)
        symbols.extend(chunk["Symbol"].astype(str).str.strip().str.upper().tolist())
        if ind_col:
            industry_map.update({
                str(r["Symbol"]).strip().upper(): str(r[ind_col]).strip()
                for _, r in chunk.iterrows()
            })
    print(f"✅ {len(symbols)} symbols loaded"
          f"{f' | industry column: {ind_col}' if ind_col else ' | NO industry column'}")

    if args.symbols:
        want = {s.strip().upper() for s in args.symbols.split(",") if s.strip()}
        symbols = [s for s in symbols if s in want]
    if args.limit:
        symbols = symbols[: args.limit]

    screener = IndianStockScreener(refresh=args.refresh)
    df = screener.screen(symbols, industry_map)

    if df.empty:
        print("\n❌ No stocks matched today — check coverage/failures above "
              "before assuming it's the market.")
        return

    if os.path.exists(PARTIAL_CSV):                       # clean partial artifact
        os.remove(PARTIAL_CSV)
    df.to_csv(OUT_CSV, index=False)

    print("\n🚀 SWING CANDIDATES (stage-1 pool for the technical validator):")
    cols = ["symbol", "industry", "current_price", "qtr_sales_growth",
            "qtr_profit_growth", "roe", "debt_to_equity", "opm_trend",
            "sales_accel", "peg_ratio", "turnaround", "financial"]
    print(df[[c for c in cols if c in df.columns]].to_string(index=False))
    print(f"\n📁 {OUT_CSV}  ({len(df)} stocks | {CACHE_TTL_DAYS}d cache in {CACHE_DIR}/)")


if __name__ == "__main__":
    main()
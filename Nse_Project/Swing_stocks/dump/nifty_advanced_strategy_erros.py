"""
╔══════════════════════════════════════════════════════════════════════════════════╗
║   NIFTY 200 — SMC PRECISION ENTRY ENGINE  v5.1                                 ║
║   Smart Money Concepts + Minervini SEPA + Institutional Flow                   ║
║                                                                                  ║
║   UPDATES IN v5.1:                                                              ║
║     • FIXED: Cache race condition (save_cache moved to main thread)             ║
║     • ADDED: Strict chronological sequence validation (Sweep -> MSS -> OB)      ║
║                                                                                  ║
║   SMC Modules:                                                                   ║
║     • Liquidity Sweep  — price sweeps swing H/L then reverses                  ║
║     • Fair Value Gap   — 3-candle imbalance; entry at 50% fill                 ║
║     • Market Structure Shift — body-close above/below swing pivot               ║
║     • Order Block      — last bearish/bullish OB before displacement            ║
║     • Premium/Discount — Fibonacci 0.5 equilibrium zone filter                 ║
║     • Displacement     — high-momentum candle confirming institutional intent   ║
╚══════════════════════════════════════════════════════════════════════════════════╝
"""

import pandas as pd
import yfinance as yf
import pandas_ta as ta
import json
import os
import importlib.util
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import warnings
from scipy.signal import argrelextrema
import numpy as np


# ── గ్లోబల్ వేరియబుల్ (Global Variable) ──────────────────────────
# యాహూ ఫైనాన్స్ ఇచ్చే సెక్టార్ పేర్లను, నిఫ్టీ సెక్టోరల్ ఇండెక్స్ తో అనుసంధానం చేయడం
SECTOR_MAP = {
    "Technology": "^CNXIT",                # Nifty IT
    "Financial Services": "^NSEBANK",      # Nifty Bank
    "Consumer Cyclical": "^CNXAUTO",       # Nifty Auto (లేదా కన్స్యూమర్)
    "Healthcare": "^CNXPHARMA",            # Nifty Pharma
    "Consumer Defensive": "^CNXFMCG",      # Nifty FMCG
    "Basic Materials": "^CNXMETAL",        # Nifty Metal
    "Energy": "^CNXENERGY",                # Nifty Energy
    "Industrials": "^CNXINFRA",            # Nifty Infra
    "Real Estate": "^CNXREALTY",           # Nifty Realty
    "Communication Services": "^CNXMEDIA", # Nifty Media
    "Utilities": "^CNXENERGY"              # (Utilities కి సపరేట్ లేదు కాబట్టి ఎనర్జీ వాడతాం)
}

SECTOR_SYMBOL_TO_NSE = {
    "^CNXIT": "NIFTY IT",
    "^NSEBANK": "NIFTY BANK",
    "^CNXAUTO": "NIFTY AUTO",
    "^CNXPHARMA": "NIFTY PHARMA",
    "^CNXFMCG": "NIFTY FMCG",
    "^CNXMETAL": "NIFTY METAL",
    "^CNXENERGY": "NIFTY ENERGY",
    "^CNXINFRA": "NIFTY INFRA",
    "^CNXREALTY": "NIFTY REALTY",
    "^CNXMEDIA": "NIFTY MEDIA",
}

try:
    from NseUtility.NseUtility import NseUtils
except Exception:
    NseUtils = None


warnings.filterwarnings("ignore")

# ════════════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ════════════════════════════════════════════════════════════════════════════════

CACHE_FILE         = "nifty_yf_cache.json"
CACHE_EXPIRY_HOURS = 6
MAX_WORKERS        = 5
SWING_LOOKBACK     = 10   # bars each side to define a swing high/low pivot
ATR_SL_MULTIPLIER  = 0.5  # extra ATR buffer below OB/sweep low for SL
MIN_RR             = 2.0  # minimum reward-to-risk ratio to flag a trade


# ════════════════════════════════════════════════════════════════════════════════
# CACHE HELPERS
# ════════════════════════════════════════════════════════════════════════════════

def load_cache() -> dict:
    """Load JSON cache from disk; return empty dict on any error."""
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE) as f:
                return json.load(f)
        except Exception:
            return {}
    return {}


def save_cache(cache: dict) -> None:
    """Persist cache to disk."""
    try:
        with open(CACHE_FILE, "w") as f:
            json.dump(cache, f, indent=2)
    except Exception as e:
        print(f"Cache save error: {e}")


def is_cache_fresh(cache: dict, key: str) -> bool:
    """Return True if the cached entry exists and is younger than CACHE_EXPIRY_HOURS."""
    if key not in cache:
        return False
    try:
        cached_time = datetime.fromisoformat(cache[key]["ts"])
        return datetime.now() - cached_time < timedelta(hours=CACHE_EXPIRY_HOURS)
    except Exception:
        return False


# ════════════════════════════════════════════════════════════════════════════════
# UTILITY — safe scalar extractor
# ════════════════════════════════════════════════════════════════════════════════

def _safe(val) -> float | None:
    """Convert a pandas scalar / numpy scalar to a plain Python float, or None."""
    try:
        if val is None:
            return None
        v = float(val)
        return round(v, 4) if not np.isnan(v) else None
    except Exception:
        return None


def _flatten_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Flatten yfinance MultiIndex columns to single-level strings."""
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [col[0] if isinstance(col, tuple) else col for col in df.columns]
    return df


def _load_nse_master_class():
    """Load NSEMasterData from 'NSE Historical.py' (filename contains a space)."""
    try:
        historical_path = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "..", "..", "NseUtility", "NSE Historical.py")
        )
        if not os.path.exists(historical_path):
            return None
        spec = importlib.util.spec_from_file_location("nse_historical_module", historical_path)
        if spec is None or spec.loader is None:
            return None
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return getattr(module, "NSEMasterData", None)
    except Exception:
        return None


def _init_nse_sources():
    """Initialize NSE utility clients once; return available sources with safe fallbacks."""
    nse_master = None
    nse_utils = None

    master_cls = _load_nse_master_class()
    if master_cls is not None:
        try:
            candidate = master_cls()
            candidate.download_symbol_master()
            if getattr(candidate, "has_data", False):
                nse_master = candidate
                cm_rows = len(candidate.nse_data) if candidate.nse_data is not None else 0
                fo_rows = len(candidate.nfo_data) if candidate.nfo_data is not None else 0
                print(f"  NSEMasterData ready — CM:{cm_rows} symbols | FO:{fo_rows} symbols.")
            else:
                # Master URLs unavailable and no local CSV → charting API unusable;
                # historical data will fall back to yfinance automatically.
                print("  ⚠️ NSEMasterData: charting master unavailable — using yfinance fallback.")
        except Exception as e:
            print(f"  ⚠️ NSEMasterData init failed: {e}")
            nse_master = None

    if NseUtils is not None:
        try:
            nse_utils = NseUtils()
            print("  NseUtils ready (institutional endpoints).")
        except Exception as e:
            print(f"  ⚠️ NseUtils init failed: {e}")
            nse_utils = None

    return nse_master, nse_utils


def _history_from_nse_master(nse_master, symbol: str, exchange: str = "NSE", days: int = 365,
                             interval: str = "1d") -> pd.DataFrame:
    """Fetch historical OHLCV from NSEMasterData and normalize schema."""
    if nse_master is None:
        return pd.DataFrame()

    try:
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)
        hist = nse_master.get_history(
            symbol=symbol,
            exchange=exchange,
            start=start_date,
            end=end_date,
            interval=interval,
        )
        if hist is None or hist.empty:
            return pd.DataFrame()
        for col in ("Open", "High", "Low", "Close", "Volume"):
            if col not in hist.columns:
                return pd.DataFrame()
        return hist
    except Exception:
        return pd.DataFrame()


def _extract_number(value) -> float:
    """Parse mixed numeric representations safely (e.g., '1,234.5', '(123)')."""
    if value is None:
        return 0.0
    if isinstance(value, (int, float, np.integer, np.floating)):
        return float(value)
    s = str(value).strip().replace(",", "")
    if not s:
        return 0.0
    sign = -1.0 if s.startswith("(") and s.endswith(")") else 1.0
    s = s.strip("()")
    try:
        return sign * float(s)
    except Exception:
        return 0.0


def build_institutional_context(nse_utils) -> dict:
    """Build market-wide and symbol-level institutional-flow context from NSE endpoints."""
    ctx = {
        "market_bias": 0.0,
        "insider_symbol_score": {},
        "bulk_block_symbol_score": {},
    }

    if nse_utils is None:
        return ctx

    # FII/DII net flow -> market bias component
    try:
        fii_dii = nse_utils.fii_dii_activity()
        if isinstance(fii_dii, pd.DataFrame) and not fii_dii.empty:
            type_col = next((c for c in fii_dii.columns if "client" in c.lower() or "category" in c.lower()), None)
            net_col = next((c for c in fii_dii.columns if "net" in c.lower()), None)
            if type_col and net_col:
                fii_net = 0.0
                dii_net = 0.0
                for _, row in fii_dii.iterrows():
                    tval = str(row.get(type_col, "")).lower()
                    nval = _extract_number(row.get(net_col))
                    if "fii" in tval or "foreign" in tval:
                        fii_net += nval
                    elif "dii" in tval or "domestic" in tval:
                        dii_net += nval
                # Soft clipping: keep impact bounded
                ctx["market_bias"] = float(np.clip((fii_net + dii_net) / 5000.0, -3.0, 3.0))
    except Exception:
        pass

    # Insider activity -> symbol-level score
    try:
        insider = nse_utils.get_insider_trading()
        if isinstance(insider, pd.DataFrame) and not insider.empty:
            symbol_col = next((c for c in insider.columns if "symbol" in c.lower()), None)
            action_cols = [c for c in insider.columns if any(k in c.lower() for k in ("type", "mode", "acq", "disp", "txn", "dealt"))]
            if symbol_col:
                for _, row in insider.iterrows():
                    sym = str(row.get(symbol_col, "")).strip().upper()
                    if not sym:
                        continue
                    action_text = " ".join(str(row.get(c, "")).lower() for c in action_cols)
                    if any(k in action_text for k in ("buy", "acq", "acquire", "purchase")):
                        ctx["insider_symbol_score"][sym] = ctx["insider_symbol_score"].get(sym, 0.0) + 0.75
                    elif any(k in action_text for k in ("sell", "disp", "dispose")):
                        ctx["insider_symbol_score"][sym] = ctx["insider_symbol_score"].get(sym, 0.0) - 0.75
    except Exception:
        pass

    # Corporate announcements -> bulk/block/deal hints
    try:
        ann = nse_utils.get_corporate_announcement()
        if isinstance(ann, pd.DataFrame) and not ann.empty:
            symbol_col = next((c for c in ann.columns if "symbol" in c.lower()), None)
            subject_col = next((c for c in ann.columns if "subject" in c.lower() or "headline" in c.lower()), None)
            if symbol_col and subject_col:
                for _, row in ann.iterrows():
                    subj = str(row.get(subject_col, "")).lower()
                    if not any(k in subj for k in ("bulk", "block", "deal")):
                        continue
                    sym = str(row.get(symbol_col, "")).strip().upper()
                    if not sym:
                        continue
                    ctx["bulk_block_symbol_score"][sym] = ctx["bulk_block_symbol_score"].get(sym, 0.0) + 0.5
    except Exception:
        pass

    return ctx


def institutional_flow_factor_for_symbol(symbol: str, ctx: dict) -> float:
    """Combine market-wide and symbol-specific institutional cues into one bounded factor."""
    if not ctx:
        return 0.0
    sym = str(symbol).upper()
    score = float(ctx.get("market_bias", 0.0))
    score += float(ctx.get("insider_symbol_score", {}).get(sym, 0.0))
    score += float(ctx.get("bulk_block_symbol_score", {}).get(sym, 0.0))
    return float(np.clip(score, -5.0, 5.0))


# ════════════════════════════════════════════════════════════════════════════════
# SMC MODULE 1 — SWING PIVOT DETECTION
# ════════════════════════════════════════════════════════════════════════════════

def find_swing_pivots(high: np.ndarray, low: np.ndarray, n: int = 2):
    """
    Scipy లైబ్రరీని ఉపయోగించి మరింత వేగంగా మరియు కచ్చితంగా
    స్వింగ్ హైస్ (sh_idx) మరియు స్వింగ్ లోస్ (sl_idx) కనుక్కునే ఫంక్షన్.
    """
    # స్వింగ్ హైస్ కనుక్కోవడం (Local Maxima)
    sh_idx = argrelextrema(high, np.greater, order=n)[0].tolist()

    # స్వింగ్ లోస్ కనుక్కోవడం (Local Minima)
    sl_idx = argrelextrema(low, np.less, order=n)[0].tolist()

    return sh_idx, sl_idx

# ════════════════════════════════════════════════════════════════════════════════
# SMC MODULE 2 — LIQUIDITY SWEEP DETECTION
# ════════════════════════════════════════════════════════════════════════════════

def detect_liquidity_sweep(high: np.ndarray, low: np.ndarray,
                           close: np.ndarray, open_: np.ndarray,
                           sh_idx: list, sl_idx: list,
                           lookback: int = 5):
    result = {"swept": False, "direction": None, "sweep_level": None,
              "sweep_bar": None, "sweep_low": None, "sweep_high": None}

    n = len(close)

    # ఎర్రర్ ఫిక్స్: n-1 (లేటెస్ట్ క్యాండిల్) కచ్చితంగా లూప్‌లో ఉండేలా రేంజ్ ని `n` కి మార్చాము.
    check_range = range(max(n - lookback, 1), n)

    # Bullish Sweep (క్రింద ఉన్న లిక్విడిటీని లాక్కోవడం)
    if sl_idx:
        recent_sl_level = low[sl_idx[-1]]
        for i in check_range:
            # క్యాండిల్ పాత లో కంటే కిందకు వెళ్లి, పైన క్లోజ్ అయితే...
            if low[i] < recent_sl_level and close[i] > recent_sl_level:
                result.update({
                    "swept": True,
                    "direction": "bullish",
                    "sweep_level": round(float(recent_sl_level), 4),
                    "sweep_bar": i,
                    "sweep_low": round(float(low[i]), 4),
                    "sweep_high": round(float(high[i]), 4),
                })
                return result

    # Bearish Sweep (పైన ఉన్న లిక్విడిటీని లాక్కోవడం)
    if sh_idx:
        recent_sh_level = high[sh_idx[-1]]
        for i in check_range:
            # క్యాండిల్ పాత హై కంటే పైకి వెళ్లి, కింద క్లోజ్ అయితే...
            if high[i] > recent_sh_level and close[i] < recent_sh_level:
                result.update({
                    "swept": True,
                    "direction": "bearish",
                    "sweep_level": round(float(recent_sh_level), 4),
                    "sweep_bar": i,
                    "sweep_low": round(float(low[i]), 4),
                    "sweep_high": round(float(high[i]), 4),
                })
                return result

    return result
# ════════════════════════════════════════════════════════════════════════════════
# SMC MODULE 3 — FAIR VALUE GAP (FVG)
# ════════════════════════════════════════════════════════════════════════════════

def detect_fvg(high: np.ndarray, low: np.ndarray, close: np.ndarray,
               direction: str | None, lookback: int = 20):
    result = {"found": False, "fvg_high": None, "fvg_low": None,
              "fvg_mid": None, "retesting": False, "direction": direction}

    if direction is None:
        return result

    n = len(close)
    current_close = close[n - 1]

    # ఎర్రర్ ఫిక్స్: n-2 బదులు n-1 వాడాము.
    # తద్వారా ఈరోజు క్లోజ్ అయిన క్యాండిల్ ను కూడా 3వ క్యాండిల్ గా పరిగణలోకి తీసుకుంటుంది.
    for i in range(n - 1, max(n - lookback - 1, 1), -1):

        # Bullish FVG (సపోర్ట్ లాగా పనిచేస్తుంది)
        if direction == "bullish":
            # 1వ క్యాండిల్ హై, 3వ క్యాండిల్ లో కంటే తక్కువ ఉంటే...
            if high[i - 2] < low[i]:
                gap_low = float(high[i - 2])
                gap_high = float(low[i])
                gap_mid = round((gap_low + gap_high) / 2, 4)
                retesting = gap_low <= current_close <= gap_high

                result.update({
                    "found": True,
                    "fvg_high": round(gap_high, 4),
                    "fvg_low": round(gap_low, 4),
                    "fvg_mid": gap_mid,
                    "retesting": bool(retesting),
                })
                return result

        # Bearish FVG (రెసిస్టన్స్ లాగా పనిచేస్తుంది)
        elif direction == "bearish":
            # 1వ క్యాండిల్ లో, 3వ క్యాండిల్ హై కంటే ఎక్కువ ఉంటే...
            if low[i - 2] > high[i]:
                gap_high = float(low[i - 2])
                gap_low = float(high[i])
                gap_mid = round((gap_low + gap_high) / 2, 4)
                retesting = gap_low <= current_close <= gap_high

                result.update({
                    "found": True,
                    "fvg_high": round(gap_high, 4),
                    "fvg_low": round(gap_low, 4),
                    "fvg_mid": gap_mid,
                    "retesting": bool(retesting),
                })
                return result

    return result

# ════════════════════════════════════════════════════════════════════════════════
# SMC MODULE 4 — MARKET STRUCTURE SHIFT (MSS)
# ════════════════════════════════════════════════════════════════════════════════

def detect_mss(high: np.ndarray, low: np.ndarray,
               close: np.ndarray,
               sh_idx: list, sl_idx: list,
               direction: str | None):
    # అనవసరమైన open_ పారామీటర్ ని పైనుంచి తీసేశాము.

    result = {"confirmed": False, "mss_level": None, "mss_bar": None}

    if direction is None:
        return result

    n = len(close)
    NOISE_FILTER = 0.0005  # ఫేక్ బ్రేక్అవుట్స్ నివారించడానికి 0.05% బఫర్

    # ఎర్రర్ ఫిక్స్: n-2 బదులు n-1 వాడాము.
    # ఇప్పుడు లేటెస్ట్ క్యాండిల్ బాడీ క్లోజ్ అయినా సరే వెంటనే కన్ఫర్మ్ చేస్తుంది.
    check_range = range(n - 1, max(n - 12, 0), -1)

    if direction == "bullish" and sh_idx:
        pivot_level = float(high[sh_idx[-1]])
        for i in check_range:
            body_close = float(close[i])
            # నాయిస్ ఫిల్టర్ కలుపుకుని పాత హై పైన క్లోజ్ అవ్వాలి
            if body_close > pivot_level * (1 + NOISE_FILTER):
                result.update({
                    "confirmed": True,
                    "mss_level": round(pivot_level, 4),
                    "mss_bar": i,
                })
                return result

    elif direction == "bearish" and sl_idx:
        pivot_level = float(low[sl_idx[-1]])
        for i in check_range:
            body_close = float(close[i])
            # నాయిస్ ఫిల్టర్ తీసేసి పాత లో కింద క్లోజ్ అవ్వాలి
            if body_close < pivot_level * (1 - NOISE_FILTER):
                result.update({
                    "confirmed": True,
                    "mss_level": round(pivot_level, 4),
                    "mss_bar": i,
                })
                return result

    return result

# ════════════════════════════════════════════════════════════════════════════════
# SMC MODULE 5 — ORDER BLOCK DETECTION
# ════════════════════════════════════════════════════════════════════════════════
def detect_order_block(high: np.ndarray, low: np.ndarray,
                       close: np.ndarray, open_: np.ndarray,
                       volume: np.ndarray, direction: str | None,
                       sweep_bar: int | None, lookback: int = 15):
    result = {"found": False, "ob_high": None, "ob_low": None,
              "ob_mid": None, "ob_bar": None}

    if direction is None:
        return result

    n = len(close)
    start = sweep_bar if sweep_bar is not None else n - 2
    start = min(start, n - 2)

    # క్యాండిల్స్ యొక్క బాడీ సైజులు
    bodies = np.abs(close - open_)

    # సగటు క్యాండిల్ సైజు మరియు సగటు వాల్యూమ్ లెక్కింపు
    avg_body = bodies[max(0, start - 10): start].mean() if start > 0 else 1.0
    if avg_body == 0: avg_body = 1.0

    avg_vol = volume[max(0, start - 10): start].mean() if start > 0 else 1.0
    if avg_vol == 0: avg_vol = 1.0

    if direction == "bullish":
        for i in range(start, max(start - lookback, 1), -1):
            is_bearish_ob = close[i] < open_[i]  # ఇది రెడ్ క్యాండిల్ అవ్వాలి
            next_body = close[i + 1] - open_[i + 1]

            # అప్‌డేట్: బాడీ 1.5 రెట్లు పెద్దగా ఉండాలి AND వాల్యూమ్ కనీసం 20% (1.2x) ఎక్కువ ఉండాలి
            is_displacement = (next_body > avg_body * 1.5) and (close[i + 1] > open_[i + 1]) and (
                        volume[i + 1] > avg_vol * 1.2)

            if is_bearish_ob and is_displacement:
                ob_low = float(low[i])
                ob_high = float(high[i])
                result.update({
                    "found": True,
                    "ob_high": round(ob_high, 4),
                    "ob_low": round(ob_low, 4),
                    "ob_mid": round((ob_low + ob_high) / 2, 4),
                    "ob_bar": i,
                })
                return result

    elif direction == "bearish":
        for i in range(start, max(start - lookback, 1), -1):
            is_bullish_ob = close[i] > open_[i]  # ఇది గ్రీన్ క్యాండిల్ అవ్వాలి
            next_body = open_[i + 1] - close[i + 1]

            # అప్‌డేట్: బాడీ 1.5 రెట్లు పెద్దగా ఉండాలి AND వాల్యూమ్ కనీసం 20% (1.2x) ఎక్కువ ఉండాలి
            is_displacement = (next_body > avg_body * 1.5) and (close[i + 1] < open_[i + 1]) and (
                        volume[i + 1] > avg_vol * 1.2)

            if is_bullish_ob and is_displacement:
                ob_low = float(low[i])
                ob_high = float(high[i])
                result.update({
                    "found": True,
                    "ob_high": round(ob_high, 4),
                    "ob_low": round(ob_low, 4),
                    "ob_mid": round((ob_low + ob_high) / 2, 4),
                    "ob_bar": i,
                })
                return result

    return result


# ════════════════════════════════════════════════════════════════════════════════
# SMC MODULE 6 — PREMIUM / DISCOUNT FIBONACCI ZONE
# ════════════════════════════════════════════════════════════════════════════════

def premium_discount_zone(high: np.ndarray, low: np.ndarray,
                          close: np.ndarray, direction: str | None,
                          lookback: int = 50):
    result = {"in_zone": False, "zone_type": None, "fib_50": None,
              "fib_618": None, "fib_786": None,
              "range_high": None, "range_low": None}

    if direction is None or len(high) < lookback:
        return result

    n = len(close)

    # ఎర్రర్ ఫిక్స్: [n - lookback: n - 1] బదులు [n - lookback: n] వాడాము.
    # తద్వారా ఈరోజు రన్ అవుతున్న కరెంట్ క్యాండిల్ యొక్క హై/లో ని కూడా లెక్కలోకి తీసుకుంటుంది.
    window_high = float(high[n - lookback: n].max())
    window_low = float(low[n - lookback: n].min())
    rng = window_high - window_low

    if rng <= 0:
        return result

    # ఫిబొనాచీ లెవెల్స్ లెక్కింపు
    fib_50 = window_low + rng * 0.500
    fib_618 = window_low + rng * 0.382
    fib_786 = window_low + rng * 0.214

    current = float(close[n - 1])  # లేటెస్ట్ క్యాండిల్ క్లోజింగ్ ధర

    result.update({
        "fib_50": round(fib_50, 4),
        "fib_618": round(fib_618, 4),
        "fib_786": round(fib_786, 4),
        "range_high": round(window_high, 4),
        "range_low": round(window_low, 4),
    })

    if direction == "bullish":
        # అప్‌ట్రెండ్ లో ధర 50% కింద ఉంటే డిస్కౌంట్ జోన్ (కొనడానికి మంచిది)
        in_discount = current <= fib_50
        result.update({"in_zone": bool(in_discount), "zone_type": "discount" if in_discount else "premium"})

    elif direction == "bearish":
        # డౌన్‌ట్రెండ్ లో ధర 50% పైన ఉంటే ప్రీమియం జోన్ (సెల్ చేయడానికి మంచిది)
        in_premium = current >= fib_50
        result.update({"in_zone": bool(in_premium), "zone_type": "premium" if in_premium else "discount"})

    return result
# ════════════════════════════════════════════════════════════════════════════════
# SMC MODULE 7 — DISPLACEMENT VOLUME CONFIRMATION
# ════════════════════════════════════════════════════════════════════════════════

def displacement_confirmed(close: np.ndarray, open_: np.ndarray,
                           volume: np.ndarray, direction: str | None,
                           lookback: int = 5) -> bool:
    # high, low పారామీటర్లను తీసేసాము (వాటి అవసరం ఇక్కడ లేదు)

    if direction is None:
        return False

    n = len(close)
    bodies = np.abs(close - open_)

    # ఎర్రర్ ఫిక్స్: లేటెస్ట్ 20 క్యాండిల్స్ ని లెక్కలోకి తీసుకోవడానికి ఇండెక్స్ మార్చాము (n-20: n)
    avg_body = bodies[max(0, n - 20): n].mean()
    avg_vol = volume[max(0, n - 20): n].mean()

    if avg_body == 0 or avg_vol == 0:
        return False

    # ఎర్రర్ ఫిక్స్: n-2 బదులు n-1 (లేటెస్ట్ క్యాండిల్) నుంచి వెతకడం మొదలుపెడుతుంది
    for i in range(n - 1, max(n - lookback - 1, -1), -1):
        body = float(bodies[i])
        vol = float(volume[i])
        is_bullish_candle = close[i] > open_[i]
        is_bearish_candle = close[i] < open_[i]

        # అప్‌ట్రెండ్ లో భారీ గ్రీన్ క్యాండిల్ + భారీ వాల్యూమ్ ఉంటే కన్ఫర్మ్
        if (direction == "bullish" and is_bullish_candle
                and body >= avg_body * 1.5 and vol >= avg_vol * 1.5):
            return True

        # డౌన్‌ట్రెండ్ లో భారీ రెడ్ క్యాండిల్ + భారీ వాల్యూమ్ ఉంటే కన్ఫర్మ్
        if (direction == "bearish" and is_bearish_candle
                and body >= avg_body * 1.5 and vol >= avg_vol * 1.5):
            return True

    return False

# ════════════════════════════════════════════════════════════════════════════════
# SMC MASTER FUNCTION — combines all modules into one entry signal
# ════════════════════════════════════════════════════════════════════════════════

def smc_entry_signal(hist: pd.DataFrame, atr_value: float | None):
    """
    మార్కెట్ డేటాను తీసుకుని అన్ని SMC మాడ్యూల్స్ ని వరుసగా రన్ చేసి,
    ఫైనల్ ట్రేడింగ్ సిగ్నల్ (Entry, SL, TP) ఇచ్చే మాస్టర్ ఫంక్షన్.
    """
    default = {
        "smc_signal": "NO_SETUP", "smc_direction": None, "smc_score": 0,
        "entry_price": None, "stop_loss": None, "take_profit": None,
        "rr_ratio": None, "sweep_level": None, "fvg_high": None,
        "fvg_low": None, "fvg_mid": None, "ob_high": None,
        "ob_low": None, "fib_50": None, "in_fib_zone": False,
        "mss_confirmed": False, "displaced": False, "confluence": 0,
    }

    try:
        # ── నంపై అరేస్ ఎక్స్‌ట్రాక్ట్ చేయడం ──────────────────────────────────
        h_arr = hist["High"].values.astype(float)
        l_arr = hist["Low"].values.astype(float)
        c_arr = hist["Close"].values.astype(float)
        o_arr = hist["Open"].values.astype(float)
        v_arr = hist["Volume"].values.astype(float)

        # ── Step 1: Swing pivots (Scipy తో) ─────────────────────────
        sh_idx, sl_idx = find_swing_pivots(h_arr, l_arr, n=SWING_LOOKBACK)
        if not sh_idx or not sl_idx:
            return default

        # ── Step 2: Liquidity sweep ──────────────────────────────────
        sweep = detect_liquidity_sweep(h_arr, l_arr, c_arr, o_arr, sh_idx, sl_idx)
        if not sweep["swept"]:
            return default  # స్వీప్ లేకపోతే ఇక్కడే కోడ్ ఆగిపోతుంది

        direction = sweep["direction"]
        sweep_bar = sweep["sweep_bar"]
        sweep_low = sweep["sweep_low"]
        sweep_high = sweep["sweep_high"]
        sweep_level = sweep["sweep_level"]

        # ── Step 3: Fair Value Gap ───────────────────────────────────
        fvg = detect_fvg(h_arr, l_arr, c_arr, direction)

        # ── Step 4: Market Structure Shift ───────────────────────────
        # ఎర్రర్ ఫిక్స్: ఇక్కడినుంచి o_arr (open) ని తీసేశాము.
        mss = detect_mss(h_arr, l_arr, c_arr, sh_idx, sl_idx, direction)

        # ── Step 5: Order Block ──────────────────────────────────────
        ob = detect_order_block(h_arr, l_arr, c_arr, o_arr, v_arr, direction, sweep_bar)

        # ── Strict Sequence Check (Sweep -> MSS -> OB) ───────────────
        # అన్నీ కరెక్ట్ ఆర్డర్ లో జరిగితేనే వాలిడ్ తీసుకుంటుంది.
        if mss["confirmed"] and mss["mss_bar"] is not None and sweep_bar is not None:
            if mss["mss_bar"] < sweep_bar:
                return default
        if ob["found"] and ob["ob_bar"] is not None and mss["mss_bar"] is not None:
            if ob["ob_bar"] > mss["mss_bar"]:
                return default

        # ── Step 6: Premium / Discount zone ──────────────────────────
        fib = premium_discount_zone(h_arr, l_arr, c_arr, direction)

        # ── Step 7: Displacement volume ──────────────────────────────
        # ఎర్రర్ ఫిక్స్: h_arr, l_arr పారామీటర్స్ ని తీసేశాము.
        displaced = displacement_confirmed(c_arr, o_arr, v_arr, direction)

        # ── Step 8: Confluence Score (మార్కులు వేయడం) ───────────────
        confluence = 0
        if sweep["swept"]:   confluence += 1
        if fvg["found"]:     confluence += 1
        if fvg["retesting"]: confluence += 1
        if mss["confirmed"]: confluence += 1
        if ob["found"]:      confluence += 1
        if fib["in_zone"]:   confluence += 1
        if displaced:        confluence += 1

        if confluence < 3:
            signal = "WEAK_SETUP"
        elif confluence < 5:
            signal = "MODERATE_SETUP"
        else:
            signal = "HIGH_CONFLUENCE"

        # ── Step 9: Entry, Stop-Loss (SL), Take-Profit (TP) ──────────
        atr_buf = float(atr_value) * ATR_SL_MULTIPLIER if atr_value else 0.0
        entry_price = stop_loss = take_profit = rr_ratio = None
        risk = 0.0

        if direction == "bullish":
            # గ్యాప్ మిడ్ పాయింట్ లేదా ఆర్డర్ బ్లాక్ మిడ్ పాయింట్ ఎంట్రీ అవుతుంది
            candidates = [x for x in [fvg.get("fvg_mid"), ob.get("ob_mid")] if x is not None]
            entry_price = max(candidates) if candidates else float(c_arr[-1])
            stop_loss = round(sweep_low - atr_buf, 4)

            risk = max(entry_price - stop_loss, atr_buf if atr_buf > 0 else entry_price * 0.01)
            tp_candidates = [float(h_arr[i]) for i in sh_idx if float(h_arr[i]) > entry_price]
            take_profit = round(min(tp_candidates), 4) if tp_candidates else round(entry_price + risk * MIN_RR, 4)

        elif direction == "bearish":
            candidates = [x for x in [fvg.get("fvg_mid"), ob.get("ob_mid")] if x is not None]
            entry_price = min(candidates) if candidates else float(c_arr[-1])
            stop_loss = round(sweep_high + atr_buf, 4)

            risk = max(stop_loss - entry_price, atr_buf if atr_buf > 0 else entry_price * 0.01)
            tp_candidates = [float(l_arr[i]) for i in sl_idx if float(l_arr[i]) < entry_price]
            take_profit = round(max(tp_candidates), 4) if tp_candidates else round(entry_price - risk * MIN_RR, 4)

        if risk > 0:
            rr_ratio = round(abs(take_profit - entry_price) / risk, 2)

        # కనీసం 1:2 లాభం లేకపోతే ట్రేడ్ వద్దు
        if rr_ratio is not None and rr_ratio < MIN_RR:
            signal = "LOW_RR"

        return {
            "smc_signal": signal,
            "smc_direction": direction,
            "smc_score": confluence,
            "entry_price": round(entry_price, 4) if entry_price else None,
            "stop_loss": stop_loss,
            "take_profit": take_profit,
            "rr_ratio": rr_ratio,
            "sweep_level": sweep_level,
            "fvg_mid": fvg.get("fvg_mid"),
            "ob_low": ob.get("ob_low"),
            "ob_high": ob.get("ob_high"),
            "fib_50": fib.get("fib_50"),
            "in_fib_zone": fib.get("in_zone", False),
            "mss_confirmed": mss.get("confirmed", False),
            "displaced": displaced,
            "confluence": confluence,
        }

    except Exception as e:
        # ఏదైనా తప్పు జరిగితే ఎర్రర్ రాకుండా సేఫ్ గా డీఫాల్ట్ పంపుతుంది
        return default
# ════════════════════════════════════════════════════════════════════════════════
# LEGACY STRATEGY SCORES
# ════════════════════════════════════════════════════════════════════════════════

def order_flow_score(hist: pd.DataFrame) -> float:
    try:
        if len(hist) < 15:
            return 0.0

        # డేటాఫ్రేమ్ మొత్తం కాపీ చేయకుండా కేవలం సీరీస్‌లపై మాత్రమే క్యాలిక్యులేట్ చేస్తున్నాం
        close = hist["Close"]
        tp = (hist["High"] + hist["Low"] + close) / 3
        mf = tp * hist["Volume"]

        # ధర పెరిగిన రోజులు / తగ్గిన రోజులు
        close_diff = close.diff()

        pf = mf.where(close_diff > 0, 0.0).tail(14).sum()
        nf = mf.where(close_diff < 0, 0.0).tail(14).sum()

        mfr = pf / (nf + 1e-9)

        # గరిష్టంగా 20 పాయింట్లు వచ్చేలా స్కోర్ ఇస్తుంది
        return round(min(mfr * 10.0, 20.0), 2)
    except Exception:
        return 0.0


def structure_score(hist: pd.DataFrame) -> float:
    try:
        if len(hist) < 20:
            return 0.0

        # కేవలం క్లోజింగ్ ధరల ఆధారంగా ట్రెండ్ మూవ్‌మెంట్ చెక్ చేయడం చాలా కచ్చితంగా ఉంటుంది
        # లేదా గత 20 రోజుల్లో జరిగిన స్వింగ్ హై/లోస్ ఆధారంగా చూద్దాం
        highs = hist["High"].tail(20).values
        lows = hist["Low"].tail(20).values

        # రోజువారీ చిన్న చిన్న హెచ్చుతగ్గులు కాకుండా, 3 రోజుల మూవింగ్ కంపారిజన్ వాడటం మంచిది
        hh_count = 0
        hl_count = 0
        comparisons = len(highs) - 2

        if comparisons <= 0:
            return 0.0

        for i in range(2, len(highs)):
            # ఈరోజు హై, 2 రోజుల క్రితం హై కంటే ఎక్కువగా ఉంటే (Better Higher High check)
            if highs[i] > highs[i - 2]:
                hh_count += 1
            # ఈరోజు లో, 2 రోజుల క్రితం లో కంటే ఎక్కువగా ఉంటే (Better Higher Low check)
            if lows[i] > lows[i - 2]:
                hl_count += 1

        score = (hh_count + hl_count) / (2 * comparisons) * 20.0
        return round(min(score, 20.0), 2)

    except Exception:
        return 0.0


import numpy as np
import pandas as pd


def vcp_score(hist: pd.DataFrame) -> float:
    try:
        if len(hist) < 20:
            return 0.0

        # గత 20 రోజుల హై - లో రేంజ్
        ranges = (hist["High"] - hist["Low"]).tail(20).values

        # Numpy ఉపయోగించి నిన్నటి కంటే ఈరోజు రేంజ్ తగ్గిన రోజులను కౌంట్ చేయడం (చాలా ఫాస్ట్)
        contractions = np.sum(np.diff(ranges) < 0)

        # ప్రాక్టికల్ స్కోరింగ్: 20 రోజుల్లో 10 సార్లు రేంజ్ తగ్గినా అది అద్భుతమైన VCP అవుతుంది
        if contractions >= 10:
            score = 15.0
        elif contractions >= 7:
            score = 8.0
        elif contractions >= 5:
            score = 4.0
        else:
            score = 0.0

        # వాల్యూమ్ డ్రై-అప్ (Volume Dry-up) చెక్
        vols = hist["Volume"].tail(10).values
        if len(vols) > 1:
            avg_vol_past = np.mean(vols[:-1])
            # లేటెస్ట్ వాల్యూమ్ పాత సగటు కంటే 30% తక్కువగా ఉంటే బోనస్ మార్కులు
            if avg_vol_past > 0 and vols[-1] < (avg_vol_past * 0.7):
                score += 5.0

        return float(min(round(score, 2), 20.0))

    except Exception:
        return 0.0


def rs_vs_index_score(symbol_hist: pd.DataFrame, nifty_hist: pd.DataFrame, sector_hist: pd.DataFrame = None) -> float:
    """
    స్టాక్ పెర్ఫార్మెన్స్‌ను అటు నిఫ్టీ-50 తో, ఇటు దాని సొంత సెక్టార్‌తో పోల్చుతుంది.
    """
    periods = [20, 60]
    try:
        score = 0.0

        # డేటాని నంపై అరేస్ గా మార్చుకోవడం (స్పీడ్ కోసం)
        s_close = symbol_hist["Close"].values
        n_close = nifty_hist["Close"].values if (nifty_hist is not None and not nifty_hist.empty) else None
        c_close = sector_hist["Close"].values if (sector_hist is not None and not sector_hist.empty) else None

        for p in periods:
            # కనీసం ఆ పీరియడ్ (20 లేదా 60 రోజులు) డేటా లేకపోతే స్కిప్ చేయాలి
            if len(s_close) <= p:
                continue

            # స్టాక్ రిటర్న్
            s_ret = (s_close[-1] - s_close[-(p + 1)]) / s_close[-(p + 1)]

            # 1. నిఫ్టీ తో పోలిక (Max 10 మార్కులు)
            if n_close is not None and len(n_close) > p:
                n_ret = (n_close[-1] - n_close[-(p + 1)]) / n_close[-(p + 1)]
                diff = s_ret - n_ret
                if diff > 0.05:
                    score += 5.0
                elif diff > 0.02:
                    score += 3.0
                elif diff > 0:
                    score += 1.0

            # 2. సొంత సెక్టార్ తో పోలిక (Max 10 మార్కులు)
            if c_close is not None and len(c_close) > p:
                c_ret = (c_close[-1] - c_close[-(p + 1)]) / c_close[-(p + 1)]
                diff_sec = s_ret - c_ret
                if diff_sec > 0.05:
                    score += 5.0
                elif diff_sec > 0.02:
                    score += 3.0
                elif diff_sec > 0:
                    score += 1.0

        return float(min(round(score, 2), 20.0))

    except Exception:
        return 0.0

def weekly_trend_check(hist: pd.DataFrame) -> dict:
    try:
        if len(hist) < 100:
            return {"mtf_score": 0, "mtf_status": "Neutral"}

        weekly = hist.resample('W-FRI').agg({
            'Open': 'first', 'High': 'max', 'Low': 'min',
            'Close': 'last', 'Volume': 'sum'
        }).dropna()

        close_w = weekly['Close']
        sma20_w = ta.sma(close_w, length=20)
        rsi_w   = ta.rsi(close_w, length=14)

        score = 0
        last_close = float(close_w.iloc[-1])
        last_sma   = float(sma20_w.iloc[-1]) if sma20_w is not None else 0.0
        last_rsi   = float(rsi_w.iloc[-1]) if rsi_w is not None else 0.0

        if last_close > last_sma > 0: score += 5
        if last_rsi > 50: score += 5

        status = "Bullish (W)" if score == 10 else "Neutral (W)" if score == 5 else "Bearish (W)"
        return {"mtf_score": score, "mtf_status": f"{status} | W-RSI: {last_rsi:.0f}"}
    except Exception:
        return {"mtf_score": 0, "mtf_status": "Error"}


def proximity_score(hist: pd.DataFrame) -> float:
    try:
        # ఒక సంవత్సరం (252 ట్రేడింగ్ రోజులు) డేటా లేకపోతే 0 ఇస్తుంది
        if len(hist) < 252:
            return 0.0

        # ఎర్రర్ ఫిక్స్: "Close" బదులు "High" వాడాము (నిజమైన 52W High కోసం)
        high_52w = float(hist["High"].tail(252).max())

        # ఎర్రర్ ఫిక్స్: "Close" బదులు "Low" వాడాము (నిజమైన 52W Low కోసం)
        low_52w = float(hist["Low"].tail(252).min())

        last = float(hist["Close"].iloc[-1])

        if high_52w <= 0:
            return 0.0

        # హై కి ఎంత దగ్గరగా ఉందో శాతం లెక్కించడం
        pfh = last / high_52w

        # 52-వీక్ రేంజ్ లో స్టాక్ ఎక్కడ ఉందో లెక్కించడం
        pfl = (last - low_52w) / (high_52w - low_52w + 1e-9)

        score = 0.0

        if pfh > 0.95:
            score += 15.0
        elif pfh > 0.90:
            score += 8.0
        elif pfh > 0.85:
            score += 4.0

        if pfl > 0.75:
            score += 5.0
        elif pfl > 0.60:
            score += 2.0

        return float(min(round(score, 2), 20.0))

    except Exception:
        return 0.0


def breakout_setup_score(hist: pd.DataFrame) -> float:
    try:
        if len(hist) < 30:
            return 0.0

        score = 0.0

        # కేవలం క్లోజింగ్ కాకుండా High, Low మరియు Close మూడింటినీ తీసుకుంటున్నాం
        highs = hist["High"].tail(30).values
        lows = hist["Low"].tail(30).values
        closes = hist["Close"].tail(30).values

        last_close = float(closes[-1])
        if last_close <= 0:
            return 0.0

        # ఎర్రర్ ఫిక్స్: గత 10 రోజుల అసలైన మార్కెట్ రేంజ్ (Highest High - Lowest Low)
        recent_high = highs[-10:].max()
        recent_low = lows[-10:].min()

        recent_range = (recent_high - recent_low) / last_close

        # రియల్ టైట్ రేంజ్: High - Low రేంజ్ 5% లోపల ఉంటేనే ఒరిజినల్ కన్సాలిడేషన్
        if recent_range < 0.05:
            score += 8.0

        # బ్రేక్అవుట్ కన్ఫర్మేషన్: ఈరోజు క్లోజింగ్.. గత 4 రోజుల 'High' ని దాటితేనే బ్రేక్అవుట్!
        if last_close > highs[-5:-1].max():
            score += 7.0

        return float(min(round(score, 2), 15.0))

    except Exception:
        return 0.0

def volume_trend_score(hist: pd.DataFrame) -> float:
    try:
        if len(hist) < 20:
            return 0.0

        # స్పీడ్ పెంచడానికి సిరీస్ కి బదులుగా Numpy Arrays వాడుతున్నాం
        vols = hist["Volume"].values

        avg20 = float(np.mean(vols[-20:]))
        avg60 = float(np.mean(vols[-60:])) if len(vols) >= 60 else avg20

        # ఇల్లిక్విడ్ (వాల్యూమ్ లేని) స్టాక్స్‌కి చెక్
        if avg20 <= 0:
            return 0.0

        last_v = float(vols[-1])
        score = 0.0

        # 1. వాల్యూమ్ స్పైక్ కన్ఫర్మేషన్
        if last_v > (avg20 * 1.3):
            score += 8.0
        elif last_v > avg20:
            score += 4.0

        # 2. అక్యుములేషన్ (Accumulation) ట్రెండ్ కన్ఫర్మేషన్
        if avg20 > avg60:
            score += 7.0
        elif avg20 > (avg60 * 0.9):
            score += 3.0

        return float(min(round(score, 2), 15.0))

    except Exception:
        return 0.0


def _safe_float(val, default=0.0) -> float:
    """
    ఎలాంటి డేటానైనా సేఫ్‌గా float రూపంలోకి మార్చడానికి హెల్పర్ ఫంక్షన్.
    """
    if val is None:
        return default
    try:
        # Pandas Series లేదా DataFrame వస్తే దాని చివరి వాల్యూ తీసుకోవాలి
        if hasattr(val, "iloc") and not val.empty:
            val = val.iloc[-1]
        elif hasattr(val, "item"):
            val = val.item()

        return float(val)
    except (ValueError, TypeError, IndexError):
        return default
# ════════════════════════════════════════════════════════════════════════════════
# DATA FETCH  — yfinance with multi-index fix, retry, and cache
# ════════════════════════════════════════════════════════════════════════════════
def fetch_stock_data(symbol: str, cache: dict, nifty_hist: pd.DataFrame,
                     sector_data_dict: dict, nse_master=None, institutional_ctx: dict | None = None) -> dict | None:
    """
    1-year డేటా మరియు ఫండమెంటల్స్ తెచ్చుకుని ఫైనల్ స్కోర్స్ క్యాలిక్యులేట్ చేసే ఫంక్షన్ (100% సేఫ్ & ఫాస్ట్).
    """
    try:
        ticker_symbol = symbol + ".NS"
        ticker = yf.Ticker(ticker_symbol)

        # ఫండమెంటల్ ఇన్ఫో తీసుకోవడం
        try:
            info = ticker.info
        except Exception:
            info = {}

        # 1. కేష్ ఫ్రెష్ గా ఉందా లేదా చెక్ చేయడం
        if is_cache_fresh(cache, symbol):
            cached = cache[symbol]["data"]
            hist = cached.get("history_data")

            if hist is None or not isinstance(hist, pd.DataFrame) or hist.empty:
                hist = _history_from_nse_master(nse_master, symbol=symbol, exchange="NSE", days=365, interval="1d")
                if hist.empty:
                    hist = _flatten_columns(ticker.history(period="1y"))
            else:
                # Cache may keep historical frame in some runs; refresh one-day candle.
                live_candle = _history_from_nse_master(nse_master, symbol=symbol, exchange="NSE", days=5, interval="1d")
                if live_candle.empty:
                    live_candle = _flatten_columns(ticker.history(period="1d"))
                if not live_candle.empty:
                    hist = pd.concat([hist[~hist.index.isin(live_candle.index)], live_candle])
        else:
            # 2. కేష్ లో లేకపోతే 1 సంవత్సరం డేటా డౌన్‌లోడ్ చేయడం
            hist = _history_from_nse_master(nse_master, symbol=symbol, exchange="NSE", days=365, interval="1d")
            if hist.empty:
                hist = _flatten_columns(ticker.history(period="1y"))

        # డేటా సరిగ్గా ఉందో లేదో చెక్ చేయడం
        if hist is None or hist.empty or len(hist) < 50:
            return None

        # ప్రైస్ మరియు ఇతర ఫండమెంటల్ వాల్యూస్ సురక్షితంగా తీసుకోవడం
        last_price = _safe_float(
            info.get("regularMarketPrice") or info.get("currentPrice") or hist["Close"].iloc[-1]
        )
        pe_ratio = _safe_float(info.get("trailingPE") or info.get("forwardPE"))
        eps_val = _safe_float(info.get("trailingEps"))
        roe_val = _safe_float(info.get("returnOnEquity"))
        if roe_val > 0 and roe_val < 5:  # కొన్నిసార్లు డెసిమల్ లో వస్తుంది
            roe_val = roe_val * 100

        sector_name = info.get("sector", "Unknown")
        sector_symbol = SECTOR_MAP.get(sector_name)
        sector_hist = sector_data_dict.get(sector_symbol) if sector_symbol else pd.DataFrame()

        # 3. ATR మరియు స్కోర్స్ లెక్కింపు
        atr_series = ta.atr(hist["High"], hist["Low"], hist["Close"], length=14)
        atr_val = float(atr_series.iloc[-1]) if atr_series is not None and not atr_series.empty else 0.0

        # SMC మాస్టర్ ఫంక్షన్ కి డేటా పంపడం
        smc = smc_entry_signal(hist, atr_val)
        mtf_data = weekly_trend_check(hist)
        inst_flow_factor = institutional_flow_factor_for_symbol(symbol, institutional_ctx or {})

        # ఫైనల్ రిజల్ట్ డిక్షనరీ (అన్ని కాలమ్స్ కచ్చితంగా ఉండేలా)
        result = {
            "symbol": symbol,
            "lastPrice": round(last_price, 2),
            "peRatio": round(pe_ratio, 2),
            "eps": round(eps_val, 2),
            "roe": round(roe_val, 2),
            "sector": sector_name,
            "history_data": hist,  # కేష్ కోసం
            "order_flow": order_flow_score(hist),
            "structure": structure_score(hist),
            "vcp": vcp_score(hist),
            "rs_vs_index": rs_vs_index_score(hist, nifty_hist, sector_hist),
            "mtf_score": mtf_data.get("mtf_score", 0),
            "mtf_status": mtf_data.get("mtf_status", "Neutral"),
            "inst_flow_factor": round(inst_flow_factor, 2),
            "proximity": proximity_score(hist),
            "breakout_setup": breakout_setup_score(hist),
            "volume_trend": volume_trend_score(hist),
            **smc,  # SMC రిజల్ట్స్
        }

        return result

    except Exception as e:
        print(f"⚠️ Error fetching data for {symbol}: {e}")
        return None
    # ...existing code...
# ════════════════════════════════════════════════════════════════════════════════
# SCORING ENGINE
# ════════════════════════════════════════════════════════════════════════════════

def score_stock_advanced(row: dict) -> dict:
    score = 0.0
    reasons = []

    # సురక్షితంగా నంబర్లను తీసుకునే ఫంక్షన్ (None వస్తే 0 ఇస్తుంది)
    def safe_val(key):
        val = row.get(key)
        try:
            return float(val)
        except (TypeError, ValueError):
            return 0.0

    # ఫండమెంటల్స్
    pe = safe_val("peRatio")
    if pe > 0:
        if 5 < pe < 20:
            score += 10; reasons.append(f"PE={pe:.1f} (deep value)")
        elif 20 <= pe < 30:
            score += 7; reasons.append(f"PE={pe:.1f} (fair value)")
        elif 30 <= pe < 50:
            score += 4; reasons.append(f"PE={pe:.1f} (growth premium)")

    roe = safe_val("roe")
    if roe > 0:
        if roe > 20:
            score += 10; reasons.append(f"ROE={roe:.1f}% (excellent)")
        elif roe > 15:
            score += 7; reasons.append(f"ROE={roe:.1f}% (good)")
        elif roe > 10:
            score += 4; reasons.append(f"ROE={roe:.1f}% (decent)")

    # టెక్నికల్స్
    rsi = safe_val("RSI")
    if rsi > 0:
        if 50 < rsi < 70:
            score += 10; reasons.append(f"RSI={rsi:.1f} (momentum)")
        elif 40 < rsi <= 50:
            score += 6; reasons.append(f"RSI={rsi:.1f} (healthy)")
        elif 30 < rsi <= 40:
            score += 4; reasons.append(f"RSI={rsi:.1f} (recovering)")

    macd = safe_val("MACD")
    macd_sig = safe_val("MACD_Signal")
    if macd and macd_sig:
        if macd > macd_sig and macd > 0:
            score += 10; reasons.append("MACD bullish+positive")
        elif macd > macd_sig:
            score += 6; reasons.append("MACD bullish crossover")

    adx = safe_val("ADX")
    if adx > 0:
        if adx > 30:
            score += 5; reasons.append(f"ADX={adx:.1f} (strong trend)")
        elif adx > 25:
            score += 3; reasons.append(f"ADX={adx:.1f} (trend forming)")

    # కస్టమ్ స్కోర్స్
    of = safe_val("order_flow")
    st = safe_val("structure")
    vcp = safe_val("vcp")
    rs = safe_val("rs_vs_index")
    prx = safe_val("proximity")
    bs = safe_val("breakout_setup")
    vt = safe_val("volume_trend")

    score += (of + st + vcp + rs + prx + bs + vt)

    if of > 10: reasons.append(f"OFI={of:.0f} (Inst. buying)")
    if st > 12: reasons.append(f"Struct={st:.0f} (Uptrend)")
    if vcp > 10: reasons.append(f"VCP={vcp:.0f} (Breakout ready)")
    if rs > 10: reasons.append(f"RS={rs:.0f} (Beating NIFTY)")
    if prx > 10: reasons.append(f"Prox={prx:.0f} (Near 52wHi)")
    if bs > 8: reasons.append(f"Breakout={bs:.0f} (Setup)")
    if vt > 6: reasons.append(f"VolTrend={vt:.0f} (Accumulation)")

    inst_flow = safe_val("inst_flow_factor")
    score += inst_flow
    if inst_flow > 0:
        reasons.append(f"Institutional Flow +{inst_flow:.1f}")
    elif inst_flow < 0:
        reasons.append(f"Institutional Flow {inst_flow:.1f}")

    # SMC కాన్సెప్ట్స్
    smc_conf = int(safe_val("confluence"))
    smc_sig = row.get("smc_signal", "NO_SETUP")
    score += smc_conf * 2

    if smc_sig == "HIGH_CONFLUENCE":
        reasons.append(f"SMC High Confluence ({smc_conf}/7)")
    elif smc_sig == "MODERATE_SETUP":
        reasons.append(f"SMC Moderate Setup ({smc_conf}/7)")

    if row.get("mss_confirmed"):
        reasons.append("MSS Confirmed")

    # ఫైబొనాచీ వాల్యూ ఉంటేనే చూపిస్తుంది, లేకపోతే ఎర్రర్ రాదు
    fib_val = row.get('fib_50')
    if row.get("in_fib_zone") and fib_val is not None:
        reasons.append(f"Discount Zone (50%={float(fib_val):.1f})")

    if row.get("displaced"):
        reasons.append("Displacement Vol Confirmed")

    # ఫైనల్ స్కోర్ క్యాలిక్యులేషన్ (Max 100)
    final_score = round(score / 1.64, 1)
    row["score"] = float(min(final_score, 100.0))

    # కారణాలను కలపడం
    row["reasons"] = " | ".join(reasons) if reasons else "No strong signals"

    return row
# ════════════════════════════════════════════════════════════════════════════════
# ENTRY REPORT PRINTER
# ════════════════════════════════════════════════════════════════════════════════

def print_entry_report(df: pd.DataFrame, top_n: int = 20) -> None:
    print("\n" + "=" * 100)
    print(f"  TOP {top_n} STOCKS BY COMPOSITE SCORE")
    print("=" * 100)

    for rank, (_, row) in enumerate(df.head(top_n).iterrows(), 1):
        # సేఫ్టీ గెట్స్ (ఏ కాలమ్ లేకపోయినా క్రాష్ అవ్వకుండా)
        sym = row.get('symbol', 'UNKNOWN')
        price = row.get('lastPrice', 0.0)
        score = row.get('score', 0.0)
        pe = row.get('peRatio', 0.0)
        roe = row.get('roe', 0.0)
        reasons = row.get('reasons', '')

        print(f"  {rank:2d}. {str(sym):12s}  Rs.{float(price):8.2f}  |  "
              f"Score:{float(score):6.1f}  |  PE:{float(pe):6.1f}  |  ROE:{float(roe):5.1f}%")
        print(f"      {str(reasons)[:115]}")
        print()

    # SMC సిగ్నల్స్ ఫిల్టర్ చేయడం
    if "smc_signal" in df.columns:
        smc_df = df[df["smc_signal"].isin(["HIGH_CONFLUENCE", "MODERATE_SETUP"])].copy()

        # స్కోరింగ్ కాలమ్స్ ఉన్నాయో లేదో చెక్ చేసి సార్ట్ చేయడం
        sort_cols = [c for c in ["smc_score", "score"] if c in smc_df.columns]
        if sort_cols:
            smc_df = smc_df.sort_values(sort_cols, ascending=False)
    else:
        smc_df = pd.DataFrame()

    if smc_df.empty:
        print("  ── No SMC setups detected in current scan ──\n")
        return

    print("=" * 100)
    print(f"  SMC PRECISION ENTRY REPORT  ({len(smc_df)} setups found)")
    print(f"  {'Symbol':<12} {'Signal':<18} {'Dir':<8} {'Entry':>9} {'SL':>9} "
          f"{'TP':>9} {'R:R':>5} {'Conf':>5}  Notes")
    print("-" * 100)

    for _, row in smc_df.iterrows():
        sym = row.get('symbol', 'UNKNOWN')
        entry = row.get("entry_price")
        sl = row.get("stop_loss")
        tp = row.get("take_profit")
        rr = row.get("rr_ratio")
        conf = row.get("confluence", 0)
        sig = row.get("smc_signal", "")
        dirn = row.get("smc_direction", "")

        notes = []
        if row.get("mss_confirmed"):  notes.append("MSS✓")
        if row.get("in_fib_zone"):    notes.append("FibDisc✓")
        if row.get("displaced"):      notes.append("Disp✓")

        fvg_mid = row.get("fvg_mid")
        if fvg_mid and pd.notna(fvg_mid):
            notes.append(f"FVG@{fvg_mid:.2f}")

        ob_low = row.get("ob_low")
        ob_high = row.get("ob_high")
        if ob_low and ob_high and pd.notna(ob_low) and pd.notna(ob_high):
            notes.append(f"OB[{ob_low:.2f}-{ob_high:.2f}]")

        entry_str = f"{entry:.2f}" if entry and pd.notna(entry) else 'N/A'
        sl_str = f"{sl:.2f}" if sl and pd.notna(sl) else 'N/A'
        tp_str = f"{tp:.2f}" if tp and pd.notna(tp) else 'N/A'
        rr_str = f"{rr:.2f}" if rr and pd.notna(rr) else 'N/A'

        print(f"  {str(sym):<12} {str(sig):<18} {str(dirn).upper():<8} "
              f"{entry_str:>9} "
              f"{sl_str:>9} "
              f"{tp_str:>9} "
              f"{rr_str:>5}  "
              f"{int(conf) if pd.notna(conf) else 0:>2}  {' | '.join(notes)}")

    print("=" * 100)
    print()
    print("  ENTRY EXECUTION GUIDE:")
    print("  • Entry  = FVG 50% fill OR Order Block mid (whichever offers better confluence)")
    print("  • SL     = Below sweep candle low − 0.5×ATR  (invalidates the setup)")
    print("  • TP     = Nearest swing high / liquidity pool above entry (min 2:1 R:R)")
    print("  • Filter = Only trade HIGH_CONFLUENCE signals with MSS + Displacement confirmed")
    print()
# ════════════════════════════════════════════════════════════════════════════════
# MAIN
# ════════════════════════════════════════════════════════════════════════════════

def get_nifty200_symbols():
    """
    NSE అఫీషియల్ వెబ్‌సైట్ నుండి నేరుగా లేటెస్ట్ NIFTY 200 లిస్ట్ డౌన్‌లోడ్ చేసే ఫంక్షన్.
    """
    print("  Fetching latest NIFTY 200 list from NSE...")
    try:
        # NSE వెబ్‌సైట్ లోని అధికారిక Nifty 200 CSV లింక్
        url = "https://archives.nseindia.com/content/indices/ind_nifty200list.csv"
        df = pd.read_csv(url)

        # 'Symbol' అనే కాలమ్ లో ఉన్న స్టాక్ పేర్లను లిస్ట్ గా మార్చడం
        symbols = df['Symbol'].tolist()
        print(f"  Successfully loaded {len(symbols)} stocks from NSE.\n")
        return symbols
    except Exception as e:
        print(f"  !! Failed to fetch NIFTY 200 list: {e}")
        # ఒకవేళ నెట్వర్క్ ఎర్రర్ వస్తే బ్యాకప్ కోసం కొన్ని ముఖ్యమైన స్టాక్స్
        return ["RELIANCE", "TCS", "HDFCBANK", "INFY", "ICICIBANK", "ITC", "SBIN", "LTIM"]


def main():
    # 1. లైవ్ లిస్ట్ ని ఫంక్షన్ ద్వారా తెచ్చుకుంటున్నాం
    symbols = get_nifty200_symbols()

    # ఒకవేళ లిస్ట్ లో డూప్లికేట్స్ ఉంటే తీసేయడానికి
    symbols = list(dict.fromkeys(symbols))

    cache = load_cache()
    cache.clear()  # ──> ఇది యాడ్ చేయండి (పాత కరప్ట్ అయిన డేటాని నాశనం చేస్తుంది)
    final_data = []
    ltp_max = 2000.0

    print("  Initializing NSE data sources...")
    nse_master, nse_utils = _init_nse_sources()
    institutional_ctx = build_institutional_context(nse_utils)

    # 2. బ్యానర్ ప్రింటింగ్
    print("\n" + "=" * 100)
    print("  ★  NIFTY 200 — SMC PRECISION ENTRY ENGINE  v5.1")
    print("     Modules: Liquidity Sweep | FVG | MSS | Order Block | Fib Zone | Displacement")
    print("     Legacy : OFI | HH/HL Structure | VCP | RS vs NIFTY | 52wHi Proximity")
    print("=" * 100)

    print(f"  Scanning {len(symbols)} stocks | {MAX_WORKERS} parallel workers | cache=12h\n")

    # 3. Fetch NIFTY 50 benchmark
    print("  Fetching NIFTY 50 for relative strength...")
    try:
        nifty_hist = _history_from_nse_master(nse_master, symbol="NIFTY 50", exchange="NSE", days=365, interval="1d")
        if nifty_hist.empty:
            nifty_hist = _flatten_columns(yf.Ticker("^NSEI").history(period="1y"))
        if nifty_hist.empty:
            print("  !! NIFTY 50 unavailable — RS scores will be 0")
            nifty_hist = pd.DataFrame()
        else:
            print(f"  NIFTY 50 loaded: {len(nifty_hist)} bars\n")
    except Exception as e:
        print(f"  !! NIFTY fetch failed: {e}")
        nifty_hist = pd.DataFrame()

    # ──> ఎర్రర్ ఫిక్స్: సెక్టార్ డేటా బ్లాక్ బయట పెట్టాం <──
    print("  Fetching Sectoral Indices for Advanced RS...")
    sector_data_dict = {}
    unique_sector_symbols = list(set(SECTOR_MAP.values()))

    for sec_sym in unique_sector_symbols:
        try:
            nse_index_name = SECTOR_SYMBOL_TO_NSE.get(sec_sym)
            if nse_index_name:
                sec_hist = _history_from_nse_master(
                    nse_master, symbol=nse_index_name, exchange="NSE", days=365, interval="1d"
                )
            else:
                sec_hist = pd.DataFrame()
            if sec_hist.empty:
                sec_hist = _flatten_columns(yf.Ticker(sec_sym).history(period="1y"))
            sector_data_dict[sec_sym] = sec_hist
        except Exception:
            sector_data_dict[sec_sym] = pd.DataFrame()
    print("  Sectoral data loaded successfully.\n")
    # ────────────────────────────────────────────────────────────


    # ── Parallel fetch ───────────────────────────────────────────────────────────
    processed_symbols = set()  # డూప్లికేట్స్ రాకుండా ట్రాక్ చేయడానికి

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(
                fetch_stock_data, sym, cache, nifty_hist, sector_data_dict, nse_master, institutional_ctx
            ): sym
            for sym in symbols
        }
        for i, future in enumerate(as_completed(futures), 1):
            sym = futures[future]

            # Ensure each symbol is handled exactly once, even if duplicate work slips in.
            if sym in processed_symbols:
                continue
            processed_symbols.add(sym)

            result = future.result()

            if result is None:
                print(f"  [{i:3d}/{len(symbols)}] {sym:15s} ✗ SKIPPED")
                continue

            result = score_stock_advanced(result)

            # Keep only serializable scalar values before storing/printing.
            clean_result = {
                k: v for k, v in result.items()
                if not isinstance(v, (pd.DataFrame, pd.Series))
            }
            clean_result["symbol"] = sym

            # Show/store only stocks with LTP strictly below 2000.
            ltp = float(clean_result.get("lastPrice", 0) or 0)
            if ltp >= ltp_max:
                continue

            final_data.append(clean_result)

            # Safely update the cache dict in memory
            cache[sym] = {
                "ts": datetime.now().isoformat(),
                "data": clean_result
            }
            # ───────────────────────────────────────────────────────────────


            smc_tag = clean_result.get("smc_signal", "NO_SETUP") or "NO_SETUP"
            smc_dir = (clean_result.get("smc_direction") or "")[:4].upper()

            # సేఫ్ ప్రింటింగ్ (None వస్తే ఎర్రర్ రాకుండా)
            score = clean_result.get('score', 0)
            last_price = clean_result.get('lastPrice', 0)

            print(
                f"  [{i:3d}/{len(symbols)}] {sym:15s} | "
                f"Score:{score:6.1f}/100 | "
                f"Rs.{last_price:8.2f} | "
                f"OFI:{clean_result.get('order_flow', 0):5.1f} "
                f"Str:{clean_result.get('structure', 0):5.1f} "
                f"VCP:{clean_result.get('vcp', 0):5.1f} "
                f"RS:{clean_result.get('rs_vs_index', 0):5.1f} "
                f"Prx:{clean_result.get('proximity', 0):5.1f} | "
                f"SMC:{smc_tag[:14]:<14} {smc_dir}"
            )

    print("\n  Saving cache to disk...")
    save_cache(cache)
    if not final_data:
        print(f"\n!! No stocks matched the LTP filter (< {ltp_max:.0f}).")
        return

    df = pd.DataFrame(final_data)

    # ──> బ్రహ్మాస్త్రం: ఏ కాలమ్ మిస్ అయినా క్రాష్ అవ్వకుండా డిఫాల్ట్ వాల్యూస్ వేస్తుంది <──
    default_columns = {
        'symbol': 'UNKNOWN',
        'lastPrice': 0.0,
        'score': 0.0,
        'peRatio': 0.0,
        'eps': 0.0,
        'RSI': 0.0,
        'order_flow': 0.0,
        'structure': 0.0,
        'vcp': 0.0,
        'rs_vs_index': 0.0,
        'proximity': 0.0,
        'mtf_score': 0.0,
        'smc_signal': 'NO_SETUP'
    }

    for col, default_val in default_columns.items():
        if col not in df.columns:
            df[col] = default_val
    # ───────────────────────────────────────────────────────────────────────────

    df = df.sort_values("score", ascending=False)

    # ── Save CSV (ఎక్సెల్ ఓపెన్ ఉన్నా క్రాష్ అవ్వకుండా) ─────────────────────────
    output_file = "nifty200_smc_precision_screen.csv"
    try:
        df.to_csv(output_file, index=False)
    except PermissionError:
        backup_file = "nifty200_smc_backup_screen.csv"
        print(f"\n  ⚠️ Warning: '{output_file}' is open. Saving to {backup_file} instead.")
        df.to_csv(backup_file, index=False)

    # ── Print reports ────────────────────────────────────────────────────────────
    try:
        print_entry_report(df, top_n=20)
    except NameError:
        pass  # print_entry_report ఫంక్షన్ లేకపోతే ఎర్రర్ రాకుండా ఇగ్నోర్ చేస్తుంది

    print("=" * 100)
    print(f"  Saved: {output_file}  ({len(df)} stocks)\n")
    print(f"  Score Distribution:")
    print(f"    Elite    80-100 : {len(df[df['score'] >= 80]):3d} stocks")
    print(f"    Strong   65-79  : {len(df[(df['score'] >= 65) & (df['score'] < 80)]):3d} stocks")
    print(f"    Moderate 50-64  : {len(df[(df['score'] >= 50) & (df['score'] < 65)]):3d} stocks")
    print(f"    Weak     35-49  : {len(df[(df['score'] >= 35) & (df['score'] < 50)]):3d} stocks")
    print(f"    No setup  <35   : {len(df[df['score'] < 35]):3d} stocks\n")

    smc_counts = df["smc_signal"].value_counts()
    print(f"  SMC Signal Distribution:")
    for sig, cnt in smc_counts.items():
        print(f"    {str(sig):<22}: {cnt:3d} stocks")

    print()
    print("  BEST COMBO (Minervini SEPA + SMC):")
    print("    RS ≥ 10  +  VCP ≥ 10  +  Proximity ≥ 10  +  SMC HIGH_CONFLUENCE")
    print("    Entry trigger: breakout candle on volume > 1.5× average, inside FVG/OB zone")
    print("=" * 100 + "\n")


if __name__ == "__main__":
    main()
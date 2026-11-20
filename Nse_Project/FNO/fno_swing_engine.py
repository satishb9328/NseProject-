"""
═══════════════════════════════════════════════════════════════════════
  FNO SWING TRADING ENGINE
═══════════════════════════════════════════════════════════════════════
  Clean, production-ready script for participant-based swing analysis.

  PIPELINE:
    CSVs → Derived Columns → Momentum → Cross-Participant
         → Calendar → SQLite → Daily Swing Report

  USAGE:
    python fno_swing_engine.py update    # Incremental SQLite upsert from CSVs
    python fno_swing_engine.py report    # Today's swing trading checklist
    python fno_swing_engine.py history   # Last 10 days signal summary
    python fno_swing_engine.py stats     # Signal accuracy over 3 years

  DJANGO INTEGRATION:
    from fno_swing_engine import SwingEngine
    engine = SwingEngine()
    context = engine.get_dashboard_context()
═══════════════════════════════════════════════════════════════════════
"""

import os
import re
import csv
import glob
import sqlite3
import sys
import datetime
from datetime import timedelta
from dataclasses import dataclass, field
from typing import Optional

import pandas as pd

# Holiday calendar lives in a sibling module so it can be extended via a
# ``holidays.json`` file (or a future API sync) without touching the engine.
try:
    from . import market_calendar
except ImportError:  # running as a top-level script (python fno_swing_engine.py)
    import market_calendar

# ══════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ══════════════════════════════════════════════════════════════════════

@dataclass
class Config:
    """Central configuration — edit paths & thresholds here."""
    csv_dir:        str = r'D:\FNO DATA\Participant data\OpenInterest'
    vol_csv_dir:    str = r'D:\FNO DATA\Participant data\Volume'
    db_path:        str = r'D:\FNO DATA\Participant data\Database\fno_swing.db'
    years_history:  int = 3          # Store 3 years (recommended sweet spot)

    # ── Dynamic percentile thresholds (regime-adaptive, replaces static %) ──
    percentile_window:    int   = 252       # 1-year rolling lookback for rank
    heavy_short_rank:     float = 0.90      # FII short rank >= this → heavy short
    extreme_short_rank:   float = 0.97      # FII short rank >= this → extreme short
    extreme_long_rank:    float = 0.05      # FII short rank <= this → extreme long
    extreme_oi_threshold: int   = 100_000   # FII/PRO "extreme" position size

    # ── Volume kicker (net-aggression divergence) ──
    volume_kicker_threshold: float = 40_000  # |net vol| beyond this triggers ±3

    # ── Smart money weights ──
    weight_fii:    int = 3    # Smartest money
    weight_pro:    int = 2    # Proprietary desks
    weight_dii:    int = 1    # Domestic institutions
    weight_client: int = 1    # Retail (contrarian)

    # ── Swing signal rules ──
    momentum_window:   int = 5     # Days for momentum average
    flow_window:       int = 3     # Days for cumulative flow


CFG = Config()

# ── NSE Market Holidays ──────────────────────────────────────────────
# Holiday data now lives in ``market_calendar.py`` (see that module's
# docstring). The calendar can be extended dynamically by dropping a
# ``holidays.json`` file next to the module, or by appending to
# ``_DEFAULT_HOLIDAYS`` in ``market_calendar.py``.

PARTICIPANTS = ['FII', 'DII', 'PRO', 'CLIENT']

RAW_COLUMNS = [
    'future_index_long', 'future_index_short',
    'future_stock_long', 'future_stock_short',
    'option_index_call_long', 'option_index_call_short',
    'option_index_put_long', 'option_index_put_short',
]


def is_working_day(dt: datetime.date) -> bool:
    """True if dt is an NSE trading day (weekday, not a holiday)."""
    return market_calendar.is_trading_day(dt)


def last_weekly_expiry(dt: datetime.date) -> datetime.date:
    """Last Thursday of the month (monthly F&O expiry)."""
    if dt.month == 12:
        last_day = datetime.date(dt.year, 12, 31)
    else:
        last_day = datetime.date(dt.year, dt.month + 1, 1) - timedelta(days=1)
    while last_day.weekday() != 3:  # 3 = Thursday
        last_day -= timedelta(days=1)
    return last_day


def safe_float(v) -> float:
    try:
        return float(str(v).replace(',', '').strip())
    except (ValueError, TypeError, AttributeError):
        return 0.0


# ══════════════════════════════════════════════════════════════════════
# 1. CSV LOADER
# ══════════════════════════════════════════════════════════════════════

def _date_from_filename(path: str) -> Optional[datetime.date]:
    """Extract the trade date embedded in a participant OI CSV filename."""
    match = re.search(r'(\d{8})', os.path.basename(path))
    if not match:
        return None
    for fmt in ('%Y%m%d', '%d%m%Y'):
        try:
            return datetime.datetime.strptime(match.group(1), fmt).date()
        except ValueError:
            continue
    return None


def _read_participant_oi_rows(path: str,
                              file_date: datetime.date) -> list[tuple[str, dict]]:
    """Parse one participant OI CSV into ``(client, row)`` pairs.

    Handles the preamble lines some NSE CSVs contain before the real header
    and stops as soon as all four participants are found. Returns ``[]`` for
    malformed files (mirroring the legacy silent-skip behaviour).
    """
    try:
        with open(path, 'r', encoding='utf-8', errors='ignore') as f:
            reader = csv.reader(f)

            # Scan for the real header row (NSE sometimes prepends comments).
            header = None
            for candidate in reader:
                if candidate and any(
                    str(h).strip().lower().startswith('client')
                    for h in candidate if h
                ):
                    header = [h.strip().lower() for h in candidate]
                    break
            if not header:
                return []

            idx = {h: i for i, h in enumerate(header) if h}
            rows = []
            seen = set()
            for row in reader:
                if not row or not row[0].strip():
                    continue
                client = str(row[0]).strip().upper()
                if client not in PARTICIPANTS or client in seen:
                    continue
                seen.add(client)

                rec = {'date': file_date}
                for col in RAW_COLUMNS:
                    header_name = ' '.join(
                        w.capitalize() for w in col.split('_')).lower()
                    i = idx.get(header_name)
                    rec[col] = (safe_float(row[i])
                                if i is not None and i < len(row) else 0.0)
                rows.append((client, rec))
                if len(seen) == len(PARTICIPANTS):
                    break
            return rows
    except Exception:
        return []


def load_all_participants(start_date=None, end_date=None) -> dict[str, list[dict]]:
    """Read every participant OI CSV **once** and bucket rows per participant.

    The previous implementation called ``load_participant_csvs`` once per
    participant, re-reading and re-parsing the entire directory 4×. This
    single pass cuts CSV I/O and parse work by ~4×.
    """
    buckets = {p: [] for p in PARTICIPANTS}
    pattern = os.path.join(CFG.csv_dir, 'participant_oi_*.csv')

    for path in sorted(glob.glob(pattern)):
        file_date = _date_from_filename(path)
        if file_date is None:
            continue
        if start_date and file_date < start_date:
            continue
        if end_date and file_date > end_date:
            continue
        if not is_working_day(file_date):
            continue

        for client, rec in _read_participant_oi_rows(path, file_date):
            buckets[client].append(rec)

    for client in buckets:
        buckets[client].sort(key=lambda r: r['date'])
    return buckets


def load_participant_csvs(client_type: str,
                          start_date: Optional[datetime.date] = None,
                          end_date: Optional[datetime.date] = None) -> list[dict]:
    """Load one participant's raw OI rows (thin wrapper over the single-pass loader)."""
    return load_all_participants(start_date, end_date)[client_type]


def _parse_volume_date(path: str) -> Optional[datetime.date]:
    """Parse a participant volume CSV filename into its trade date.

    NSE volume files are named ``participant_vol_DDMMYYYY.csv`` (day-month-
    year, unlike the YYYYMMDD OI files). Try DDMMYYYY first, then fall back
    to YYYYMMDD for any locally re-named files.
    """
    match = re.search(r'(\d{8})', os.path.basename(path))
    if not match:
        return None
    digits = match.group(1)
    for fmt in ('%d%m%Y', '%Y%m%d'):
        try:
            return datetime.datetime.strptime(digits, fmt).date()
        except ValueError:
            continue
    return None


def _net_aggression(row: dict) -> float:
    """Net aggression (buy − sell) for one participant's volume row.

    Expects snake_case keys (``future_index_long``, …) as produced by
    ``_read_participant_oi_rows`` and ``load_latest_volume_csv``.
    """
    net_fut = row.get('future_index_long', 0) - row.get('future_index_short', 0)
    net_opt = ((row.get('option_index_call_long', 0) + row.get('option_index_put_short', 0))
               - (row.get('option_index_put_long', 0) + row.get('option_index_call_short', 0)))
    return net_fut + net_opt


def load_all_volume_history() -> dict[str, dict[datetime.date, float]]:
    """Parse every participant volume CSV into daily net aggression.

    Returns ``{client: {date: net_volume}}``, where net aggression (buy −
    sell) = (Future Index Long − Short) + (Call Long + Put Short)
    − (Put Long + Call Short).
    """
    buckets: dict[str, dict[datetime.date, float]] = {p: {} for p in PARTICIPANTS}
    pattern = os.path.join(CFG.vol_csv_dir, '*participant_vol_*.csv')
    files = sorted(glob.glob(pattern))
    if not files:
        return buckets

    for path in files:
        file_date = _parse_volume_date(path)
        if file_date is None or not is_working_day(file_date):
            continue
        for client, rec in _read_participant_oi_rows(path, file_date):
            buckets[client][file_date] = _net_aggression(rec)

    return buckets


def load_latest_volume_csv() -> tuple[dict, Optional[str]]:
    """Loads the most recent participant volume CSV."""
    pattern = os.path.join(CFG.vol_csv_dir, '*participant_vol_*.csv')
    files = sorted(glob.glob(pattern))
    if not files:
        return {}, None

    # Sort by parsed trade date (DDMMYYYY filenames don't sort chronologically).
    dated = [(f, _parse_volume_date(f)) for f in files]
    dated = [(f, d) for f, d in dated if d is not None]
    if not dated:
        return {}, None
    latest_file, file_date = max(dated, key=lambda x: x[1])
    file_date_str = file_date.isoformat()

    data = {}
    try:
        with open(latest_file, 'r', encoding='utf-8', errors='ignore') as f:
            reader = csv.reader(f)
            headers = None
            for row in reader:
                if not row or not row[0].strip(): continue
                if str(row[0]).strip().lower() == 'client type':
                    headers = [h.strip().lower() for h in row]
                    continue
                if headers and str(row[0]).strip().upper() in PARTICIPANTS:
                    client = str(row[0]).strip().upper()
                    record = {}
                    for i, h in enumerate(headers):
                        if i < len(row):
                            record[h.replace(' ', '_')] = safe_float(row[i])
                    data[client] = record
    except Exception:
        pass
    return data, file_date_str


def calculate_net_volume(data_dict: dict) -> dict:
    """Calculates Net Aggression (Buy - Sell) from Volume CSV."""
    return {client: _net_aggression(row) for client, row in data_dict.items()}


def get_tomorrow_matrix(
        fii_oi: float,
        fii_vol: float,
        pro_oi: float,
        pro_vol: float,
) -> tuple[str, str, str]:
    """Merge FII/PRO positioning and volume, flagging material divergence."""
    if fii_oi * pro_oi < 0:
        smaller_position = min(abs(fii_oi), abs(pro_oi))
        larger_position = max(abs(fii_oi), abs(pro_oi))
        balanced_positions = (
            smaller_position >= CFG.extreme_oi_threshold
            and smaller_position / larger_position >= 0.5
        )
        if balanced_positions:
            volume_context = (
                " Both volume flows confirm their respective positions."
                if fii_oi * fii_vol > 0 and pro_oi * pro_vol > 0
                else " Volume does not fully confirm the opposing OI positions."
            )
            return (
                "MIXED POSITIONING",
                "⚖️ MIXED / NEUTRAL (FII/PRO divergence)",
                "FII and PRO hold substantial opposing positions."
                f"{volume_context} Avoid a directional trade until "
                "positioning aligns.",
            )

    if fii_oi < 0 and fii_vol > 0:
        return "SHORT COVERING", "🟢 STRONG BULLISH (Gap-Up/Squeeze)", "FII were Short but bought heavily today. Trapped! Expect panic buying tomorrow. Buy CE at open/dips."
    elif fii_oi > 0 and fii_vol > 0:
        return "LONG BUILD-UP", "🟢 BULLISH (Continuation)", "Fresh buying alongside existing longs. Trend is strong. Buy CE on dips."
    elif fii_oi > 0 and fii_vol < 0:
        return "LONG UNWINDING", "🔴 BEARISH (Topping/Reversal)", "FII booking profits on longs. Upside capped. Buy PE on rallies."
    elif fii_oi < 0 and fii_vol < 0:
        return "ADDING SHORTS", "🔴 STRONG BEARISH (Breakdown)", "Fresh shorting alongside existing shorts. Do NOT buy CE. Buy PE on rallies."
    return "NEUTRAL / CHOPPY", "⚖️ SIDEWAYS", "No clear institutional aggression. Avoid directional option buying."


# ══════════════════════════════════════════════════════════════════════
# 2. DERIVED COLUMNS (per participant)
# ══════════════════════════════════════════════════════════════════════

def compute_derived(rows: list[dict]) -> list[dict]:
    """Compute all Tier 2 + Tier 3 derived columns for one participant.

    Tier 2 (levels):   FNL, FNS, NPF, NCI, NPI, NOPO, NFNO, SF_NET,
                       fii_short_pct, long_pct, short_pct, ls_ratio
    Tier 3 (deltas):   nfno_delta, nopo_delta, npf_delta
    Tier 3 (momentum): fil_3d, fis_3d, nfno_5d_avg, nfno_5d_mom,
                       short_pct_3d_trend

    Fully vectorized with Pandas (``.diff()`` / ``.shift()`` /
    ``.rolling()``) and converted back to the same ``list[dict]`` shape the
    rest of the pipeline expects.
    """
    if not rows:
        return []

    df = pd.DataFrame(rows)  # columns: date + RAW_COLUMNS

    fil = df['future_index_long']
    fis = df['future_index_short']
    fsl = df['future_stock_long']
    fss = df['future_stock_short']
    oicl = df['option_index_call_long']
    oics = df['option_index_call_short']
    oipl = df['option_index_put_long']
    oips = df['option_index_put_short']

    # ── Tier 2: levels / flows ──
    # Day-over-day change in raw long/short. The first row has no prior
    # day, so it is filled with the raw level (matching the legacy code's
    # ``prev or 0`` behaviour).
    df['fnl'] = fil.diff().fillna(fil)
    df['fns'] = fis.diff().fillna(fis)
    df['npf'] = fil - fis
    df['nci'] = oicl - oips       # Call Long − Put Short  (bullish options)
    df['npi'] = oipl - oics       # Put Long − Call Short  (bearish options)
    df['nopo'] = df['nci'] - df['npi']
    df['nfno'] = df['npf'] + df['nopo']        # ★ MASTER SWING SIGNAL
    df['sf_net'] = fsl - fss

    total_fut = fil + fis
    # Guarded divisions → 0.0 when the denominator is zero, mirroring the
    # legacy ``if denominator else 0.0`` branches.
    df['short_pct'] = (fis * 100.0).div(total_fut.where(total_fut > 0)).fillna(0.0)
    df['long_pct'] = (df['fnl'] * 100.0).div(fil.where(fil != 0)).fillna(0.0)
    df['short_delta_pct'] = (df['fns'] * 100.0).div(fis.where(fis != 0)).fillna(0.0)
    df['ls_ratio'] = fil.div(fis.where(fis != 0)).round(2).fillna(0.0)

    # ── Tier 3: day-over-day deltas ──
    df['nfno_delta'] = df['nfno'].diff()
    df['nopo_delta'] = df['nopo'].diff()
    df['npf_delta'] = df['npf'].diff()

    # ── Tier 3: multi-day momentum ──
    n3, n5 = CFG.flow_window, CFG.momentum_window
    df['fil_3d'] = fil - fil.shift(n3)
    df['fis_3d'] = fis - fis.shift(n3)
    df['short_pct_3d_trend'] = (df['short_pct'] - df['short_pct'].shift(n3)).round(2)
    df['nfno_5d_avg'] = df['nfno'].rolling(window=n5).mean()
    df['nfno_5d_mom'] = df['nfno'] - df['nfno_5d_avg']

    # DII combined position and 20-day rolling deviation baseline.
    df['dii_combined'] = df['npf'] + df['nopo']
    df['dii_combined_20d_mean'] = (
        df['dii_combined'].rolling(window=20, min_periods=5).mean()
    )

    # ── Tier 4: dynamic percentile rank of short_pct (1-year lookback) ──
    # Vectorized rolling percentile rank: for each day, the rank of today's
    # short_pct within its trailing window (0..1, method='max' → P(X <= x)).
    # NaN until the window fills, which downstream treats as neutral (0.5).
    df['short_percentile'] = (
        df['short_pct'].rolling(window=CFG.percentile_window,
                                min_periods=CFG.percentile_window)
        .rank(method='max', pct=True)
    )

    # ── Convert back to the legacy ``list[dict]`` shape ──
    # Residual NaN → None so downstream dict/SQLite code sees the same
    # "not yet available" sentinel as before.
    records = []
    for rec in df.to_dict('records'):
        records.append({k: (None if pd.isna(v) else v) for k, v in rec.items()})
    return records



# ══════════════════════════════════════════════════════════════════════
# 3. CALENDAR FEATURES (Tier 5)
# ══════════════════════════════════════════════════════════════════════

def add_calendar_features(rows: list[dict]) -> list[dict]:
    """Annotate each row with calendar context for swing timing."""
    for rec in rows:
        d = rec['date']
        expiry = last_weekly_expiry(d)   # monthly expiry for this month
        days_to_expiry = (expiry - d).days

        rec['day_of_week']    = d.weekday()          # 0=Mon … 4=Fri
        rec['month']          = d.month
        rec['days_to_expiry'] = days_to_expiry
        rec['is_expiry_week'] = 0 <= days_to_expiry <= 5
        # Week-of-month: 1st/2nd/3rd/4th week
        rec['week_of_month']  = min((d.day - 1) // 7 + 1, 5)

    return rows


# ══════════════════════════════════════════════════════════════════════
# 4. CROSS-PARTICIPANT SIGNALS (Tier 4)
# ══════════════════════════════════════════════════════════════════════

@dataclass
class DaySignals:
    """All cross-participant signals for a single date."""
    date: datetime.date
    fii_npf: float = 0
    pro_nopo: float = 0
    dii_combined: float = 0
    client_nfno: float = 0
    fii_short_pct: float = 0.0

    fii_net_vol: float = 0.0      # net aggression (buy - sell)
    pro_net_vol: float = 0.0
    dii_net_vol: float = 0.0
    client_net_vol: float = 0.0

    smart_money_score: int = 0
    retail_trap: bool = False
    fii_pro_aligned: bool = False
    alignment_direction: str = 'NEUTRAL'   # BULLISH / BEARISH / NEUTRAL
    fii_client_diverged: bool = False
    institutional_strength: float = 0

    ai_sentiment: str = '⚖️ NEUTRAL'
    ai_confidence: int = 55


def compute_day_signals(fii: dict, dii: dict, pro: dict,
                        client: dict,
                        fii_net_vol: float = 0.0,
                        pro_net_vol: float = 0.0,
                        dii_net_vol: float = 0.0,
                        client_net_vol: float = 0.0) -> DaySignals:
    """Compute all cross-participant signals for one date.

    All inputs are derived-row dicts for that date (may be empty dicts).
    """
    s = DaySignals(date=fii.get('date') or dii.get('date')
                        or pro.get('date') or client.get('date'))

    fii_npf  = fii.get('npf', 0) or 0
    fii_spc  = fii.get('short_pct', 0.0) or 0.0
    pro_nopo = pro.get('nopo', 0) or 0
    pro_npf  = pro.get('npf', 0) or 0
    client_nfno = client.get('nfno', 0) or 0

    dii_combined = (
        (dii.get('npf', 0) or 0) + (dii.get('nopo', 0) or 0)
    )

    s.fii_npf = fii_npf
    s.fii_short_pct = fii_spc
    s.pro_nopo = pro_nopo
    s.dii_combined = dii_combined
    s.client_nfno = client_nfno

    s.fii_net_vol = fii_net_vol
    s.pro_net_vol = pro_net_vol
    s.dii_net_vol = dii_net_vol
    s.client_net_vol = client_net_vol

    # Dynamic percentile rank of FII short% (1-year lookback); neutral (0.5)
    # until enough history exists to compute it.
    fii_short_rank = fii.get('short_percentile')
    if fii_short_rank is None:
        fii_short_rank = 0.5

    # ── Alignment detection ──
    fii_dir = (fii_npf > 0) - (fii_npf < 0)      # +1 / 0 / -1
    pro_dir = (pro_nopo > 0) - (pro_nopo < 0)

    s.fii_pro_aligned = (fii_dir != 0 and fii_dir == pro_dir)
    if s.fii_pro_aligned:
        s.alignment_direction = 'BULLISH' if fii_dir > 0 else 'BEARISH'

    # ── Divergence / Retail Trap ──
    s.fii_client_diverged = (
        (fii_npf < 0 and client_nfno > 0) or
        (fii_npf > 0 and client_nfno < 0)
    )
    heavy_short   = fii_npf < 0 and fii_short_rank >= CFG.heavy_short_rank
    pro_short     = (pro_nopo + pro_npf) < 0
    client_long   = client_nfno > 0
    s.retail_trap = heavy_short and pro_short and client_long

    # ── Magnitude-weighted smart money score ──
    score = 0
    fii_momentum = fii.get('nfno_5d_mom')
    if fii_npf > 0:
        score += 3
    elif fii_npf < 0:
        score += -1 if fii_momentum is not None and fii_momentum > 0 else -3

    # FII dynamic percentile positioning bonus/penalty (regime-adaptive)
    if fii_short_rank >= CFG.extreme_short_rank:
        score -= 2
    elif fii_short_rank >= CFG.heavy_short_rank:
        score -= 1
    elif fii_short_rank <= CFG.extreme_long_rank:
        score += 2
    elif fii_short_rank <= 0.25:
        score += 1

    pro_combined = pro_nopo + pro_npf
    score += CFG.weight_pro * ((pro_combined > 0) - (pro_combined < 0))
    dii_20d_mean = dii.get('dii_combined_20d_mean')
    if dii_20d_mean is not None:
        dii_deviation = dii_combined - dii_20d_mean
        if dii_deviation > 15_000:
            score += 1
        elif dii_deviation < -15_000:
            score -= 1
    score -= CFG.weight_client * ((client_nfno > 0) - (client_nfno < 0))

    # ── Volume kicker: OI vs net-aggression divergence ──
    # FII net short on OI but heavy net buying in volume = major short
    # covering (+3). FII net long but heavy net selling = major long
    # unwinding (-3).
    if fii_npf < 0 and fii_net_vol > CFG.volume_kicker_threshold:
        score += 3
    elif fii_npf > 0 and fii_net_vol < -CFG.volume_kicker_threshold:
        score -= 3

    s.smart_money_score = score
    s.institutional_strength = abs(fii_npf) + abs(pro_combined)

    # ── Sentiment determination ──
    confidence = min(95, 55 + abs(score) * 4)

    fii_extreme_size = abs(fii_npf) > CFG.extreme_oi_threshold
    pro_extreme_size = abs(pro_combined) > CFG.extreme_oi_threshold
    fii_pro_disagree = (fii_npf * pro_combined < 0) and \
                       (fii_extreme_size or pro_extreme_size)

    if s.retail_trap:
        s.ai_sentiment, s.ai_confidence = \
            '⚠️ CRITICAL RETAIL TRAP (DEEP BEARISH)', 95
    elif fii_pro_disagree and abs(score) <= 3:
        s.ai_sentiment, s.ai_confidence = \
            '⚖️ MIXED SIGNALS — CAUTION', max(confidence, 72)
    elif score <= -4:
        s.ai_sentiment, s.ai_confidence = \
            '⚠️ DEEP BEARISH (Smart Money Selling)', max(confidence, 88)
    elif score >= 4 and fii_npf > 0:
        s.ai_sentiment, s.ai_confidence = '🚀 STRONG BULLISH', max(confidence, 84)
    elif score >= 4 and fii_npf < 0:
        s.ai_sentiment, s.ai_confidence = \
            '🟡 BULLISH WITH CAUTION (FII Short)', max(confidence, 70)
    elif score <= -2:
        s.ai_sentiment, s.ai_confidence = '🔴 BEARISH', max(confidence, 75)
    elif score >= 2:
        s.ai_sentiment, s.ai_confidence = '🟢 BULLISH', max(confidence, 70)
    else:
        s.ai_sentiment, s.ai_confidence = '⚖️ NEUTRAL', max(confidence, 60)

    return s


def compute_cross_participant(
        data: dict[str, list[dict]],
        volume_history: dict[str, dict[datetime.date, float]] = None,
) -> list[DaySignals]:
    """Align all 4 participants by date and compute signals per day.

    ``volume_history`` maps client → date → net aggression; missing dates
    default to 0.0 (volume data not available yet).
    """
    volume_history = volume_history or {p: {} for p in PARTICIPANTS}

    # Build date → row maps
    by_date = {
        p: {r['date']: r for r in rows}
        for p, rows in data.items()
    }
    all_dates = sorted(set().union(*[set(m.keys()) for m in by_date.values()]))

    signals = []
    for d in all_dates:
        fii    = by_date['FII'].get(d, {})
        dii    = by_date['DII'].get(d, {})
        pro    = by_date['PRO'].get(d, {})
        client = by_date['CLIENT'].get(d, {})

        # Only compute when at least FII data exists (primary participant)
        if not fii:
            continue
        signals.append(compute_day_signals(
            fii, dii, pro, client,
            fii_net_vol=volume_history['FII'].get(d, 0.0),
            pro_net_vol=volume_history['PRO'].get(d, 0.0),
            dii_net_vol=volume_history['DII'].get(d, 0.0),
            client_net_vol=volume_history['CLIENT'].get(d, 0.0),
        ))

    return signals


# ══════════════════════════════════════════════════════════════════════
# 5. SWING SIGNAL GENERATOR (the actual trade logic)
# ══════════════════════════════════════════════════════════════════════

@dataclass
class SwingSignal:
    date: datetime.date
    bias: str            # LONG / SHORT / NO-TRADE
    strength: str        # STRONG / MODERATE / WEAK
    entry_note: str
    stop_note: str
    warnings: list = field(default_factory=list)


def generate_swing_signal(day: DaySignals, fii_row: dict) -> SwingSignal:
    """Convert one day's signals into an actionable swing trade plan."""
    warnings = []
    score = day.smart_money_score
    momentum = fii_row.get('nfno_5d_mom')

    # ── Hard blocks (NO-TRADE conditions) ──
    if day.retail_trap:
        return SwingSignal(
            date=day.date, bias='NO-TRADE', strength='—',
            entry_note='RETAIL TRAP ACTIVE. Longs forbidden. '
                       'Consider shorts only on rally to resistance.',
            stop_note='If short: stop above yesterday high.',
            warnings=['🚨 Retail trap: FII+PRO short, Client long'])

    if day.ai_sentiment.startswith('⚖️ MIXED'):
        return SwingSignal(
            date=day.date, bias='NO-TRADE', strength='—',
            entry_note='FII/PRO disagreement. Stay flat until alignment.',
            stop_note='—',
            warnings=['⚡ Smart money in disagreement'])

    # ── Warning collection (dynamic percentile rank) ──
    short_rank = fii_row.get('short_percentile')
    if short_rank is not None:
        if short_rank >= CFG.extreme_short_rank:
            warnings.append(f'🔥 FII extreme short (rank {short_rank:.0%}) — '
                            f'squeeze risk on any bounce')
        elif short_rank >= CFG.heavy_short_rank:
            warnings.append(f'⚠️ FII heavy short (rank {short_rank:.0%})')

    if fii_row.get('is_expiry_week'):
        warnings.append('📅 Expiry week — use wider stops (×1.5)')

    # Momentum flip detection
    if momentum is not None:
        if momentum < 0 and day.fii_npf > 0:
            warnings.append('📉 NFNO momentum turning down despite net long — '
                            'FII reducing exposure')
        elif momentum > 0 and day.fii_npf < 0:
            warnings.append('📈 NFNO momentum turning up despite net short — '
                            'FII adding exposure')

    # ── Bias determination ──
    if score >= 4:
        bias, strength = 'LONG', 'STRONG'
    elif score >= 2:
        bias, strength = 'LONG', 'MODERATE'
        if short_rank is not None and short_rank >= CFG.heavy_short_rank:
            strength = 'WEAK'
    elif score <= -4:
        bias, strength = 'SHORT', 'STRONG'
    elif score <= -2:
        bias, strength = 'SHORT', 'MODERATE'
    else:
        bias, strength = 'NO-TRADE', '—'

    # ── Entry / stop notes ──
    if bias == 'LONG':
        entry_note = (f'Buy dips to support. Score={score:+d}, '
                      f'FII short={day.fii_short_pct:.0f}%. '
                      f'{"Aligned with PRO ✓" if day.fii_pro_aligned else "PRO not aligned — half size"}')
        stop_note = ('Stop below 2-day low. Trail after +1% move.'
                     if strength == 'STRONG' else
                     'Half position. Stop below yesterday low.')
    elif bias == 'SHORT':
        entry_note = (f'Sell rallies to resistance. Score={score:+d}. '
                      f'{"Aligned with PRO ✓" if day.fii_pro_aligned else "PRO not aligned — half size"}')
        stop_note = ('Stop above 2-day high. Trail after -1% move.'
                     if strength == 'STRONG' else
                     'Half position. Stop above yesterday high.')
    else:
        entry_note = f'Neutral zone (score={score:+d}). Wait for ±2 break.'
        stop_note = '—'

    return SwingSignal(day.date, bias, strength, entry_note, stop_note, warnings)


# ══════════════════════════════════════════════════════════════════════
# 6. SQLITE STORAGE
# ══════════════════════════════════════════════════════════════════════

DB_SCHEMA = """
CREATE TABLE IF NOT EXISTS participant_derived (
    date         TEXT NOT NULL,
    client_type  TEXT NOT NULL,
    {raw_cols},
    fnl REAL, fns REAL, npf REAL,
    nci REAL, npi REAL, nopo REAL, nfno REAL, sf_net REAL,
    short_pct REAL, long_pct REAL, short_delta_pct REAL, ls_ratio REAL,
    nfno_delta REAL, nopo_delta REAL, npf_delta REAL,
    fil_3d REAL, fis_3d REAL, nfno_5d_avg REAL, nfno_5d_mom REAL,
    short_pct_3d_trend REAL,
    short_percentile REAL,
    day_of_week INT, month INT, days_to_expiry INT,
    is_expiry_week INT, week_of_month INT,
    PRIMARY KEY (date, client_type)
);

CREATE TABLE IF NOT EXISTS cross_signals (
    date TEXT PRIMARY KEY,
    fii_npf REAL, pro_nopo REAL, dii_combined REAL, client_nfno REAL,
    fii_short_pct REAL,
    fii_net_vol REAL, pro_net_vol REAL, dii_net_vol REAL, client_net_vol REAL,
    smart_money_score INT, retail_trap INT,
    fii_pro_aligned INT, alignment_direction TEXT,
    fii_client_diverged INT, institutional_strength REAL,
    ai_sentiment TEXT, ai_confidence INT
);

CREATE INDEX IF NOT EXISTS idx_pd_date ON participant_derived(date);
CREATE INDEX IF NOT EXISTS idx_cs_date ON cross_signals(date);
""".format(raw_cols=',\n    '.join(f'{c} REAL' for c in RAW_COLUMNS))


class SwingDB:
    """Thin SQLite wrapper for the swing engine."""

    def __init__(self, db_path: str = None):
        self.path = db_path or CFG.db_path
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        self.conn = sqlite3.connect(self.path)
        # WAL + NORMAL: faster single-writer commits, safe concurrent readers.
        self.conn.execute('PRAGMA journal_mode=WAL')
        self.conn.execute('PRAGMA synchronous=NORMAL')
        self.conn.executescript(DB_SCHEMA)
        self.conn.commit()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        if exc_type is None:
            self.conn.commit()
        self.conn.close()
        return False

    def save(self, derived: dict[str, list[dict]],
             signals: list[DaySignals]):
        """Incrementally upsert derived + signal rows.

        Uses ``INSERT OR REPLACE`` keyed on each table's primary key so that
        new dates are appended and an already-stored date is refreshed
        in-place. Nothing is deleted — historical rows are preserved.
        """
        cur = self.conn.cursor()

        pd_cols = (['date', 'client_type'] + RAW_COLUMNS + [
            'fnl', 'fns', 'npf', 'nci', 'npi', 'nopo', 'nfno', 'sf_net',
            'short_pct', 'long_pct', 'short_delta_pct', 'ls_ratio',
            'nfno_delta', 'nopo_delta', 'npf_delta',
            'fil_3d', 'fis_3d', 'nfno_5d_avg', 'nfno_5d_mom',
            'short_pct_3d_trend', 'short_percentile',
            'day_of_week', 'month', 'days_to_expiry',
            'is_expiry_week', 'week_of_month',
        ])
        pd_ph = ','.join('?' * len(pd_cols))
        pd_sql = (f'INSERT OR REPLACE INTO participant_derived '
                  f'({",".join(pd_cols)}) VALUES ({pd_ph})')

        pd_rows = []
        for client, rows in derived.items():
            for r in rows:
                vals = []
                for c in pd_cols:
                    if c == 'client_type':
                        v = client
                    elif c == 'date':
                        v = r['date'].isoformat()
                    else:
                        v = r.get(c)
                    if isinstance(v, bool):
                        v = int(v)
                    vals.append(v)
                pd_rows.append(vals)

        if pd_rows:
            cur.executemany(pd_sql, pd_rows)

        cs_cols = ['date', 'fii_npf', 'pro_nopo', 'dii_combined',
                   'client_nfno', 'fii_short_pct',
                   'fii_net_vol', 'pro_net_vol', 'dii_net_vol', 'client_net_vol',
                   'smart_money_score',
                   'retail_trap', 'fii_pro_aligned', 'alignment_direction',
                   'fii_client_diverged', 'institutional_strength',
                   'ai_sentiment', 'ai_confidence']
        cs_ph = ','.join('?' * len(cs_cols))
        cs_sql = (f'INSERT OR REPLACE INTO cross_signals '
                  f'({",".join(cs_cols)}) VALUES ({cs_ph})')

        cs_rows = [
            (s.date.isoformat(), s.fii_npf, s.pro_nopo, s.dii_combined,
             s.client_nfno, s.fii_short_pct,
             s.fii_net_vol, s.pro_net_vol, s.dii_net_vol, s.client_net_vol,
             s.smart_money_score,
             int(s.retail_trap), int(s.fii_pro_aligned),
             s.alignment_direction, int(s.fii_client_diverged),
             s.institutional_strength, s.ai_sentiment, s.ai_confidence)
            for s in signals
        ]
        if cs_rows:
            cur.executemany(cs_sql, cs_rows)

        self.conn.commit()

    def latest_signals(self, n: int = 10) -> list[dict]:
        cur = self.conn.execute(
            'SELECT * FROM cross_signals ORDER BY date DESC LIMIT ?', (n,))
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]

    def latest_participant(self, client: str, n: int = 1) -> list[dict]:
        cur = self.conn.execute(
            'SELECT * FROM participant_derived WHERE client_type=? '
            'ORDER BY date DESC LIMIT ?', (client, n))
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]

    def latest_stored_date(self) -> Optional[datetime.date]:
        """Newest date already persisted (across both tables), or None."""
        row = self.conn.execute(
            'SELECT MAX(date) FROM participant_derived').fetchone()
        val = row[0] if row else None
        if val is None:
            row = self.conn.execute(
                'SELECT MAX(date) FROM cross_signals').fetchone()
            val = row[0] if row else None
        return datetime.date.fromisoformat(val) if val else None

    def close(self):
        try:
            self.conn.close()
        except sqlite3.Error:
            pass


def get_eod_participant_bias() -> Optional[dict]:
    """Return the latest EOD participant setup for the pre-open trap matrix.

    Single read-only entry point for ``Live_dashboard.views``: exposes the
    newest ``cross_signals`` row (FII Net OI ``fii_npf``, FII Net Vol
    ``fii_net_vol``, ``retail_trap``) and derives ``action_type`` /
    ``tomorrow_bias`` with the same ``get_tomorrow_matrix()`` the daily FNO
    report uses. Returns ``None`` when the database is missing or empty so the
    caller can degrade to a neutral pre-open read instead of raising.
    """
    if not os.path.isfile(CFG.db_path):
        return None

    db = SwingDB(CFG.db_path)
    try:
        sigs = db.latest_signals(1)
        if not sigs:
            return None
        latest = sigs[0]

        fii_net_oi = latest.get('fii_npf') or 0
        fii_net_vol = latest.get('fii_net_vol') or 0

        # PRO totals feed get_tomorrow_matrix's divergence guard (same recipe
        # as SwingEngine.report() and FNO/views._load_daily_report()).
        pro_rows = db.latest_participant('PRO', 1)
        pro_row = pro_rows[0] if pro_rows else {}
        if pro_row.get('date') != latest.get('date'):
            pro_row = {}
        pro_total_oi = (pro_row.get('nopo') or 0) + (pro_row.get('npf') or 0)
        pro_net_vol = latest.get('pro_net_vol') or 0

        action_type, tomorrow_bias, tomorrow_note = get_tomorrow_matrix(
            fii_net_oi, fii_net_vol, pro_total_oi, pro_net_vol
        )

        return {
            'date': latest.get('date'),
            'fii_net_oi': fii_net_oi,
            'fii_net_vol': fii_net_vol,
            'retail_trap': bool(latest.get('retail_trap')),
            'action_type': action_type,
            'tomorrow_bias': tomorrow_bias,
            'tomorrow_note': tomorrow_note,
            'pro_net_oi': pro_total_oi,
            'pro_net_vol': pro_net_vol,
            'smart_money_score': latest.get('smart_money_score'),
            'fii_short_pct': latest.get('fii_short_pct'),
            'ai_sentiment': latest.get('ai_sentiment'),
            'alignment_direction': latest.get('alignment_direction'),
        }
    finally:
        db.close()


# ══════════════════════════════════════════════════════════════════════
# 7. HIGH-LEVEL ENGINE API
# ══════════════════════════════════════════════════════════════════════

class SwingEngine:
    """Main entry point. Use for CLI or import into Django."""

    def __init__(self, config: Config = None):
        global CFG
        if config:
            CFG = config

    # ── Incremental update: CSV → derived → signals → SQLite ──
    def update(self) -> dict:
        today = datetime.date.today()
        cutoff = today - timedelta(days=365 * CFG.years_history)

        if not os.path.isdir(CFG.csv_dir):
            print(f'❌ CSV directory not found: {CFG.csv_dir}')
            print('   Download participant OI CSVs first '
                  '(fetch_participent_oi_vol.py), then re-run update.')
            return {}

        # Find the newest date already persisted so we only fetch / process
        # data newer than it. A small warm-up buffer keeps the rolling
        # momentum windows accurate for the first new row.
        try:
            with SwingDB() as db:
                last_date = db.latest_stored_date()
        except (sqlite3.Error, OSError) as exc:
            print(f'❌ Could not open database {CFG.db_path}: {exc}')
            return {}

        if last_date is None:
            start = cutoff
            mode = 'full'
        else:
            # short_percentile needs a full 1-year lookback, so warm up far
            # enough to recompute it correctly for the new rows.
            warmup = timedelta(days=CFG.percentile_window * 2)
            start = max(cutoff, last_date - warmup)
            mode = 'incremental'

        print(f'Loading CSVs from {CFG.csv_dir} '
              f'({mode}, since {start.isoformat()})...')
        data = load_all_participants(start_date=start)

        for p, rows in data.items():
            print(f'  {p:8s}: {len(rows):4d} trading days loaded')

        if not any(data.values()):
            print('❌ No data found. Check CFG.csv_dir path.')
            return {}

        print('Computing derived columns...')
        derived = {p: add_calendar_features(compute_derived(rows))
                   for p, rows in data.items() if rows}

        print('Loading volume history...')
        volume_history = load_all_volume_history()

        print('Computing cross-participant signals...')
        signals = compute_cross_participant(derived, volume_history)

        # Only persist rows strictly newer than what is already stored.
        # (The warm-up rows are recomputed only as context and discarded.)
        if last_date is not None:
            new_derived = {
                p: [r for r in rows if r['date'] > last_date]
                for p, rows in derived.items()
            }
            new_signals = [s for s in signals if s.date > last_date]
        else:
            new_derived = derived
            new_signals = signals

        new_rows = sum(len(v) for v in new_derived.values())
        if new_rows == 0 and not new_signals:
            print('✅ Already up to date — no new data to save.')
            return {'derived': derived, 'signals': signals}

        print(f'Saving {new_rows} new rows + {len(new_signals)} new signal '
              f'days to SQLite (INSERT OR REPLACE)...')
        try:
            with SwingDB() as db:
                db.save(new_derived, new_signals)
        except (sqlite3.Error, OSError) as exc:
            print(f'❌ Failed to write to database {CFG.db_path}: {exc}')
            return {}
        print(f'✅ Done. Database: {CFG.db_path}')
        return {'derived': derived, 'signals': signals}

    # ── Today's swing report ──
    def report(self) -> str:
        db = SwingDB()
        sigs = db.latest_signals(1)
        if not sigs:
            db.close()
            return '❌ No data. Run: python fno_swing_engine.py update'

        latest = sigs[0]
        fii_row = db.latest_participant('FII', 1)
        fii = fii_row[0] if fii_row else {}
        pro_rows = db.latest_participant('PRO', 1)
        pro = pro_rows[0] if pro_rows else {}
        if pro.get('date') != latest['date']:
            pro = {}
        pro_total_oi = (pro.get('nopo') or 0) + (pro.get('npf') or 0)

        # Reconstruct DaySignals from DB row
        day = DaySignals(
            date=datetime.date.fromisoformat(latest['date']),
            fii_npf=latest['fii_npf'], pro_nopo=latest['pro_nopo'],
            dii_combined=latest['dii_combined'],
            client_nfno=latest['client_nfno'],
            fii_short_pct=latest['fii_short_pct'],
            smart_money_score=latest['smart_money_score'],
            retail_trap=bool(latest['retail_trap']),
            fii_pro_aligned=bool(latest['fii_pro_aligned']),
            alignment_direction=latest['alignment_direction'],
            fii_client_diverged=bool(latest['fii_client_diverged']),
            institutional_strength=latest['institutional_strength'],
            ai_sentiment=latest['ai_sentiment'],
            ai_confidence=latest['ai_confidence'],
        )

        signal = generate_swing_signal(day, fii)

        vol_data, vol_date = load_latest_volume_csv()
        vol_net = calculate_net_volume(vol_data) if vol_data else {}
        fii_net_vol = vol_net.get('FII', 0)
        dii_net_vol = vol_net.get('DII', 0)
        pro_net_vol = vol_net.get('PRO', 0)
        cli_net_vol = vol_net.get('CLIENT', 0)

        action = "N/A"
        tomorrow_bias = "⚠️ VOLUME DATA MISSING"
        tomorrow_note = "Download today's volume CSV to predict tomorrow."
        if vol_data:
            action, tomorrow_bias, tomorrow_note = get_tomorrow_matrix(
                day.fii_npf, fii_net_vol, pro_total_oi, pro_net_vol)

        lines = [
            '╔═══════════════════════════════════════════════════════╗',
            '║        FNO SWING TRADING REPORT                       ║',
            '╚═══════════════════════════════════════════════════════╝',
            f'  Date       : {day.date.strftime("%d %b %Y (%A)")}',
            f'  Sentiment  : {day.ai_sentiment}',
            f'  Confidence : {day.ai_confidence}%',
            '',
            '  ── PARTICIPANT POSITIONS ─────────────────────────',
            f'  FII    NPF : {day.fii_npf:>+12,.0f}   (short {day.fii_short_pct:.1f}%)',
            f'  PRO Net OI  : {pro_total_oi:>+12,.0f}',
            f'  DII  Net   : {day.dii_combined:>+12,.0f}',
            f'  CLIENT NFNO: {day.client_nfno:>+12,.0f}',
            '',
            '  ── TOMORROW\'S DIRECTION (OI + VOLUME) ────────────',
            f'  Volume Date: {vol_date or "Missing"}',
            '  Participant  │   Net OI (Pos)   │  Net Vol (Aggr)  │ Role',
            '  ─────────────┼──────────────────┼──────────────────┼───────────────',
            f'  FII          │ {day.fii_npf:>+15,.0f} │ {fii_net_vol:>+15,.0f} │ Trend Driver',
            f'  PRO          │ {pro_total_oi:>+15,.0f} │ {pro_net_vol:>+15,.0f} │ Smart Snipers',
            f'  DII          │ {day.dii_combined:>+15,.0f} │ {dii_net_vol:>+15,.0f} │ Hedgers',
            f'  CLIENT       │ {day.client_nfno:>+15,.0f} │ {cli_net_vol:>+15,.0f} │ Contrarian',
            '  ─────────────┴──────────────────┴──────────────────┴───────────────',
            f'  Matrix Logic : FII OI ({day.fii_npf:+,.0f}) / Vol ({fii_net_vol:+,.0f}) '
            f'vs PRO OI ({pro_total_oi:+,.0f}) / Vol ({pro_net_vol:+,.0f})',
            f'  Action       : {action}',
            f'  Bias         : {tomorrow_bias}',
            f'  Advice       : {tomorrow_note}',
            '',
            '  ── KEY FLAGS ─────────────────────────────────────',
            f'  Smart Money Score    : {day.smart_money_score:+d}',
            f'  FII/PRO Aligned      : {"YES → " + day.alignment_direction if day.fii_pro_aligned else "NO"}',
            f'  Retail Trap          : {"🚨 YES" if day.retail_trap else "No"}',
            f'  FII/Client Diverged  : {"Yes" if day.fii_client_diverged else "No"}',
            f'  Institutional Scale  : {day.institutional_strength:,.0f}',
            '',
        ]

        if fii:
            lines += [
                '  ── MOMENTUM ──────────────────────────────────────',
                f'  NFNO 5-day momentum  : {fii.get("nfno_5d_mom") or 0:>+12,.0f}',
                f'  FII 3-day long flow  : {fii.get("fil_3d") or 0:>+12,.0f}',
                f'  FII 3-day short flow : {fii.get("fis_3d") or 0:>+12,.0f}',
                f'  Days to expiry       : {fii.get("days_to_expiry", "?")}',
                '',
            ]

        lines += [
            '  ── SWING TRADE PLAN ──────────────────────────────',
            f'  Bias      : {signal.bias}  ({signal.strength})',
            f'  Entry     : {signal.entry_note}',
            f'  Stop      : {signal.stop_note}',
        ]

        if signal.warnings:
            lines.append('  Warnings  :')
            for w in signal.warnings:
                lines.append(f'    • {w}')

        lines.append('═' * 56)
        db.close()
        return '\n'.join(lines)

    # ── Last N days summary ──
    def history(self, n: int = 10) -> str:
        db = SwingDB()
        rows = db.latest_signals(n)
        db.close()
        if not rows:
            return '❌ No data. Run update first.'

        lines = [f'{"Date":<12} {"Score":>5} {"Sentiment":<38} {"Trap":<5} {"Aligned"}',
                 '─' * 85]
        for r in rows:
            d = datetime.date.fromisoformat(r['date']).strftime('%d-%b-%Y')
            lines.append(
                f'{d:<12} {r["smart_money_score"]:>+5d} '
                f'{r["ai_sentiment"]:<38.38} '
                f'{"YES" if r["retail_trap"] else "—":<5} '
                f'{r["alignment_direction"]}')
        return '\n'.join(lines)

    # ── Simple backtest: how often did score sign predict next-day move? ──
    def stats(self) -> str:
        """Check historical hit-rate of the smart money score."""
        db = SwingDB()
        rows = db.latest_signals(750)  # ~3 years
        db.close()
        if len(rows) < 50:
            return f'❌ Need ≥50 signal days (have {len(rows)}). Run update first.'

        rows.sort(key=lambda r: r['date'])
        # Group by month and count score distribution
        from collections import Counter
        buckets = Counter()
        for r in rows:
            s = r['smart_money_score']
            bucket = ('≤-4' if s <= -4 else '-3..-2' if s <= -2
                      else '-1..1' if s <= 1
                      else '2..3' if s <= 3 else '≥4')
            buckets[bucket] += 1

        lines = ['SMART MONEY SCORE DISTRIBUTION (history)', '─' * 40]
        for b in ['≤-4', '-3..-2', '-1..1', '2..3', '≥4']:
            cnt = buckets.get(b, 0)
            bar = '█' * int(cnt / max(buckets.values()) * 30) if buckets else ''
            lines.append(f'  {b:>6}: {cnt:>4}  {bar}')

        trap_days = sum(1 for r in rows if r['retail_trap'])
        aligned = sum(1 for r in rows if r['fii_pro_aligned'])
        lines += ['─' * 40,
                  f'  Retail trap days  : {trap_days}/{len(rows)} '
                  f'({trap_days/len(rows)*100:.0f}%)',
                  f'  FII/PRO aligned   : {aligned}/{len(rows)} '
                  f'({aligned/len(rows)*100:.0f}%)']
        return '\n'.join(lines)

    # ── Django context builder ──
    def get_dashboard_context(self, client: str = 'FII') -> dict:
        """Drop-in context for a Django template render()."""
        db = SwingDB()
        sigs = db.latest_signals(6)          # today + 5-day timeline
        part_rows = db.latest_participant(client, 30)
        all_pos = []
        for p in PARTICIPANTS:
            r = db.latest_participant(p, 1)
            if r:
                all_pos.append({'client': p, 'nfno':
                                (r[0]['npf'] or 0) + (r[0]['nopo'] or 0),
                                'date': r[0]['date']})
        db.close()

        all_pos.sort(key=lambda x: x['nfno'], reverse=True)
        latest = sigs[0] if sigs else None

        ctx = {
            'selected_client': client,
            'participant_position': all_pos,
            'regime_timeline': [
                {'date': r['date'], 'ai_sentiment': r['ai_sentiment'],
                 'ai_confidence': r['ai_confidence'],
                 'retail_trap': bool(r['retail_trap']),
                 'retail_trap_text': 'YES' if r['retail_trap'] else 'NO'}
                for r in reversed(sigs)
            ],
            'data': part_rows,
        }
        if latest:
            ctx.update({
                'ai_sentiment': latest['ai_sentiment'],
                'ai_confidence': latest['ai_confidence'],
                'smart_money_score': latest['smart_money_score'],
                'retail_trap': bool(latest['retail_trap']),
                'fii_short_pct': latest['fii_short_pct'],
            })
        return ctx


# ══════════════════════════════════════════════════════════════════════
# 8. CLI ENTRY POINT
# ══════════════════════════════════════════════════════════════════════

def main():
    engine = SwingEngine()
    cmd = sys.argv[1] if len(sys.argv) > 1 else 'help'

    commands = {
        'update':  engine.update,
        'report':  engine.report,
        'history': engine.history,
        'stats':   engine.stats,
    }

    if cmd in commands:
        result = commands[cmd]()
        if isinstance(result, str):
            print(result)
    else:
        print(__doc__)
        print('Commands: update | report | history | stats')


if __name__ == '__main__':
    main()
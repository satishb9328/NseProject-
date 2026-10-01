"""
═══════════════════════════════════════════════════════════════════════
  FNO SWING TRADING & DIRECTION ENGINE (OI + VOLUME)
═══════════════════════════════════════════════════════════════════════
  Merges Participant OI (Positions) + Participant Volume (Aggression)
  for all 4 participants (FII, DII, PRO, CLIENT) to predict tomorrow's 
  market direction and provide a swing trade plan.

  USAGE:
    python fno_swing_engine.py update    # Rebuild SQLite from OI CSVs
    python fno_swing_engine.py report    # Combined OI + Volume Report
    python fno_swing_engine.py history   # Last 10 days signal summary
    python fno_swing_engine.py stats     # Score distribution stats
═══════════════════════════════════════════════════════════════════════
"""

import os
import re
import csv
import glob
import sqlite3
import datetime
import sys
from datetime import timedelta
from dataclasses import dataclass, field
from typing import Optional
from collections import Counter

# ══════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ══════════════════════════════════════════════════════════════════════

@dataclass
class Config:
    """Central configuration — edit paths & thresholds here."""
    csv_dir:        str = r'D:\FNO DATA\Participant data\OpenInterest'
    vol_csv_dir:    str = r'D:\FNO DATA\Participant data\Volume'
    db_path:        str = r'D:\FNO DATA\fno_swing.db'
    years_history:  int = 3

    fii_heavy_short_pct:  float = 60.0
    fii_extreme_short:    float = 80.0
    fii_extreme_long:     float = 20.0
    extreme_oi_threshold: int   = 100_000

    weight_fii:    int = 3
    weight_pro:    int = 2
    weight_dii:    int = 1
    weight_client: int = 1

    momentum_window: int = 5
    flow_window:     int = 3

CFG = Config()

PARTICIPANTS = ['FII', 'DII', 'PRO', 'CLIENT']
RAW_COLUMNS = [
    'future_index_long', 'future_index_short',
    'future_stock_long', 'future_stock_short',
    'option_index_call_long', 'option_index_call_short',
    'option_index_put_long', 'option_index_put_short',
]

def get_file_mapping(directory: str) -> dict[datetime.date, str]:
    """Map dates parsed from CSV filenames to their exact file paths."""
    file_mapping = {}
    for path in glob.glob(os.path.join(directory, '*.csv')):
        match = re.search(r'(\d{8})', os.path.basename(path))
        if not match:
            continue

        date_string = match.group(1)
        try:
            file_date = datetime.datetime.strptime(date_string, '%Y%m%d').date()
        except ValueError:
            try:
                file_date = datetime.datetime.strptime(date_string, '%d%m%Y').date()
            except ValueError:
                continue
        file_mapping[file_date] = path
    return file_mapping

def last_weekly_expiry(dt: datetime.date) -> datetime.date:
    if dt.month == 12:
        last_day = datetime.date(dt.year, 12, 31)
    else:
        last_day = datetime.date(dt.year, dt.month + 1, 1) - timedelta(days=1)
    while last_day.weekday() != 3:
        last_day -= timedelta(days=1)
    return last_day

def safe_float(v) -> float:
    try:
        return float(str(v).replace(',', '').strip())
    except (ValueError, TypeError, AttributeError):
        return 0.0


# ══════════════════════════════════════════════════════════════════════
# 1. CSV LOADERS (OI + VOLUME)
# ══════════════════════════════════════════════════════════════════════

def load_participant_csvs(client_type: str, file_mapping: dict[datetime.date, str]) -> list[dict]:
    rows = []
    for file_date, path in sorted(file_mapping.items()):
        try:
            with open(path, 'r', encoding='utf-8', errors='ignore') as f:
                reader = csv.reader(f)
                header = None
                while True:
                    header = next(reader, None)
                    if header is None: break
                    if header and any(str(h).strip().lower().startswith('client') for h in header if h): break
                if not header: continue

                idx = {h.strip().lower(): i for i, h in enumerate(header) if h}
                for row in reader:
                    if not row or str(row[0]).strip().upper() != client_type.upper(): continue
                    def get(col, _r=row, _i=idx):
                        i = _i.get(col.lower())
                        if i is None or i >= len(_r): return 0.0
                        return safe_float(_r[i])
                    rows.append({'date': file_date, **{col: get(' '.join(w.capitalize() for w in col.split('_'))) for col in RAW_COLUMNS}})
                    break
        except Exception as exc:
            print(f'Error loading participant CSV {path}: {exc}', file=sys.stderr)
    rows.sort(key=lambda r: r['date'])
    return rows

def load_all_participants(start_date=None) -> dict[str, list[dict]]:
    file_mapping = get_file_mapping(CFG.csv_dir)
    if start_date:
        file_mapping = {
            file_date: path
            for file_date, path in file_mapping.items()
            if file_date >= start_date
        }
    return {p: load_participant_csvs(p, file_mapping) for p in PARTICIPANTS}

def load_latest_volume_csv() -> tuple[dict, Optional[str]]:
    """Loads the most recent participant volume CSV."""
    pattern = os.path.join(CFG.vol_csv_dir, '*participant_vol_*.csv')
    files = sorted(glob.glob(pattern))
    if not files:
        return {}, None
    
    latest_file = files[-1]
    date_match = re.search(r'participant_vol_(\d{4})(\d{2})(\d{2})\.csv', os.path.basename(latest_file))
    file_date_str = f"{date_match.group(3)}-{date_match.group(2)}-{date_match.group(1)}" if date_match else "Unknown"
    
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
                            record[h] = safe_float(row[i])
                    data[client] = record
    except Exception:
        pass
    return data, file_date_str


# ══════════════════════════════════════════════════════════════════════
# 2. DERIVED COLUMNS & MATH
# ══════════════════════════════════════════════════════════════════════

def compute_derived(rows: list[dict]) -> list[dict]:
    out = []
    prev = None
    history = []
    for row in rows:
        fil, fis = row['future_index_long'], row['future_index_short']
        fsl, fss = row['future_stock_long'], row['future_stock_short']
        oicl, oics = row['option_index_call_long'], row['option_index_call_short']
        oipl, oips = row['option_index_put_long'], row['option_index_put_short']

        fnl = fil - (prev['future_index_long'] if prev else 0)
        fns = fis - (prev['future_index_short'] if prev else 0)
        npf = fil - fis
        nci = oicl - oips
        npi = oipl - oics
        nopo = nci - npi
        nfno = npf + nopo
        sf_net = fsl - fss

        total_fut = fil + fis
        short_pct = (fis / total_fut * 100) if total_fut > 0 else 0.0
        long_pct = (fnl / fil * 100) if fil else 0.0
        short_dlt_pct = (fns / fis * 100) if fis else 0.0
        ls_ratio = round(fil / fis, 2) if fis else 0.0

        rec = {'date': row['date'], **{c: row[c] for c in RAW_COLUMNS},
               'fnl': fnl, 'fns': fns, 'npf': npf, 'nci': nci, 'npi': npi, 'nopo': nopo, 'nfno': nfno, 'sf_net': sf_net,
               'short_pct': short_pct, 'long_pct': long_pct, 'short_delta_pct': short_dlt_pct, 'ls_ratio': ls_ratio,
               'nfno_delta': None, 'nopo_delta': None, 'npf_delta': None, 'fil_3d': None, 'fis_3d': None,
               'nfno_5d_avg': None, 'nfno_5d_mom': None, 'short_pct_3d_trend': None}

        if prev is not None:
            rec['nfno_delta'] = nfno - prev['nfno']
            rec['nopo_delta'] = nopo - prev['nopo']
            rec['npf_delta']  = npf  - prev['npf']

        history.append(rec)
        n3, n5 = CFG.flow_window, CFG.momentum_window
        if len(history) >= n3 + 1:
            rec['fil_3d'] = fil - history[-(n3 + 1)]['future_index_long']
            rec['fis_3d'] = fis - history[-(n3 + 1)]['future_index_short']
            rec['short_pct_3d_trend'] = round(short_pct - history[-(n3 + 1)]['short_pct'], 2)
        if len(history) >= n5:
            window = history[-n5:]
            rec['nfno_5d_avg'] = sum(h['nfno'] for h in window) / n5
            rec['nfno_5d_mom'] = nfno - rec['nfno_5d_avg']

        out.append(rec)
        prev = rec
    return out

def add_calendar_features(rows: list[dict]) -> list[dict]:
    for rec in rows:
        d = rec['date']
        expiry = last_weekly_expiry(d)
        rec['day_of_week'] = d.weekday()
        rec['month'] = d.month
        rec['days_to_expiry'] = (expiry - d).days
        rec['is_expiry_week'] = 0 <= rec['days_to_expiry'] <= 5
        rec['week_of_month'] = min((d.day - 1) // 7 + 1, 5)
    return rows

def calculate_net_volume(data_dict: dict) -> dict:
    """Calculates Net Aggression (Buy - Sell) from Volume CSV."""
    results = {}
    for client, row in data_dict.items():
        net_fut = row.get('future index long', 0) - row.get('future index short', 0)
        net_opt = (row.get('option index call long', 0) + row.get('option index put short', 0)) - \
                  (row.get('option index put long', 0) + row.get('option index call short', 0))
        results[client] = net_fut + net_opt
    return results


# ══════════════════════════════════════════════════════════════════════
# 3. CROSS-PARTICIPANT SIGNALS & MATRIX
# ══════════════════════════════════════════════════════════════════════

@dataclass
class DaySignals:
    date: datetime.date
    fii_npf: float = 0
    pro_nopo: float = 0
    dii_combined: float = 0
    client_nfno: float = 0
    fii_short_pct: float = 0.0
    smart_money_score: int = 0
    retail_trap: bool = False
    fii_pro_aligned: bool = False
    alignment_direction: str = 'NEUTRAL'
    fii_client_diverged: bool = False
    institutional_strength: float = 0
    ai_sentiment: str = '⚖️ NEUTRAL'
    ai_confidence: int = 55

def compute_day_signals(fii, dii, pro, client) -> DaySignals:
    s = DaySignals(date=fii.get('date') or dii.get('date') or pro.get('date') or client.get('date'))
    fii_npf = fii.get('npf', 0) or 0
    fii_spc = fii.get('short_pct', 0.0) or 0.0
    pro_nopo = pro.get('nopo', 0) or 0
    pro_npf = pro.get('npf', 0) or 0
    client_nfno = client.get('nfno', 0) or 0
    dii_combined = (dii.get('npf', 0) or 0) + (dii.get('nopo', 0) or 0)

    s.fii_npf, s.fii_short_pct, s.pro_nopo, s.dii_combined, s.client_nfno = fii_npf, fii_spc, pro_nopo, dii_combined, client_nfno
    fii_dir = (fii_npf > 0) - (fii_npf < 0)
    pro_dir = (pro_nopo > 0) - (pro_nopo < 0)
    s.fii_pro_aligned = (fii_dir != 0 and fii_dir == pro_dir)
    if s.fii_pro_aligned: s.alignment_direction = 'BULLISH' if fii_dir > 0 else 'BEARISH'

    s.fii_client_diverged = (fii_npf < 0 and client_nfno > 0) or (fii_npf > 0 and client_nfno < 0)
    s.retail_trap = (fii_npf < 0 and fii_spc >= CFG.fii_heavy_short_pct) and ((pro_nopo + pro_npf) < 0) and (client_nfno > 0)

    score = CFG.weight_fii * ((fii_npf > 0) - (fii_npf < 0))
    if fii_spc >= CFG.fii_extreme_short: score -= 2
    elif fii_spc >= CFG.fii_heavy_short_pct: score -= 1
    elif fii_spc <= CFG.fii_extreme_long: score += 2
    elif fii_spc <= 40.0: score += 1

    pro_combined = pro_nopo + pro_npf
    score += CFG.weight_pro * ((pro_combined > 0) - (pro_combined < 0))
    score += CFG.weight_dii * ((dii_combined > 0) - (dii_combined < 0))
    score -= CFG.weight_client * ((client_nfno > 0) - (client_nfno < 0))
    s.smart_money_score = score
    s.institutional_strength = abs(fii_npf) + abs(pro_combined)

    confidence = min(95, 55 + abs(score) * 4)
    fii_pro_disagree = (fii_npf * pro_combined < 0) and (abs(fii_npf) > CFG.extreme_oi_threshold or abs(pro_combined) > CFG.extreme_oi_threshold)

    if s.retail_trap: s.ai_sentiment, s.ai_confidence = '⚠️ CRITICAL RETAIL TRAP (DEEP BEARISH)', 95
    elif fii_pro_disagree and abs(score) <= 3: s.ai_sentiment, s.ai_confidence = '⚖️ MIXED SIGNALS — CAUTION', max(confidence, 72)
    elif score <= -4: s.ai_sentiment, s.ai_confidence = '⚠️ DEEP BEARISH (Smart Money Selling)', max(confidence, 88)
    elif score >= 4 and fii_npf > 0: s.ai_sentiment, s.ai_confidence = '🚀 STRONG BULLISH', max(confidence, 84)
    elif score >= 4 and fii_npf < 0: s.ai_sentiment, s.ai_confidence = '🟡 BULLISH WITH CAUTION (FII Short)', max(confidence, 70)
    elif score <= -2: s.ai_sentiment, s.ai_confidence = '🔴 BEARISH', max(confidence, 75)
    elif score >= 2: s.ai_sentiment, s.ai_confidence = '🟢 BULLISH', max(confidence, 70)
    else: s.ai_sentiment, s.ai_confidence = '⚖️ NEUTRAL', max(confidence, 60)
    return s

def compute_cross_participant(data: dict) -> list[DaySignals]:
    by_date = {p: {r['date']: r for r in rows} for p, rows in data.items()}
    all_dates = sorted(set().union(*[set(m.keys()) for m in by_date.values()]))
    signals = []
    for d in all_dates:
        fii, dii, pro, client = by_date['FII'].get(d, {}), by_date['DII'].get(d, {}), by_date['PRO'].get(d, {}), by_date['CLIENT'].get(d, {})
        if not fii: continue
        signals.append(compute_day_signals(fii, dii, pro, client))
    return signals

def get_tomorrow_matrix(fii_oi_npf: float, fii_net_vol: float) -> tuple[str, str, str]:
    """The core logic merging OI stance with Volume aggression."""
    if fii_oi_npf < 0 and fii_net_vol > 0:
        return "SHORT COVERING", "🟢 STRONG BULLISH (Gap-Up/Squeeze)", "FII were Short but bought heavily today. Trapped! Expect panic buying tomorrow. Buy CE at open/dips."
    elif fii_oi_npf > 0 and fii_net_vol > 0:
        return "LONG BUILD-UP", "🟢 BULLISH (Continuation)", "Fresh buying alongside existing longs. Trend is strong. Buy CE on dips."
    elif fii_oi_npf > 0 and fii_net_vol < 0:
        return "LONG UNWINDING", "🔴 BEARISH (Topping/Reversal)", "FII booking profits on longs. Upside capped. Buy PE on rallies."
    elif fii_oi_npf < 0 and fii_net_vol < 0:
        return "ADDING SHORTS", "🔴 STRONG BEARISH (Breakdown)", "Fresh shorting alongside existing shorts. Do NOT buy CE. Buy PE on rallies."
    return "NEUTRAL / CHOPPY", "⚖️ SIDEWAYS", "No clear institutional aggression. Avoid directional option buying."


# ══════════════════════════════════════════════════════════════════════
# 4. SWING SIGNAL GENERATOR
# ══════════════════════════════════════════════════════════════════════

@dataclass
class SwingSignal:
    date: datetime.date
    bias: str
    strength: str
    entry_note: str
    stop_note: str
    warnings: list = field(default_factory=list)

def generate_swing_signal(day: DaySignals, fii_row: dict) -> SwingSignal:
    warnings = []
    score = day.smart_money_score
    momentum = fii_row.get('nfno_5d_mom')

    if day.retail_trap:
        return SwingSignal(day.date, 'NO-TRADE', '—', 'RETAIL TRAP ACTIVE. Longs forbidden.', 'If short: stop above yesterday high.', ['🚨 Retail trap: FII+PRO short, Client long'])
    if day.ai_sentiment.startswith('⚖️ MIXED'):
        return SwingSignal(day.date, 'NO-TRADE', '—', 'FII/PRO disagreement. Stay flat.', '—', ['⚡ Smart money in disagreement'])

    if day.fii_short_pct >= CFG.fii_extreme_short: warnings.append(f'🔥 FII extreme short ({day.fii_short_pct:.0f}%) — squeeze risk')
    elif day.fii_short_pct >= CFG.fii_heavy_short_pct: warnings.append(f'⚠️ FII heavy short ({day.fii_short_pct:.0f}%)')
    if fii_row.get('is_expiry_week'): warnings.append('📅 Expiry week — use wider stops (×1.5)')
    if momentum is not None:
        if momentum < 0 and day.fii_npf > 0: warnings.append('📉 NFNO momentum turning down despite net long')
        elif momentum > 0 and day.fii_npf < 0: warnings.append('📈 NFNO momentum turning up despite net short')

    if score >= 4: bias, strength = 'LONG', 'STRONG'
    elif score >= 2:
        bias, strength = 'LONG', 'MODERATE'
        if day.fii_short_pct >= CFG.fii_heavy_short_pct: strength = 'WEAK'
    elif score <= -4: bias, strength = 'SHORT', 'STRONG'
    elif score <= -2: bias, strength = 'SHORT', 'MODERATE'
    else: bias, strength = 'NO-TRADE', '—'

    if bias == 'LONG':
        entry_note = f'Buy dips. Score={score:+d}. {"Aligned PRO ✓" if day.fii_pro_aligned else "PRO unaligned - half size"}'
        stop_note = 'Stop below 2-day low.' if strength == 'STRONG' else 'Half pos. Stop below yesterday low.'
    elif bias == 'SHORT':
        entry_note = f'Sell rallies. Score={score:+d}. {"Aligned PRO ✓" if day.fii_pro_aligned else "PRO unaligned - half size"}'
        stop_note = 'Stop above 2-day high.' if strength == 'STRONG' else 'Half pos. Stop above yesterday high.'
    else:
        entry_note, stop_note = f'Neutral (score {score:+d}). Wait for ±2 break.', '—'
    return SwingSignal(day.date, bias, strength, entry_note, stop_note, warnings)


# ══════════════════════════════════════════════════════════════════════
# 5. SQLITE STORAGE
# ══════════════════════════════════════════════════════════════════════

DB_SCHEMA = """
CREATE TABLE IF NOT EXISTS participant_derived (
    date TEXT NOT NULL, client_type TEXT NOT NULL, {raw_cols},
    fnl REAL, fns REAL, npf REAL, nci REAL, npi REAL, nopo REAL, nfno REAL, sf_net REAL,
    short_pct REAL, long_pct REAL, short_delta_pct REAL, ls_ratio REAL,
    nfno_delta REAL, nopo_delta REAL, npf_delta REAL, fil_3d REAL, fis_3d REAL, nfno_5d_avg REAL, nfno_5d_mom REAL,
    short_pct_3d_trend REAL, day_of_week INT, month INT, days_to_expiry INT, is_expiry_week INT, week_of_month INT,
    PRIMARY KEY (date, client_type)
);
CREATE TABLE IF NOT EXISTS cross_signals (
    date TEXT PRIMARY KEY, fii_npf REAL, pro_nopo REAL, dii_combined REAL, client_nfno REAL, fii_short_pct REAL,
    smart_money_score INT, retail_trap INT, fii_pro_aligned INT, alignment_direction TEXT,
    fii_client_diverged INT, institutional_strength REAL, ai_sentiment TEXT, ai_confidence INT
);
""".format(raw_cols=',\n    '.join(f'{c} REAL' for c in RAW_COLUMNS))

class SwingDB:
    def __init__(self, db_path: str = None):
        self.path = db_path or CFG.db_path
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        self.conn = sqlite3.connect(self.path)
        self.conn.executescript(DB_SCHEMA)
        self.conn.commit()

    def save(self, derived, signals):
        cur = self.conn.cursor()
        cur.execute('DELETE FROM participant_derived')
        cur.execute('DELETE FROM cross_signals')
        pd_cols = ['date', 'client_type'] + RAW_COLUMNS + ['fnl', 'fns', 'npf', 'nci', 'npi', 'nopo', 'nfno', 'sf_net', 'short_pct', 'long_pct', 'short_delta_pct', 'ls_ratio', 'nfno_delta', 'nopo_delta', 'npf_delta', 'fil_3d', 'fis_3d', 'nfno_5d_avg', 'nfno_5d_mom', 'short_pct_3d_trend', 'day_of_week', 'month', 'days_to_expiry', 'is_expiry_week', 'week_of_month']
        pd_ph = ','.join('?' * len(pd_cols))
        for client, rows in derived.items():
            for r in rows:
                vals = [r['date'].isoformat() if c == 'date' else (client if c == 'client_type' else r.get(c)) for c in pd_cols]
                cur.execute(f'INSERT INTO participant_derived ({",".join(pd_cols)}) VALUES ({pd_ph})', vals)
        cs_cols = ['date', 'fii_npf', 'pro_nopo', 'dii_combined', 'client_nfno', 'fii_short_pct', 'smart_money_score', 'retail_trap', 'fii_pro_aligned', 'alignment_direction', 'fii_client_diverged', 'institutional_strength', 'ai_sentiment', 'ai_confidence']
        cs_ph = ','.join('?' * len(cs_cols))
        for s in signals:
            cur.execute(f'INSERT INTO cross_signals ({",".join(cs_cols)}) VALUES ({cs_ph})', (s.date.isoformat(), s.fii_npf, s.pro_nopo, s.dii_combined, s.client_nfno, s.fii_short_pct, s.smart_money_score, int(s.retail_trap), int(s.fii_pro_aligned), s.alignment_direction, int(s.fii_client_diverged), s.institutional_strength, s.ai_sentiment, s.ai_confidence))
        self.conn.commit()

    def latest_signals(self, n: int = 10) -> list[dict]:
        cur = self.conn.execute('SELECT * FROM cross_signals ORDER BY date DESC LIMIT ?', (n,))
        return [dict(zip([d[0] for d in cur.description], row)) for row in cur.fetchall()]

    def latest_participant(self, client: str, n: int = 1) -> list[dict]:
        cur = self.conn.execute('SELECT * FROM participant_derived WHERE client_type=? ORDER BY date DESC LIMIT ?', (client, n))
        return [dict(zip([d[0] for d in cur.description], row)) for row in cur.fetchall()]

    def close(self): self.conn.close()


# ══════════════════════════════════════════════════════════════════════
# 6. HIGH-LEVEL ENGINE API
# ══════════════════════════════════════════════════════════════════════

class SwingEngine:
    def __init__(self, config: Config = None):
        global CFG
        if config: CFG = config

    def update(self) -> dict:
        cutoff = datetime.date.today() - timedelta(days=365 * CFG.years_history)
        print(f'Loading OI CSVs from {CFG.csv_dir}...')
        data = load_all_participants(start_date=cutoff)
        if not any(data.values()):
            print('❌ No OI data found. Check CFG.csv_dir path.')
            return {}
        print(f"Loaded {len(data['FII'])} files for processing...")
        print('Computing derived columns...')
        derived = {p: add_calendar_features(compute_derived(rows)) for p, rows in data.items() if rows}
        print('Computing cross-participant signals...')
        signals = compute_cross_participant(derived)
        print(f'Saving to SQLite: {CFG.db_path}...')
        db = SwingDB()
        db.save(derived, signals)
        db.close()
        print('✅ OI Database Updated.')
        return {'derived': derived, 'signals': signals}

    def report(self) -> str:
        db = SwingDB()
        sigs = db.latest_signals(2)
        if not sigs:
            db.close()
            return '❌ No OI data. Run: python fno_swing_engine.py update'

        latest = sigs[0]
        fii_row = db.latest_participant('FII', 1)
        fii = fii_row[0] if fii_row else {}
        db.close()

        day = DaySignals(
            date=datetime.date.fromisoformat(latest['date']), fii_npf=latest['fii_npf'], pro_nopo=latest['pro_nopo'],
            dii_combined=latest['dii_combined'], client_nfno=latest['client_nfno'], fii_short_pct=latest['fii_short_pct'],
            smart_money_score=latest['smart_money_score'], retail_trap=bool(latest['retail_trap']),
            fii_pro_aligned=bool(latest['fii_pro_aligned']), alignment_direction=latest['alignment_direction'],
            fii_client_diverged=bool(latest['fii_client_diverged']), institutional_strength=latest['institutional_strength'],
            ai_sentiment=latest['ai_sentiment'], ai_confidence=latest['ai_confidence']
        )
        signal = generate_swing_signal(day, fii)

        # ── Fetch & Compute Volume Data ──
        vol_data, vol_date = load_latest_volume_csv()
        vol_net = calculate_net_volume(vol_data) if vol_data else {}
        
        fii_net_vol = vol_net.get('FII', 0)
        dii_net_vol = vol_net.get('DII', 0)
        pro_net_vol = vol_net.get('PRO', 0)
        cli_net_vol = vol_net.get('CLIENT', 0)
        
        action, tomorrow_bias, tomorrow_note = "N/A", "⚠️ VOLUME DATA MISSING", "Download today's volume CSV to predict tomorrow."
        if vol_data:
            action, tomorrow_bias, tomorrow_note = get_tomorrow_matrix(day.fii_npf, fii_net_vol)

        lines = [
            '╔═══════════════════════════════════════════════════════╗',
            '║        FNO COMBINED SWING & DIRECTION REPORT          ║',
            '╚═══════════════════════════════════════════════════════╝',
            f'  OI Data Date : {day.date.strftime("%d %b %Y (%A)")}',
            f'  Vol Data Date: {vol_date or "Missing"}',
            '',
            '  ── 📈 TOMORROW\'S DIRECTION (OI + VOLUME) ─────────',
            '  Participant  │   Net OI (Pos)   │  Net Vol (Aggr)  │ Role',
            '  ─────────────┼──────────────────┼──────────────────┼───────────────',
            f'  FII          │ {day.fii_npf:>+15,.0f} │ {fii_net_vol:>+15,.0f} │ 👑 Trend Driver',
            f'  PRO          │ {day.pro_nopo:>+15,.0f} │ {pro_net_vol:>+15,.0f} │ 🎯 Smart Snipers',
            f'  DII          │ {day.dii_combined:>+15,.0f} │ {dii_net_vol:>+15,.0f} │ 🛡️ Hedgers',
            f'  CLIENT       │ {day.client_nfno:>+15,.0f} │ {cli_net_vol:>+15,.0f} │ 🐟 Contrarian',
            '  ─────────────┴──────────────────┴──────────────────┴───────────────',
            f'  Matrix Logic : FII OI ({day.fii_npf:+,.0f}) vs FII Vol ({fii_net_vol:+,.0f})',
            f'  Action       : {action}',
            f'  Bias         : {tomorrow_bias}',
            f'  Advice       : {tomorrow_note}',
            '',
            '  ── 📊 SWING TRADING OI MATRIX ─────────────────────',
            f'  Sentiment    : {day.ai_sentiment}',
            f'  Confidence   : {day.ai_confidence}%',
            f'  Smart Score  : {day.smart_money_score:+d}',
            f'  Retail Trap  : {"🚨 YES" if day.retail_trap else "No"}',
            '',
            '  ── SWING TRADE PLAN ──────────────────────────────',
            f'  Bias      : {signal.bias}  ({signal.strength})',
            f'  Entry     : {signal.entry_note}',
            f'  Stop      : {signal.stop_note}',
        ]

        if signal.warnings:
            lines.append('  Warnings  :')
            for w in signal.warnings: lines.append(f'    • {w}')

        lines.append('═' * 56)
        return '\n'.join(lines)

    def history(self, n: int = 10) -> str:
        db = SwingDB()
        rows = db.latest_signals(n)
        db.close()
        if not rows: return '❌ No data. Run update first.'
        lines = [f'{"Date":<12} {"Score":>5} {"Sentiment":<38} {"Trap":<5} {"Aligned"}', '─' * 85]
        for r in rows:
            d = datetime.date.fromisoformat(r['date']).strftime('%d-%b-%Y')
            lines.append(f'{d:<12} {r["smart_money_score"]:>+5d} {r["ai_sentiment"]:<38.38} {"YES" if r["retail_trap"] else "—":<5} {r["alignment_direction"]}')
        return '\n'.join(lines)

    def stats(self) -> str:
        db = SwingDB()
        rows = db.latest_signals(750)
        db.close()
        if len(rows) < 50: return f'❌ Need ≥50 signal days (have {len(rows)}). Run update first.'
        buckets = Counter()
        for r in rows:
            s = r['smart_money_score']
            buckets['≤-4' if s <= -4 else '-3..-2' if s <= -2 else '-1..1' if s <= 1 else '2..3' if s <= 3 else '≥4'] += 1
        lines = ['SMART MONEY SCORE DISTRIBUTION (history)', '─' * 40]
        for b in ['≤-4', '-3..-2', '-1..1', '2..3', '≥4']:
            cnt = buckets.get(b, 0)
            bar = '█' * int(cnt / max(buckets.values()) * 30) if buckets else ''
            lines.append(f'  {b:>6}: {cnt:>4}  {bar}')
        trap_days = sum(1 for r in rows if r['retail_trap'])
        aligned = sum(1 for r in rows if r['fii_pro_aligned'])
        lines += ['─' * 40, f'  Retail trap days  : {trap_days}/{len(rows)} ({trap_days/len(rows)*100:.0f}%)', f'  FII/PRO aligned   : {aligned}/{len(rows)} ({aligned/len(rows)*100:.0f}%)']
        return '\n'.join(lines)


# ══════════════════════════════════════════════════════════════════════
# 7. CLI ENTRY POINT
# ══════════════════════════════════════════════════════════════════════

def main():
    import sys
    engine = SwingEngine()
    cmd = sys.argv[1] if len(sys.argv) > 1 else 'help'
    commands = {'update': engine.update, 'report': engine.report, 'history': engine.history, 'stats': engine.stats}
    if cmd in commands:
        result = commands[cmd]()
        if isinstance(result, str): print(result)
    else:
        print(__doc__)
        print('Commands: update | report | history | stats')

if __name__ == '__main__':
    main()
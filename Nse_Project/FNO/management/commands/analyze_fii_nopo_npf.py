from django.core.management.base import BaseCommand
import csv
import os
import json
import glob
from datetime import datetime

FNO_CSV_DIR = r'D:\FNO DATA\Participant data'
OUTPUT_PATH = os.path.join(os.path.dirname(__file__), '../../nopd_npf_analysis.json')

class Command(BaseCommand):
    help = 'Analyze FII NOPO/NPF trends and output analysis summary as JSON.'

    def handle(self, *args, **options):
        # Find latest FII participant CSVs
        pattern = os.path.join(FNO_CSV_DIR, 'participant_oi_*.csv')
        files = sorted([f for f in glob.glob(pattern)])
        rows = []
        for path in files:
            with open(path, 'r', encoding='utf-8', errors='ignore') as f:
                reader = csv.reader(f)
                header = None
                while True:
                    header = next(reader, None)
                    if header is None:
                        break
                    if not header:
                        continue
                    if any(str(h).strip().lower().startswith('client') for h in header if h is not None):
                        break
                if not header:
                    continue
                idx_map = {h.lower(): i for i, h in enumerate(header)}
                for row in reader:
                    if not row or len(row) < 1:
                        continue
                    client = str(row[0]).strip().upper()
                    if client != 'FII':
                        continue
                    def _get(col):
                        i = idx_map.get(col.lower())
                        if i is None or i >= len(row):
                            return 0.0
                        try:
                            return float(str(row[i]).replace(',', '').strip())
                        except Exception:
                            return 0.0
                    rows.append({
                        'date': path[-12:-4],
                        'future_index_long': _get('Future Index Long'),
                        'future_index_short': _get('Future Index Short'),
                        'option_index_call_long': _get('Option Index Call Long'),
                        'option_index_put_long': _get('Option Index Put Long'),
                        'option_index_call_short': _get('Option Index Call Short'),
                        'option_index_put_short': _get('Option Index Put Short'),
                    })
                    break
        rows.sort(key=lambda x: datetime.strptime(x['date'], '%d%m%Y'))
        result = []
        prev_nopo = None
        prev_npf = None
        for r in rows:
            NCI = r['option_index_call_long'] - r['option_index_put_short']
            NPI = r['option_index_put_long'] - r['option_index_call_short']
            NOPO = NCI - NPI
            NPF = r['future_index_long'] - r['future_index_short']
            nopo_delta = NOPO - prev_nopo if prev_nopo is not None else None
            npf_delta = NPF - prev_npf if prev_npf is not None else None
            result.append({
                'date': r['date'],
                'NOPO': NOPO,
                'NPF': NPF,
                'NOPO_delta': nopo_delta,
                'NPF_delta': npf_delta
            })
            prev_nopo = NOPO
            prev_npf = NPF
        # Analyze trend (last 7 days)
        analysis = []
        for r in result[-7:]:
            trend = ''
            if r['NOPO_delta'] is not None:
                if r['NOPO_delta'] > 0:
                    trend += 'NOPO rising (bullish). '
                elif r['NOPO_delta'] < 0:
                    trend += 'NOPO falling (bearish). '
            if r['NPF_delta'] is not None:
                if r['NPF_delta'] > 0:
                    trend += 'NPF rising (bullish). '
                elif r['NPF_delta'] < 0:
                    trend += 'NPF falling (bearish). '
            analysis.append({'date': r['date'], 'summary': trend.strip()})
        with open(OUTPUT_PATH, 'w', encoding='utf-8') as f:
            json.dump({'data': result, 'analysis': analysis}, f, indent=2)
        # Also write nopd_npf_data.json for charting
        chart_path = os.path.join(os.path.dirname(__file__), '../../nopd_npf_data.json')
        with open(chart_path, 'w', encoding='utf-8') as f:
            json.dump(result, f, indent=2)
        self.stdout.write(self.style.SUCCESS(f'Analysis written to {OUTPUT_PATH} and chart data to {chart_path}'))

import os
import pandas as pd
from datetime import datetime, timedelta

def get_participant_oi_files(data_dir, days=None):
    files = [f for f in os.listdir(data_dir) if f.startswith('participant_oi_') and f.endswith('.csv')]
    files.sort(reverse=True)  # Latest first
    if days:
        files = files[:days]
    return [os.path.join(data_dir, f) for f in files]

def read_and_concat(files):
    dfs = []
    for f in files:
        # Skip first row (comment), use second row as header
        df = pd.read_csv(f, skiprows=1)
        df.columns = [c.strip() for c in df.columns]
        df['Date'] = extract_date_from_filename(f)
        dfs.append(df)
    return pd.concat(dfs, ignore_index=True)

def extract_date_from_filename(filename):
    # expects ...participant_oi_DDMMYYYY.csv
    base = os.path.basename(filename)
    date_str = base.replace('participant_oi_', '').replace('.csv', '')
    return datetime.strptime(date_str, '%d%m%Y').date()

def analyze_positions(df):
    # Clean up column names for consistency
    df = df.rename(columns={
        'Client Type': 'Participant',
        'Future Index Long': 'Future Index Long',
        'Future Index Short': 'Future Index Short',
        'Future Stock Long': 'Future Stock Long',
        'Future Stock Short': 'Future Stock Short',
        'Option Index Call Long': 'Option Index Call Long',
        'Option Index Put Long': 'Option Index Put Long',
        'Option Index Call Short': 'Option Index Call Short',
        'Option Index Put Short': 'Option Index Put Short',
        'Option Stock Call Long': 'Option Stock Call Long',
        'Option Stock Put Long': 'Option Stock Put Long',
        'Option Stock Call Short': 'Option Stock Call Short',
        'Option Stock Put Short': 'Option Stock Put Short',
        'Total Long Contracts': 'Total Long Contracts',
        'Total Short Contracts': 'Total Short Contracts',
    })
    # Group by Date and Participant, sum all numeric columns
    summary = df.groupby(['Date', 'Participant']).sum(numeric_only=True).reset_index()
    return summary

def save_summary(summary, out_path):
    summary.to_csv(out_path, index=False)


def get_actionable_summary(latest_df):
    # Calculate net position (Total Long - Total Short) for each participant
    latest_df = latest_df[latest_df['Participant'] != 'TOTAL']
    latest_df = latest_df.copy()
    latest_df['Net Position'] = latest_df['Total Long Contracts'] - latest_df['Total Short Contracts']
    # Find dominant participant
    dominant = latest_df.loc[latest_df['Net Position'].abs().idxmax()]
    dom_name = dominant['Participant']
    dom_net = dominant['Net Position']
    dom_type = 'Net Long' if dom_net > 0 else 'Net Short'
    # Simple trend logic
    if dom_net > 0:
        trend = 'Bullish bias'
    elif dom_net < 0:
        trend = 'Bearish bias'
    else:
        trend = 'Neutral'
    # Compose summary
    summary = {
        'dominant_participant': dom_name,
        'net_position': int(dom_net),
        'position_type': dom_type,
        'trend_prediction': trend,
        'details': [
            {
                'participant': row['Participant'],
                'net_position': int(row['Net Position']),
                'type': 'Net Long' if row['Net Position'] > 0 else 'Net Short' if row['Net Position'] < 0 else 'Neutral'
            } for _, row in latest_df.iterrows()
        ]
    }
    return summary

def main():
    data_dir = r'D:\FNO DATA\Participant data'
    out_dir = os.path.join(os.path.dirname(__file__), 'FNO', 'FNO')
    os.makedirs(out_dir, exist_ok=True)


    # 1. One month (last 30 days)
    month_files = get_participant_oi_files(data_dir, days=30)
    month_df = read_and_concat(month_files)
    month_summary = analyze_positions(month_df)
    save_summary(month_summary, os.path.join(out_dir, 'participant_oi_month_summary.csv'))

    # 1b. 30-day actionable summary (old logic, kept for compatibility)
    import json
    if not month_summary.empty:
        # Aggregate by participant over 30 days
        month_agg = month_summary[month_summary['Participant'] != 'TOTAL'].groupby('Participant').agg({
            'Total Long Contracts': 'sum',
            'Total Short Contracts': 'sum'
        }).reset_index()
        month_agg['Net Position'] = month_agg['Total Long Contracts'] - month_agg['Total Short Contracts']
        dominant = month_agg.loc[month_agg['Net Position'].abs().idxmax()]
        dom_name = dominant['Participant']
        dom_net = dominant['Net Position']
        dom_type = 'Net Long' if dom_net > 0 else 'Net Short'
        trend = 'Bullish bias' if dom_net > 0 else 'Bearish bias' if dom_net < 0 else 'Neutral'
        summary_30d = {
            'dominant_participant': dom_name,
            'net_position': int(dom_net),
            'position_type': dom_type,
            'trend_prediction': trend,
            'details': [
                {
                    'participant': row['Participant'],
                    'net_position': int(row['Net Position']),
                    'type': 'Net Long' if row['Net Position'] > 0 else 'Net Short' if row['Net Position'] < 0 else 'Neutral'
                } for _, row in month_agg.iterrows()
            ]
        }
        with open(os.path.join(out_dir, 'participant_oi_30day_actionable.json'), 'w') as f:
            json.dump(summary_30d, f, indent=2)
        print('Saved 30-day actionable summary')

        # --- Advanced segment-wise analysis ---
        # Define segment columns
        segment_map = {
            'Index Futures': ['Future Index Long', 'Future Index Short'],
            'Stock Futures': ['Future Stock Long', 'Future Stock Short'],
            'Index Options': [
                'Option Index Call Long', 'Option Index Put Long',
                'Option Index Call Short', 'Option Index Put Short'],
            'Stock Options': [
                'Option Stock Call Long', 'Option Stock Put Long',
                'Option Stock Call Short', 'Option Stock Put Short']
        }
        participants = ['Client', 'DII', 'FII', 'Pro']
        seg_results = {p: {} for p in participants}
        for seg, cols in segment_map.items():
            if 'Futures' in seg:
                long_col, short_col = cols
                seg_agg = month_summary[month_summary['Participant'].isin(participants)].groupby('Participant').agg({
                    long_col: 'sum', short_col: 'sum'
                }).reset_index()
                seg_agg['Net Position'] = seg_agg[long_col] - seg_agg[short_col]
                for _, row in seg_agg.iterrows():
                    seg_results[row['Participant']][seg] = int(row['Net Position'])
            else:
                # Options: Net = (Call Long + Put Long) - (Call Short + Put Short)
                cl, pl, cs, ps = cols
                seg_agg = month_summary[month_summary['Participant'].isin(participants)].groupby('Participant').agg({
                    cl: 'sum', pl: 'sum', cs: 'sum', ps: 'sum'
                }).reset_index()
                seg_agg['Net Position'] = (seg_agg[cl] + seg_agg[pl]) - (seg_agg[cs] + seg_agg[ps])
                for _, row in seg_agg.iterrows():
                    seg_results[row['Participant']][seg] = int(row['Net Position'])

        # Composite logic: assign +1 for net long, -1 for net short, 0 for neutral in each segment, sum for each participant
        composite_scores = {}
        for p in participants:
            score = 0
            for seg in segment_map:
                val = seg_results[p][seg]
                if val > 0:
                    score += 1
                elif val < 0:
                    score -= 1
            composite_scores[p] = score

        # Find dominant participant by composite score, then by total net position if tie
        max_score = max(composite_scores.values())
        dom_candidates = [p for p, s in composite_scores.items() if s == max_score]
        if len(dom_candidates) == 1:
            dom_participant = dom_candidates[0]
        else:
            # Tie-breaker: sum of absolute net positions across segments
            dom_participant = max(dom_candidates, key=lambda p: sum(abs(seg_results[p][seg]) for seg in segment_map))

        # Composite market direction: sum all segment net positions for all participants
        market_score = sum(
            (1 if seg_results[p][seg] > 0 else -1 if seg_results[p][seg] < 0 else 0)
            for p in participants for seg in segment_map
        )
        if market_score > 0:
            composite_trend = 'Bullish bias'
        elif market_score < 0:
            composite_trend = 'Bearish bias'
        else:
            composite_trend = 'Neutral'

        # Prepare output
        segmentwise_details = []
        for p in participants:
            entry = {'participant': p, 'composite_score': composite_scores[p]}
            entry.update({seg: seg_results[p][seg] for seg in segment_map})
            segmentwise_details.append(entry)

        segmentwise_summary = {
            'dominant_participant': dom_participant,
            'composite_trend': composite_trend,
            'segmentwise_details': segmentwise_details,
            'segment_names': list(segment_map.keys()),
            'participants': participants
        }
        with open(os.path.join(out_dir, 'participant_oi_30day_segmentwise_actionable.json'), 'w') as f:
            json.dump(segmentwise_summary, f, indent=2)
        print('Saved 30-day segmentwise actionable summary')

    # 2. All data
    all_files = get_participant_oi_files(data_dir)
    all_df = read_and_concat(all_files)
    all_summary = analyze_positions(all_df)
    save_summary(all_summary, os.path.join(out_dir, 'participant_oi_total_summary.csv'))

    # 3. Actionable summary for latest date
    import json
    if not month_summary.empty:
        latest_date = month_summary['Date'].max()
        latest_df = month_summary[month_summary['Date'] == latest_date]
        actionable = get_actionable_summary(latest_df)
        with open(os.path.join(out_dir, 'participant_oi_actionable.json'), 'w') as f:
            json.dump(actionable, f, indent=2)
        print('Saved actionable summary for', latest_date)

    print('Saved:')
    print(' - participant_oi_month_summary.csv (last 30 days)')
    print(' - participant_oi_total_summary.csv (all data)')
    print(' - participant_oi_actionable.json (latest actionable summary)')

if __name__ == '__main__':
    main()

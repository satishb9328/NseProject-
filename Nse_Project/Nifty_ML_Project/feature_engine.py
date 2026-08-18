# feature_engine.py
import pandas as pd
import numpy as np
import config

def apply_tick_rule(df):
    """Classifies volume as Aggressive Buy or Sell based on price movement."""
    print("🧠 Applying Tick Rule to calculate Order Flow...")
    df['price_diff'] = df['ltp'].diff()
    df['trade_direction'] = np.sign(df['price_diff'])
    df['buy_volume'] = np.where(df['trade_direction'] > 0, df['volume'], 0)
    df['sell_volume'] = np.where(df['trade_direction'] < 0, df['volume'], 0)
    return df

def build_candles(df, hist_oi_df):
    """Aggregates raw ticks into 2-minute OHLC + Order Flow candles."""
    print(f"📈 Building {config.TIMEFRAME} candles with ML features...")
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df.set_index('timestamp', inplace=True)
    
    agg_dict = {
        'ltp': ['first', 'max', 'min', 'last'],
        'total_buy_qty': 'last',
        'total_sell_qty': 'last',
        'buy_volume': 'sum',
        'sell_volume': 'sum'
    }
    
    candles = df.resample(config.TIMEFRAME).agg(agg_dict)
    candles.columns = ['open', 'high', 'low', 'close', 'total_buy_qty', 'total_sell_qty', 'candle_buy_vol', 'candle_sell_vol']
    candles.dropna(subset=['open'], inplace=True)
    
    # Merge Historical OI
    if not hist_oi_df.empty:
        oi_2min = hist_oi_df.resample(config.TIMEFRAME).last()
        candles = candles.join(oi_2min, how='left')
        candles['oi'] = candles['oi'].ffill().fillna(0) 
        candles['oi_change'] = candles['oi'].diff().fillna(0)
    else:
        candles['oi'] = 0
        candles['oi_change'] = 0
    
    # Order Flow Metrics
    candles['volume_delta'] = candles['candle_buy_vol'] - candles['candle_sell_vol']
    candles['ofi'] = candles['total_buy_qty'] - candles['total_sell_qty']
    candles['ofi_imbalance'] = candles['ofi'] / (candles['total_buy_qty'] + candles['total_sell_qty'])
    
    # VWAP
    typical_price = (candles['high'] + candles['low'] + candles['close']) / 3
    daily_group = candles.groupby(candles.index.date)
    cum_vol = daily_group['candle_buy_vol'].cumsum() + daily_group['candle_sell_vol'].cumsum()
    cum_typ_price_vol = (typical_price * (candles['candle_buy_vol'] + candles['candle_sell_vol'])).groupby(candles.index.date).cumsum()
    candles['vwap'] = cum_typ_price_vol / cum_vol
    candles['vwap_distance'] = (candles['close'] - candles['vwap']) / candles['vwap']
    
    # Lag Features
    candles['prev_delta'] = candles['volume_delta'].shift(1)
    candles['prev_oi_change'] = candles['oi_change'].shift(1)
    candles['prev_ofi_imbalance'] = candles['ofi_imbalance'].shift(1)
    
    # Labeling (0.05% threshold)
    candles['future_return'] = candles['close'].shift(-2) / candles['close'] - 1
    candles['target'] = np.where(candles['future_return'] > 0.0005, 1, 0) 
    
    candles.dropna(inplace=True)
    
    ml_features = candles[['candle_buy_vol', 'candle_sell_vol', 'oi_change', 'volume_delta', 'ofi', 'ofi_imbalance', 
                           'vwap_distance', 'prev_delta', 'prev_oi_change', 'prev_ofi_imbalance', 'target']].copy()
    
    return ml_features
# live_signal.py
import json
import threading
import time
from typing import Any
import pandas as pd
import numpy as np
import xgboost as xgb
from datetime import datetime, timezone, timedelta
from fyers_apiv3.FyersWebsocket import data_ws
import config
import db_manager

# Load the Trained ML Model
try:
    MODEL = xgb.XGBClassifier()
    MODEL.load_model("nifty_tbt_model.json")
    print("🧠 ML Model loaded successfully!")
except Exception as e:
    print(f"❌ Error loading model: {e}")
    exit()

# Live Data State
live_buffer = []
current_candle = {}
last_candle = {}
buffer_lock = threading.Lock()
fyers_socket: Any | None = None

def onmessage(message):
    """Receives live ticks and builds 2-minute candles."""
    global live_buffer, current_candle
    
    payload = message
    if isinstance(message, str):
        try: payload = json.loads(message)
        except: return
        
    ticks = []
    if isinstance(payload, dict):
        if isinstance(payload.get("d"), list):
            ticks = payload.get("d", [])
        elif "ltp" in payload or "lp" in payload:
            ticks = [payload]  # Wrap flat dict in a list
    elif isinstance(payload, list):
        ticks = payload
        
    for raw in ticks:
        symbol = raw.get("symbol") or raw.get("n") or config.FUT_KEY
        if symbol != config.FUT_KEY:
            continue
            
        ltp = raw.get("ltp") or raw.get("lp")
        cum_vol = raw.get("vol_traded_today") or raw.get("vtt") or raw.get("vol") or 0
        tbq = raw.get("tot_buy_qty") or raw.get("tbq") or 0
        tsq = raw.get("tot_sell_qty") or raw.get("tsq") or 0
        
        if not ltp: continue
        
        # Calculate 2-minute candle bucket
        ts = datetime.now(timezone(timedelta(hours=5, minutes=30)))
        bucket_minute = (ts.minute // 2) * 2
        bucket_time = ts.replace(minute=bucket_minute, second=0, microsecond=0)
        
        # Tick Rule for Volume Delta
        prev_ltp = current_candle.get("last_price", ltp)
        price_diff = ltp - prev_ltp
        direction = 1 if price_diff > 0 else (-1 if price_diff < 0 else 0)
        
        # Calculate actual volume traded in this specific tick
        prev_cum_vol = current_candle.get("cum_vol", cum_vol)
        if prev_cum_vol is None: prev_cum_vol = 0
        if cum_vol is None: cum_vol = 0
        
        tick_vol = max(0, cum_vol - prev_cum_vol)
        buy_vol = tick_vol if direction > 0 else 0
        sell_vol = tick_vol if direction < 0 else 0
        
        # If new 2-min bucket started, save old candle and start new one
        if current_candle and current_candle.get("timestamp") != bucket_time:
            with buffer_lock:
                live_buffer.append(current_candle.copy())
                if len(live_buffer) > 5: live_buffer.pop(0) # Keep last 5 candles
            current_candle = {}
            
        # Update current candle
        if not current_candle:
            current_candle = {
                "timestamp": bucket_time,
                "open": ltp, "high": ltp, "low": ltp, "close": ltp,
                "cum_vol": cum_vol, "buy_vol": buy_vol, "sell_vol": sell_vol,
                "tbq": tbq, "tsq": tsq, "last_price": ltp
            }
        else:
            current_candle["high"] = max(current_candle["high"], ltp)
            current_candle["low"] = min(current_candle["low"], ltp)
            current_candle["close"] = ltp
            current_candle["cum_vol"] = cum_vol
            current_candle["buy_vol"] += buy_vol
            current_candle["sell_vol"] += sell_vol
            current_candle["tbq"] = tbq
            current_candle["tsq"] = tsq
            current_candle["last_price"] = ltp

def onerror(message):
    print(f"❌ Socket Error: {message}")

def onclose(message):
    print("🔌 Socket Closed.")

def onopen():
    if fyers_socket is None:
        print("❌ Fyers socket not initialized.")
        return
    fyers_socket.subscribe(symbols=[config.FUT_KEY], data_type="SymbolUpdate")
    fyers_socket.keep_running()
    print(f"⚡ Live Signal Engine connected to {config.FUT_KEY}! Waiting for 2-min candle close...")

def generate_signal():
    """Checks if a candle just closed, and predicts the next move."""
    global live_buffer, last_candle
    
    while True:
        if len(live_buffer) > 0:
            closed_candle = live_buffer[-1]
            
            # If this candle is newer than the last one we predicted, run ML
            if closed_candle["timestamp"] != last_candle.get("timestamp"):
                last_candle = closed_candle
                
                # 1. Calculate Features
                delta = closed_candle["buy_vol"] - closed_candle["sell_vol"]
                ofi = closed_candle["tbq"] - closed_candle["tsq"]
                ofi_imb = ofi / (closed_candle["tbq"] + closed_candle["tsq"]) if (closed_candle["tbq"] + closed_candle["tsq"]) > 0 else 0
                vwap = (closed_candle["high"] + closed_candle["low"] + closed_candle["close"]) / 3
                vwap_dist = (closed_candle["close"] - vwap) / vwap if vwap > 0 else 0
                
                # Get lag features from the candle before it
                prev_candle = live_buffer[-2] if len(live_buffer) > 1 else closed_candle
                prev_delta = prev_candle["buy_vol"] - prev_candle["sell_vol"]
                prev_ofi_imb = (prev_candle["tbq"] - prev_candle["tsq"]) / (prev_candle["tbq"] + prev_candle["tsq"]) if (prev_candle["tbq"] + prev_candle["tsq"]) > 0 else 0
                
                # 2. Prepare Data for Model
                features = pd.DataFrame([[
                    closed_candle["buy_vol"], closed_candle["sell_vol"], 0, # oi_change (unknown in real-time)
                    delta, ofi, ofi_imb, vwap_dist, 
                    prev_delta, 0, # prev_oi_change
                    prev_ofi_imb
                ]], columns=['candle_buy_vol', 'candle_sell_vol', 'oi_change', 'volume_delta', 
                             'ofi', 'ofi_imbalance', 'vwap_distance', 'prev_delta', 
                             'prev_oi_change', 'prev_ofi_imbalance'])
                
                # 3. Predict
                prob_up = MODEL.predict_proba(features)[0][1]
                
                # SCALPING THRESHOLDS (52% / 48%)
                signal = "🟢 BUY CE (Scalp)" if prob_up > 0.52 else ("🔴 SHORT PE (Scalp)" if prob_up < 0.48 else "⚪ NEUTRAL - NO TRADE")
                
                print("\n" + "="*50)
                print(f"⏰ Candle Closed: {closed_candle['timestamp'].strftime('%H:%M')}")
                print(f"Close Price: {closed_candle['close']}")
                print(f"Volume Delta: {delta:.0f} | OFI Imbalance: {ofi_imb:.2f}")
                print(f"ML Probability UP: {prob_up*100:.1f}%")
                print(f"SIGNAL: {signal}")
                print("="*50 + "\n")
                
        # Sleep outside the if statement so the while loop doesn't max out CPU
        time.sleep(1) 

def start_socket():
    token = db_manager.get_fyers_token()
    if not token:
        print("❌ No Fyers token found in Supabase. Login on Render first.")
        return
        
    access_token = f"{config.FYERS_APP_ID}:{token}"
    
    global fyers_socket
    fyers_socket = data_ws.FyersDataSocket(
        access_token=access_token, log_path="", litemode=False, write_to_file=False,
        reconnect=True, on_connect=onopen, on_close=onclose, on_error=onerror, on_message=onmessage
    )
    fyers_socket.connect()

if __name__ == "__main__":
    print("🚀 STARTING LIVE SIGNAL ENGINE...")
    
    # Start ML Signal Checker in background
    threading.Thread(target=generate_signal, daemon=True).start()
    
    try:
        # Start WebSocket (This blocks)
        start_socket()
    except KeyboardInterrupt:
        print("\n🛑 Ctrl+C detected. Shutting down Live Signal Engine...")
        try:
            socket_obj = globals().get("fyers_socket")
            if socket_obj is not None and hasattr(socket_obj, "close_connection"):
                socket_obj.close_connection()
            elif socket_obj is not None and hasattr(socket_obj, "close"):
                socket_obj.close()
        except Exception:
            pass
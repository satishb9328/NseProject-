"""
╔══════════════════════════════════════════════════════════════════════════════════╗
║   main.py — Example: Live Market Data Pipeline with FyersSocketManager         ║
║                                                                                  ║
║   This file demonstrates how to:                                                 ║
║     1. Import FyersSocketManager from the Brokers package                       ║
║     2. Start the WebSocket and subscribe to NIFTY 50 + NIFTY Bank symbols       ║
║     3. Consume ticks from the thread-safe queue in a separate consumer thread   ║
║     4. Query the shared in-memory price state from any module                   ║
║     5. React to candle close events for SMC strategy triggers                   ║
║     6. Gracefully shut down on Ctrl+C                                           ║
║                                                                                  ║
║   Usage:                                                                         ║
║     python main.py --token YOUR_ACCESS_TOKEN                                     ║
╚══════════════════════════════════════════════════════════════════════════════════╝
"""

import os
import sys
import time
import logging
import argparse
import threading
from datetime import datetime

# ── Import the Fyers WebSocket manager ───────────────────────────────────────────
# main.py is at:  MyProject2/main.py
# Brokers/ is at: MyProject2/MyProject2/Brokers/
# We use a direct import via the MyProject2 package so PyCharm can resolve it.
from MyProject2.Brokers.fyers_socket import (
    FyersSocketManager,
    Tick,
    Candle,
    nifty_50_eq_symbols,
    nifty_bank_eq_symbols,
)

# ── Logging setup ────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("main")


# ════════════════════════════════════════════════════════════════════════════════
# CALLBACKS
# ════════════════════════════════════════════════════════════════════════════════

def on_tick_callback(tick: Tick) -> None:
    """
    Called on every tick (runs in the WebSocket thread — keep this FAST).

    This is optional.  For heavy processing, use the queue consumer instead
    (see `tick_consumer_thread` below).
    """
    # Example: log every 100th tick for RELIANCE to avoid flooding
    if tick.symbol.endswith("RELIANCE-EQ") and int(tick.ltp * 100) % 100 == 0:
        logger.debug(f"[CALLBACK] {tick.symbol} @ {tick.ltp}")


def on_candle_close_callback(candle: Candle) -> None:
    """
    Called whenever a candle closes (any timeframe).

    This is the ideal hook for running SMC calculations:
        • Detect liquidity sweeps on the closed candle
        • Check for Fair Value Gaps
        • Evaluate Market Structure Shifts
        • Generate entry signals

    The candle is already finalised (candle.closed == True).
    """
    logger.info(
        f"[CANDLE CLOSE] {candle.symbol} | {candle.timeframe} | "
        f"O:{candle.open:.2f} H:{candle.high:.2f} L:{candle.low:.2f} "
        f"C:{candle.close:.2f} V:{candle.volume:,}"
    )

    # ── Placeholder: call your SMC strategy here ────────────────────────────────
    # from MyProject2.Swing_stocks.nifty_advanced_strategy import smc_entry_signal
    # signal = smc_entry_signal(candle_to_dataframe(candle), atr_value=...)
    # if signal["smc_signal"] == "HIGH_CONFLUENCE":
    #     place_order(signal)


# ════════════════════════════════════════════════════════════════════════════════
# TICK CONSUMER THREAD
# ════════════════════════════════════════════════════════════════════════════════

def tick_consumer_thread(manager: FyersSocketManager) -> None:
    """
    Background thread that consumes ticks from the thread-safe queue.

    This is the recommended pattern for heavy processing:
        • The WebSocket thread stays fast (just enqueues ticks)
        • The consumer thread does the heavy lifting (SMC calc, DB writes, etc.)
    """
    logger.info("Tick consumer thread started.")
    while True:
        tick = manager.get_tick(block=True, timeout=1.0)
        if tick is None:
            continue

        # ── Example: print a summary every 50 ticks for top symbols ─────────────
        if int(tick.ltp * 100) % 50 == 0:
            state = manager.get_price_state(tick.symbol)
            if state:
                logger.info(
                    f"[QUEUE] {tick.symbol} | LTP:{state['ltp']:.2f} "
                    f"H:{state['high']:.2f} L:{state['low']:.2f} "
                    f"V:{state['volume']:,}"
                )


# ════════════════════════════════════════════════════════════════════════════════
# PRICE MONITOR — demonstrates querying shared state from any thread
# ════════════════════════════════════════════════════════════════════════════════

def price_monitor_thread(manager: FyersSocketManager) -> None:
    """
    Background thread that periodically queries the shared price state.

    This demonstrates how any module (SMC scanner, order executor, dashboard)
    can read the latest price without touching the WebSocket.
    """
    while True:
        time.sleep(5)

        # ── Query a specific symbol ─────────────────────────────────────────────
        ltp = manager.get_latest_price("NSE:RELIANCE-EQ")
        if ltp:
            logger.info(f"[MONITOR] RELIANCE LTP = {ltp:.2f}")

        # ── Query all symbols ───────────────────────────────────────────────────
        all_states = manager.get_all_price_states()
        logger.info(
            f"[MONITOR] Tracking {len(all_states)} symbols in shared state."
        )

        # ── Query current candle ────────────────────────────────────────────────
        candle_1m = manager.get_current_candle("NSE:SBIN-EQ", "1m")
        if candle_1m:
            logger.info(
                f"[MONITOR] SBIN 1m candle: O={candle_1m.open:.2f} "
                f"H={candle_1m.high:.2f} L={candle_1m.low:.2f} "
                f"C={candle_1m.close:.2f}"
            )


# ════════════════════════════════════════════════════════════════════════════════
# MAIN
# ════════════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="Fyers WebSocket Live Market Data Pipeline"
    )
    parser.add_argument(
        "--token",
        type=str,
        default=os.environ.get("FYERS_ACCESS_TOKEN"),
        help="Fyers API access token (or set FYERS_ACCESS_TOKEN env var)",
    )
    parser.add_argument(
        "--symbols",
        type=str,
        nargs="*",
        default=None,
        help="Specific symbols to subscribe to (default: NIFTY 50 + NIFTY Bank)",
    )
    args = parser.parse_args()

    if not args.token:
        logger.error(
            "No access token provided. "
            "Pass --token or set the FYERS_ACCESS_TOKEN environment variable."
        )
        sys.exit(1)

    # ── Build symbol list ───────────────────────────────────────────────────────
    if args.symbols:
        symbols_to_track = args.symbols
    else:
        symbols_to_track = nifty_50_eq_symbols() + nifty_bank_eq_symbols()
        logger.info(
            f"Using default symbol set: "
            f"{len(nifty_50_eq_symbols())} NIFTY 50 + "
            f"{len(nifty_bank_eq_symbols())} NIFTY Bank = "
            f"{len(symbols_to_track)} total"
        )

    # ── Create the WebSocket manager ────────────────────────────────────────────
    manager = FyersSocketManager(
        access_token=args.token,
        symbols=symbols_to_track,
        on_tick=on_tick_callback,
        on_candle_close=on_candle_close_callback,
        tick_queue_maxsize=20_000,
    )

    # ── Start the WebSocket (daemon thread) ─────────────────────────────────────
    manager.start()

    # ── Start consumer threads (daemon) ─────────────────────────────────────────
    consumer = threading.Thread(
        target=tick_consumer_thread,
        args=(manager,),
        name="tick-consumer",
        daemon=True,
    )
    consumer.start()

    monitor = threading.Thread(
        target=price_monitor_thread,
        args=(manager,),
        name="price-monitor",
        daemon=True,
    )
    monitor.start()

    # ── Main loop — keep the process alive ──────────────────────────────────────
    logger.info("=" * 60)
    logger.info("Pipeline is running. Press Ctrl+C to stop.")
    logger.info(f"  Queue size: {manager.tick_queue_size()} ticks buffered")
    logger.info("=" * 60)

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Shutdown requested...")
    finally:
        manager.stop()
        logger.info("Pipeline stopped. Goodbye.")


if __name__ == "__main__":
    main()

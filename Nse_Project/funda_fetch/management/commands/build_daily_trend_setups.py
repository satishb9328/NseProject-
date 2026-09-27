"""Build the daily ``TrendSetup`` table from ``master_trend_setups.csv``.

Daily MTF half of the pipeline (no Upstox calls). Reads the fundamentally-cleared
candidates produced by ``trend_ready.py`` and, for each symbol, runs the expensive
MTF calculations — ``build_mtf_dataframe`` (parquet reads from
``D:/Api data/ML_HISTO_DATA/...``), ``scan_mtf_patterns``, ``detect_double_bottom``
and ``detect_trendline_breakout`` — then persists the fully-computed rows into the
``TrendSetup`` table.

This command does NOT touch ``live_sniper_pattern`` — that field is owned by the
``update_live_sniper`` command (run every 15 minutes).

    python manage.py build_daily_trend_setups
    python manage.py build_daily_trend_setups --symbols RELIANCE,TCS,SBILIFE
    python manage.py build_daily_trend_setups --keep-stale
"""
import os
from pathlib import Path

import pandas as pd
from django.core.management.base import BaseCommand, CommandError

from candlestick_scanner import scan_mtf_patterns
from chart_patterns import detect_double_bottom, detect_trendline_breakout
from data_engine import build_mtf_dataframe

from Nse_Project.funda_fetch.models import TrendSetup

APP_DIR = Path(__file__).resolve().parents[2]  # .../Nse_Project/funda_fetch
SETUPS_CSV = APP_DIR / "master_trend_setups.csv"
UNIVERSE_CSV_CANDIDATES = [
    APP_DIR / "ind_nifty500list.csv",          # used by trend_ready.py
    APP_DIR.parent / "ind_nifty500list.csv",   # used by the old view
]

# Frontend string mapping for the MTF candlestick signals (identical to the
# mapping that used to live in views.py).
SIGNAL_LABELS = {
    "hammer": "🔨 D-Hammer",
    "bullish_engulfing": "📈 D-Engulf",
    "fresh_golden_cross": "✨ Golden Cross",
    "weekly_hammer": "🔨 W-Hammer",
    "weekly_bullish_engulfing": "📈 W-Engulf",
    "weekly_bearish_engulfing": "📉 W-Bear Engulf",
    "double_bottom": "✌️ W-Pattern",
    "daily_tl": "🚀 D-TL Breakout",
    "weekly_tl": "🚀 W-TL Breakout",
}

# "Recent" lookback windows (trading days) for signal extraction.
STRUCTURAL_LOOKBACK = 3
CANDLE_LOOKBACK = 2

# Signal keys grouped by their lookback window.
STRUCTURAL_SIGNAL_KEYS = {"fresh_golden_cross", "double_bottom", "daily_tl", "weekly_tl"}
CANDLE_SIGNAL_KEYS = {
    "hammer",
    "bullish_engulfing",
    "weekly_hammer",
    "weekly_bullish_engulfing",
    "weekly_bearish_engulfing",
}

# 🔥 HOT breakout-candidate thresholds (all three must hold for the badge).
HOT_52W_POSITION_MIN = 75.0   # "52W Position" (pct_above_52w_low) > 75%
HOT_MOMENTUM_MIN = 50.0       # Momentum_Score > 50
HOT_VOL_DRY_MAX = 0.75        # Volume_Dry_Ratio < 0.75


def _clean_str(value):
    if value is None:
        return ""
    return str(value).strip()


def _to_float(value):
    if value is None:
        return None
    try:
        return float(str(value).strip().replace(",", "").replace("%", ""))
    except (TypeError, ValueError):
        return None


def _to_bool(value):
    if value is None:
        return False
    if isinstance(value, str):
        return value.strip().lower() in ("true", "1", "yes", "y")
    return bool(value)


class Command(BaseCommand):
    help = ("Build the daily TrendSetup table by running the MTF signal pipeline "
            "over master_trend_setups.csv (no Upstox/sniper calls).")

    def add_arguments(self, parser):
        parser.add_argument(
            "--csv", type=str, default=None,
            help="Path to master_trend_setups.csv (default: the app-local file).")
        parser.add_argument(
            "--symbols", type=str, default=None,
            help="Comma-separated symbols to limit processing (e.g. RELIANCE,TCS).")
        parser.add_argument(
            "--keep-stale", action="store_true",
            help="Keep TrendSetup rows missing from the CSV (skip pruning).")

    def handle(self, *args, **options):
        csv_path = options["csv"] or str(SETUPS_CSV)
        if not os.path.exists(csv_path):
            raise CommandError(
                f"Master CSV not found: {csv_path} — run trend_ready.py first.")

        records = self._read_csv(csv_path)
        if not records:
            raise CommandError("Master CSV is empty; the TrendSetup table was left untouched.")

        if options["symbols"]:
            wanted = {s.strip().upper() for s in options["symbols"].split(",") if s.strip()}
            records = [r for r in records if _clean_str(r.get("symbol")).upper() in wanted]
            if not records:
                raise CommandError("No rows matched the --symbols filter.")

        isin_map = self._load_isin_map()
        self.stdout.write(f"Processing {len(records)} symbols "
                          f"({len(isin_map)} ISIN mappings loaded).")

        self.signal_errors = 0
        created = updated = db_failed = 0
        processed_symbols = set()

        for i, rec in enumerate(records, 1):
            symbol = _clean_str(rec.get("symbol"))
            if not symbol:
                continue

            self.stdout.write(f"  [{i:3d}/{len(records)}] {symbol:<12s}", ending="")
            signals = self._detect_signals(symbol, isin_map.get(symbol.upper()))

            try:
                _, was_created = TrendSetup.objects.update_or_create(
                    symbol=symbol,
                    defaults=self._build_defaults(rec, signals),
                )
            except Exception as exc:  # noqa: BLE001
                db_failed += 1
                self.stdout.write(self.style.ERROR(f" DB FAILED: {exc}"))
                continue

            processed_symbols.add(symbol.upper())
            if was_created:
                created += 1
                self.stdout.write(self.style.SUCCESS(" created"))
            else:
                updated += 1
                self.stdout.write(self.style.SUCCESS(" updated"))

        removed = 0
        # Only prune on a full run. A selective --symbols run must never delete
        # rows it did not process.
        if not options["keep_stale"] and not options["symbols"] and processed_symbols:
            removed = TrendSetup.objects.exclude(symbol__in=processed_symbols).delete()[0]

        self.stdout.write(self.style.SUCCESS(
            f"\nDone: {created} created, {updated} updated, "
            f"{self.signal_errors} signal errors, {db_failed} DB errors, {removed} pruned."
        ))

    # ── helpers ─────────────────────────────────────────────────────────────

    def _read_csv(self, csv_path):
        df = pd.read_csv(csv_path)
        df.columns = [str(c).strip() for c in df.columns]
        # Replace NaN with None so downstream coercion yields None / "" / False.
        return df.where(pd.notnull(df), None).to_dict("records")

    def _load_isin_map(self):
        mapping = {}
        for path in UNIVERSE_CSV_CANDIDATES:
            if not path.exists():
                continue
            try:
                uni = pd.read_csv(path)
                uni.columns = [str(c).strip() for c in uni.columns]
                sym_col = next((c for c in uni.columns if c.lower() == "symbol"), None)
                isin_col = next((c for c in uni.columns if "isin" in c.lower()), None)
                if sym_col is None or isin_col is None:
                    continue
                for sym, isin in zip(uni[sym_col], uni[isin_col]):
                    key = str(sym).strip().upper()
                    if key and pd.notna(isin):
                        mapping[key] = str(isin).strip()
                if mapping:
                    break
            except Exception:  # noqa: BLE001
                continue
        return mapping

    def _detect_signals(self, symbol, isin):
        """Run the MTF pipeline for one symbol; return [] on any failure."""
        try:
            df = build_mtf_dataframe(symbol, isin=isin)
            signals_df = scan_mtf_patterns(df)
            w_pattern = detect_double_bottom(df)
            d_tl = detect_trendline_breakout(df, high_col="High", close_col="Close")
            w_tl = detect_trendline_breakout(df, high_col="Weekly_High",
                                             close_col="Weekly_Close")
        except Exception as exc:  # noqa: BLE001
            self.signal_errors += 1
            self.stdout.write(self.style.WARNING(f" signal=[] ({exc})"), ending="")
            return []

        series_map = {}
        if signals_df is not None and not signals_df.empty:
            for col in signals_df.columns:
                if col in SIGNAL_LABELS:
                    series_map[col] = signals_df[col]
        if w_pattern is not None and len(w_pattern):
            series_map["double_bottom"] = w_pattern
        if d_tl is not None and len(d_tl):
            series_map["daily_tl"] = d_tl
        if w_tl is not None and len(w_tl):
            series_map["weekly_tl"] = w_tl

        signals = []
        for key, label in SIGNAL_LABELS.items():
            series = series_map.get(key)
            if series is None:
                continue
            window = CANDLE_LOOKBACK if key in CANDLE_SIGNAL_KEYS else STRUCTURAL_LOOKBACK
            if bool(series.iloc[-window:].any()):
                signals.append(label)
        return signals

    def _build_defaults(self, rec, signals):
        pos = _to_float(rec.get("pct_above_52w_low"))
        momentum = _to_float(rec.get("Momentum_Score"))
        vol = _to_float(rec.get("Volume_Dry_Ratio"))

        is_hot = (
            pos is not None and pos > HOT_52W_POSITION_MIN
            and momentum is not None and momentum > HOT_MOMENTUM_MIN
            and vol is not None and vol < HOT_VOL_DRY_MAX
        )

        return {
            "name": _clean_str(rec.get("name")),
            "industry": _clean_str(rec.get("industry")),
            "setup_category": _clean_str(rec.get("Setup_Category")),
            "close_price": _to_float(rec.get("last_close")),
            "position_52w": pos,
            "weekly_ema_signal": _clean_str(rec.get("weekly_ema20_signal")),
            "momentum": momentum,
            "volume_dry_ratio": vol,
            "weekly_vol_dry": _to_float(rec.get("Weekly_Vol_Ratio")),
            "trend_status": _to_bool(rec.get("uptrend_50_200")),
            "is_hot": is_hot,
            "signals": signals or [],
            "last_date": _clean_str(rec.get("last_date")),
        }

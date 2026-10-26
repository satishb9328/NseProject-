import datetime
import tempfile
from decimal import Decimal
from pathlib import Path
from unittest.mock import Mock, patch

import pandas as pd

from django.test import TestCase
from django.urls import reverse
from django.utils import timezone

from . import live_ml_scanner
from .live_ml_scanner import qualifies_mega_reversal
from .models import LiveSignal, TrendSetup


class MegaReversalConditionTests(TestCase):
    def test_requires_close_below_midline_and_live_price_within_three_percent(self):
        midline = 100.0

        self.assertFalse(qualifies_mega_reversal(100.0, 101.0, midline))
        self.assertFalse(qualifies_mega_reversal(99.0, 99.99, midline))
        self.assertTrue(qualifies_mega_reversal(99.0, 100.0, midline))
        self.assertTrue(qualifies_mega_reversal(99.0, 103.0, midline))
        self.assertFalse(qualifies_mega_reversal(99.0, 103.01, midline))

    def test_rejects_non_positive_midline(self):
        self.assertFalse(qualifies_mega_reversal(99.0, 100.0, 0.0))


class LiveScannerDataTests(TestCase):
    def test_upstox_utc_timestamps_are_normalized_to_ist(self):
        response = Mock()
        response.status_code = 200
        response.json.return_value = {
            "data": {
                "candles": [[
                    "2026-10-20T03:45:00Z",
                    "100",
                    "101",
                    "99",
                    "100.5",
                    "1200",
                    "0",
                ], [
                    "2026-10-20T03:46:00Z",
                    "100",
                    "98",
                    "97",
                    "99",
                    "800",
                    "0",
                ]]
            }
        }

        with patch.object(live_ml_scanner.requests, "get", return_value=response):
            result = live_ml_scanner.fetch_latest_1m_data("NSE_EQ|TEST", {}, "TEST")

        self.assertEqual(len(result), 1)
        self.assertEqual(result.iloc[0]["Datetime"], pd.Timestamp("2026-10-20 09:15"))
        self.assertEqual(result.iloc[0]["Volume"], 1200)

    def test_2h_bars_align_to_market_open_and_drop_forming_candle(self):
        timestamps = pd.date_range(
            "2026-10-20 09:15",
            "2026-10-20 15:25",
            freq="5min",
        )
        candles = pd.DataFrame({
            "Datetime": timestamps,
            "Open": 100.0,
            "High": 101.0,
            "Low": 99.0,
            "Close": 100.5,
            "Volume": 1000,
            "OI": 0,
        })
        ist = datetime.timezone(datetime.timedelta(hours=5, minutes=30))

        frame = live_ml_scanner.build_2h_frame(
            candles,
            datetime.datetime(2026, 10, 20, 12, 0, tzinfo=ist),
        )

        self.assertEqual(list(frame.index), [pd.Timestamp("2026-10-20 09:15")])


class LiveSignalEndpointTests(TestCase):
    def test_endpoint_returns_only_signals_for_local_today(self):
        today = timezone.localdate()
        LiveSignal.objects.create(
            stock_symbol="TODAY",
            signal_type="Mega Reversal",
            trigger_price=Decimal("101.00"),
            weekly_midline=Decimal("100.00"),
            date=today,
        )
        LiveSignal.objects.create(
            stock_symbol="YESTERDAY",
            signal_type="Mega Reversal",
            trigger_price=Decimal("101.00"),
            weekly_midline=Decimal("100.00"),
            date=today - datetime.timedelta(days=1),
        )

        response = self.client.get(reverse("live_signals_today"))

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            [row["stock_symbol"] for row in response.json()["signals"]],
            ["TODAY"],
        )

    def test_dashboard_contains_live_signals_control(self):
        response = self.client.get(reverse("trend_setups"))

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Show Live Signals")
        self.assertContains(response, "/trend-setups/live-signals/")

    def test_get_or_create_prevents_same_day_duplicates(self):
        today = timezone.localdate()
        lookup = {
            "stock_symbol": "RELIANCE",
            "signal_type": "Mega Reversal",
            "date": today,
        }
        defaults = {
            "trigger_price": Decimal("101.00"),
            "weekly_midline": Decimal("100.00"),
        }

        first, created_first = LiveSignal.objects.get_or_create(
            **lookup, defaults=defaults
        )
        second, created_second = LiveSignal.objects.get_or_create(
            **lookup,
            defaults={
                "trigger_price": Decimal("102.00"),
                "weekly_midline": Decimal("100.00"),
            },
        )

        self.assertTrue(created_first)
        self.assertFalse(created_second)
        self.assertEqual(first.pk, second.pk)
        self.assertEqual(LiveSignal.objects.filter(**lookup).count(), 1)


class SidebarScannerCountTests(TestCase):
    def test_context_processor_counts_trend_rows_and_smc_csv_records(self):
        TrendSetup.objects.create(symbol="TREND1", setup_category="Bottom Fishing")
        TrendSetup.objects.create(symbol="TREND2", setup_category="VCP Breakout")

        with tempfile.TemporaryDirectory() as temp_dir:
            smc_csv = Path(temp_dir) / "setups.csv"
            smc_csv.write_text(
                "Symbol,Signal\nSMC1,Ready\nSMC2,Ready\n",
                encoding="utf-8",
            )
            with patch(
                "Nse_Project.funda_fetch.context_processors.SMC_SETUP_CSV",
                smc_csv,
            ):
                response = self.client.get(reverse("trend_setups"))

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.context["total_trend_count"], 2)
        self.assertEqual(response.context["ready_smc_count"], 2)

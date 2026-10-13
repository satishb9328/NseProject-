import datetime
import tempfile
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

from django.test import TestCase
from django.urls import reverse
from django.utils import timezone

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

"""Shared scanner counts for the dashboard sidebar."""

import csv
import logging
from pathlib import Path

from .models import TrendSetup

logger = logging.getLogger(__name__)
SMC_SETUP_CSV = (
    Path(__file__).resolve().parents[1]
    / "52_Week_Low"
    / "nifty500_pure_smc_trade_setups.csv"
)


def sidebar_scanner_counts(request):
    """Expose current trend and SMC setup counts to every template."""
    total_trend_count = TrendSetup.objects.count()
    ready_smc_count = 0

    try:
        with SMC_SETUP_CSV.open(newline="", encoding="utf-8-sig") as csv_file:
            ready_smc_count = sum(
                1 for row in csv.DictReader(csv_file) if row.get("Symbol")
            )
    except FileNotFoundError:
        logger.warning("SMC setup CSV not found: %s", SMC_SETUP_CSV)
    except (OSError, csv.Error):
        logger.exception("Failed to read SMC setup CSV: %s", SMC_SETUP_CSV)

    return {
        "total_trend_count": total_trend_count,
        "ready_smc_count": ready_smc_count,
    }

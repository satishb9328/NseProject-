"""Refresh ``TrendSetup.live_sniper_pattern`` from the 2H live sniper bot.

Runs every 15 minutes during market hours. It ONLY performs the live Upstox
sniper scan and updates the ``live_sniper_pattern`` column — it never touches the
heavy MTF/parquet calculations owned by ``build_daily_trend_setups``.

    python manage.py update_live_sniper
"""
from django.core.management.base import BaseCommand
from django.utils import timezone

from Nse_Project.funda_fetch.models import LiveSignal, TrendSetup

# Live 2H "Sniper" bridge (live_ml_scanner.py in this same app). Imported
# defensively so the command still runs if the scanner is missing.
try:
    from Nse_Project.funda_fetch.live_ml_scanner import main as run_live_scanner
except Exception:  # noqa: BLE001
    run_live_scanner = None


def get_live_sniper_result() -> dict:
    """Return the scanner result, including both 2H and Mega Reversal signals."""
    if run_live_scanner is None:
        return {}

    try:
        result = run_live_scanner()
        return result if isinstance(result, dict) else {}
    except Exception:  # noqa: BLE001
        return {}


def get_live_sniper_alerts() -> dict:
    """Return a ``{SYMBOL: pattern_label}`` map from the 2H live sniper bot."""
    result = get_live_sniper_result()
    signals = result.get("signals", [])
    return {
        str(alert.get("symbol", "")).strip().upper(): alert.get("pattern")
        for alert in signals
        if alert.get("symbol") and alert.get("pattern")
    }


class Command(BaseCommand):
    help = ("Refresh TrendSetup.live_sniper_pattern from the 2H live sniper bot "
            "(Upstox). Does not touch MTF/parquet data.")

    def handle(self, *args, **options):
        self.stdout.write("Scanning live 2H sniper patterns (Upstox) ...")
        result = get_live_sniper_result()
        signals = result.get("signals", [])
        mega_signals = result.get("mega_reversal_signals", [])
        live_alerts = {
            str(alert.get("symbol", "")).strip().upper(): alert.get("pattern")
            for alert in signals
            if alert.get("symbol") and alert.get("pattern")
        }
        self.stdout.write(f"  -> {len(live_alerts)} live sniper patterns found.")

        changed = []
        for obj in TrendSetup.objects.all():
            new_pattern = live_alerts.get(obj.symbol.upper())
            if obj.live_sniper_pattern != new_pattern:
                obj.live_sniper_pattern = new_pattern
                changed.append(obj)

        if changed:
            TrendSetup.objects.bulk_update(changed, ["live_sniper_pattern"])
            self.stdout.write(self.style.SUCCESS(
                f"Updated live_sniper_pattern on {len(changed)} rows."))
        else:
            self.stdout.write("No live sniper pattern changes.")

        today = timezone.localdate()
        created_count = 0
        for signal in mega_signals:
            _, created = LiveSignal.objects.get_or_create(
                stock_symbol=str(signal["symbol"]).strip().upper(),
                signal_type=signal.get("signal_type", "Mega Reversal"),
                date=today,
                defaults={
                    "trigger_price": signal["trigger_price"],
                    "weekly_midline": signal["weekly_midline"],
                },
            )
            created_count += int(created)

        self.stdout.write(f"  -> {created_count} new Mega Reversal signals saved.")

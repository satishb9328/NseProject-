"""Refresh ``TrendSetup.live_sniper_pattern`` from the 2H live sniper bot.

Runs every 15 minutes during market hours. It ONLY performs the live Upstox
sniper scan and updates the ``live_sniper_pattern`` column — it never touches the
heavy MTF/parquet calculations owned by ``build_daily_trend_setups``.

    python manage.py update_live_sniper
"""
from django.core.management.base import BaseCommand

from Nse_Project.funda_fetch.models import TrendSetup

# Live 2H "Sniper" bridge (live_ml_scanner.py in this same app). Imported
# defensively so the command still runs if the scanner is missing.
try:
    from Nse_Project.funda_fetch.live_ml_scanner import main as run_live_scanner
except Exception:  # noqa: BLE001
    run_live_scanner = None


def get_live_sniper_alerts() -> dict:
    """Return a ``{SYMBOL: pattern_label}`` map from the 2H live sniper bot."""
    if run_live_scanner is None:
        return {}

    try:
        result = run_live_scanner()
        signals = result.get("signals", []) if isinstance(result, dict) else []
        return {
            str(a.get("symbol", "")).strip().upper(): a.get("pattern")
            for a in signals
            if a.get("symbol") and a.get("pattern")
        }
    except Exception:  # noqa: BLE001
        return {}


class Command(BaseCommand):
    help = ("Refresh TrendSetup.live_sniper_pattern from the 2H live sniper bot "
            "(Upstox). Does not touch MTF/parquet data.")

    def handle(self, *args, **options):
        self.stdout.write("Scanning live 2H sniper patterns (Upstox) ...")
        live_alerts = get_live_sniper_alerts()
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

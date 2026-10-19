"""
Scheduled wrapper script for fetch_preopen.py
Checks if today (in IST) is a trading day (weekday and not a holiday) before running.

The 09:00 AM trigger itself is owned by the Windows Task Scheduler
(setup_scheduler.ps1); this script only decides *whether* to run once triggered.
"""
import os
import sys
import json
import logging
from datetime import datetime, date

import pytz

# IST is the single source of truth for "today" — never rely on local system time.
IST = pytz.timezone('Asia/Kolkata')

# Configure logging
log_file = os.path.join(os.path.dirname(__file__), 'scheduler.log')
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)

# Cache the holiday list so it is only read from disk once per process.
_holidays_cache = None


def load_holidays():
    """Load the NSE holiday list from JSON (cached after the first read)."""
    global _holidays_cache
    if _holidays_cache is not None:
        return _holidays_cache

    holidays_file = os.path.join(os.path.dirname(__file__), 'nse_holidays.json')
    try:
        with open(holidays_file, 'r') as f:
            data = json.load(f)
        # Convert string dates to date objects
        _holidays_cache = [datetime.strptime(d, '%Y-%m-%d').date() for d in data['holidays']]
    except FileNotFoundError:
        logging.warning(f"Holidays file not found: {holidays_file}")
        _holidays_cache = []
    except Exception as e:
        logging.error(f"Error loading holidays: {e}")
        _holidays_cache = []
    return _holidays_cache


def is_trading_day(check_date=None):
    """
    Check if the given date is a trading day.
    Returns True if it's a weekday and not a holiday.
    Special case: February 1, 2026 is treated as trading day for budget announcement.
    """
    if check_date is None:
        check_date = datetime.now(IST).date()

    # Special case for budget announcement day
    if check_date == date(2026, 2, 1):
        return True

    # Check if weekend (Saturday=5, Sunday=6)
    if check_date.weekday() >= 5:
        return False

    # Check if holiday
    holidays = load_holidays()
    if check_date in holidays:
        return False

    return True


def main():
    """Main scheduler function."""
    now = datetime.now(IST)
    today = now.date()

    logging.info("="*70)
    logging.info(f"Preopen Data Scheduler - Starting at {now.strftime('%Y-%m-%d %H:%M:%S')} IST")
    logging.info("="*70)

    # Check if today is a trading day
    if not is_trading_day(today):
        day_name = today.strftime('%A')
        holidays = load_holidays()

        if today.weekday() >= 5:
            logging.info(f"[SKIP] Today is {day_name} (weekend)")
        elif today in holidays:
            logging.info(f"[SKIP] Today ({today.strftime('%Y-%m-%d')}) is a NSE holiday")

        logging.info("Script execution cancelled - not a trading day")
        return 0

    # It's a trading day - run the fetch script
    logging.info(f"[OK] Today is a trading day ({today.strftime('%A, %Y-%m-%d')})")
    logging.info("Executing fetch_preopen.py...")
    logging.info("-"*70)

    try:
        # Import and run the fetch function
        from fetch_preopen import download_graph_csv

        download_graph_csv()
        logging.info("[SUCCESS] Preopen data fetched successfully!")

        # Publish the final 09:10 settlement snapshot to Redis so the dashboard
        # updates in real time (no page refresh needed).
        try:
            import django
            os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'Nse_Project.settings')
            project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
            if project_root not in sys.path:
                sys.path.insert(0, project_root)
            django.setup()
            from Nse_Project.Live_dashboard.views import publish_preopen_trap_to_redis
            publish_preopen_trap_to_redis()
            logging.info("[SUCCESS] Preopen snapshot published to Redis.")
        except Exception as pub_exc:
            logging.error(f"[WARN] Could not publish preopen snapshot to Redis: {pub_exc}")

        return 0

    except Exception as e:
        logging.error(f"[ERROR] Error fetching preopen data: {e}", exc_info=True)
        return 1


if __name__ == '__main__':
    try:
        sys.exit(main())
    except Exception:
        logging.exception("FATAL: unexpected error in scheduler")
        sys.exit(1)

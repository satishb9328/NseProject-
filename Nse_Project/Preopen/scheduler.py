"""
Scheduled wrapper script for fetch_preopen.py
Checks if today is a trading day (weekday and not a holiday) before running.
"""
import os
import sys
import json
import logging
from datetime import datetime, date

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


def load_holidays():
    """Load NSE holiday list from JSON file."""
    holidays_file = os.path.join(os.path.dirname(__file__), 'nse_holidays.json')
    try:
        with open(holidays_file, 'r') as f:
            data = json.load(f)
            # Convert string dates to date objects
            holidays = [datetime.strptime(d, '%Y-%m-%d').date() for d in data['holidays']]
            return holidays
    except FileNotFoundError:
        logging.warning(f"Holidays file not found: {holidays_file}")
        return []
    except Exception as e:
        logging.error(f"Error loading holidays: {e}")
        return []


def is_trading_day(check_date=None):
    """
    Check if the given date is a trading day.
    Returns True if it's a weekday and not a holiday.
    Special case: February 1, 2026 is treated as trading day for budget announcement.
    """
    if check_date is None:
        check_date = date.today()

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
    today = date.today()
    now = datetime.now()

    logging.info("="*70)
    logging.info(f"Preopen Data Scheduler - Starting at {now.strftime('%Y-%m-%d %H:%M:%S')}")
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
        from fetch_preopen import fetch_and_save_preopen_data_selenium

        fetch_and_save_preopen_data_selenium(use_selenium=False)
        logging.info("[SUCCESS] Preopen data fetched successfully!")
        return 0

    except Exception as e:
        logging.error(f"[ERROR] Error fetching preopen data: {e}", exc_info=True)
        return 1


if __name__ == '__main__':
    exit_code = main()
    sys.exit(exit_code)


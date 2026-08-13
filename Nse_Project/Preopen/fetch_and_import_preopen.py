#!/usr/bin/env python3
"""Clean wrapper for Preopen fetch + import.

Uses the existing Preopen app modules and avoids duplicate script logic.
"""

import argparse
import asyncio
import os
import sys

if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)
os.chdir(PROJECT_ROOT)
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'Nse_Project.settings')


def main():
    parser = argparse.ArgumentParser(
        description='Fetch preopen CSV from NSE and import it into the Django PreopenSummary model.'
    )
    parser.add_argument('--skip-fetch', action='store_true', help='Skip the fetch step and use existing local files.')
    parser.add_argument('--skip-import', action='store_true', help='Skip the database import step.')
    parser.add_argument('--verbose', action='store_true', help='Print extra status information.')
    args = parser.parse_args()

    try:
        from Nse_Project.Preopen.fetch_preopen import download_graph_csv
    except Exception as exc:
        print('ERROR: cannot import Nse_Project.Preopen.fetch_preopen:', exc)
        raise

    try:
        from Nse_Project.Preopen.import_preopen_to_db import main as import_preopen_data_to_db
    except Exception as exc:
        print('ERROR: cannot import Nse_Project.Preopen.import_preopen_to_db:', exc)
        raise

    if not args.skip_fetch:
        if args.verbose:
            print('Fetching preopen CSV from NSE...')
        try:
            download_graph_csv()
            if args.verbose:
                print('Fetch completed successfully.')
        except Exception as exc:
            print('ERROR: fetch failed:', exc)
            print('Continuing to import stage if skip-import is not set.')

    if not args.skip_import:
        if args.verbose:
            print('Importing preopen data into the database...')
        try:
            import_preopen_data_to_db()
            if args.verbose:
                print('Import completed successfully.')
        except Exception as exc:
            print('ERROR: import failed:', exc)
            raise

    if not args.skip_fetch and not args.skip_import:
        print('\n=== fetch_and_import_preopen - Copy summary ===')
        print('Fetch: SUCCESS' if not args.skip_fetch else 'Fetch: SKIPPED')
        print('Import: SUCCESS' if not args.skip_import else 'Import: SKIPPED')


if __name__ == '__main__':
    main()


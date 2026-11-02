#!/usr/bin/env python3
"""Clean, fail-fast wrapper for Preopen fetch + import + Redis publish.

Order of operations is strict: if the fetch step fails, the whole run HALTS with a
non-zero exit code and the import/publish steps are skipped. This prevents the old
false-positive behaviour where a failed download still printed "SUCCESS" and updated
a stale legacy date.
"""

import argparse
import os
import sys
from pathlib import Path

# Repository root (the folder that holds manage.py and the top-level preopen dir).
# Derived from __file__ so it resolves identically on Windows (local) and Linux (Render).
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
os.chdir(PROJECT_ROOT)
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'Nse_Project.settings')

# RENDER mode persists ticks straight to Supabase, so there is no local CSV to import.
DEPLOY_ENV = os.getenv("DEPLOY_ENV", "LOCAL").strip().upper()


def _print_summary(fetch_ok, import_ok, publish_ok):
    print('\n=== fetch_and_import_preopen summary ===')
    print('Fetch: SUCCESS' if fetch_ok else 'Fetch: FAILED')

    if import_ok is None:
        print('Import: SKIPPED (fetch failed)')
    else:
        print('Import: SUCCESS' if import_ok else 'Import: FAILED')

    if publish_ok is None:
        print('Publish: SKIPPED (earlier step failed)')
    else:
        print('Publish: SUCCESS' if publish_ok else 'Publish: FAILED')


def main():
    parser = argparse.ArgumentParser(
        description='Fetch preopen CSV from NSE and import it into the Django PreopenSummary model.'
    )
    parser.add_argument('--skip-fetch', action='store_true', help='Skip the fetch step and use existing local files.')
    parser.add_argument('--skip-import', action='store_true', help='Skip the database import step.')
    parser.add_argument('--skip-publish', action='store_true', help='Skip publishing the final snapshot to Redis.')
    parser.add_argument('--verbose', action='store_true', help='Print extra status information.')
    args = parser.parse_args()

    # ---- 1. FETCH (must succeed before anything else) ----
    fetch_ok = args.skip_fetch
    if not args.skip_fetch:
        if args.verbose:
            print('Fetching preopen data from NSE...')
        try:
            from Nse_Project.Preopen.fetch_preopen import download_graph_csv
            result = download_graph_csv()
            fetch_ok = True
            print(f'Fetch: SUCCESS — {result}')
        except Exception as exc:
            print(f'Fetch: FAILED — {exc}')

    if not fetch_ok:
        print('Halting: the pre-open download failed, so import and publish will not run.')
        _print_summary(fetch_ok=False, import_ok=None, publish_ok=None)
        return 1

    # ---- 2. IMPORT ----
    # On Render the fetch step already bulk-inserted ticks into Supabase, so there is
    # no CSV to import (and no durable local DB anyway). Only the local CSV ->
    # PreopenSummary import runs when files exist on disk.
    import_ok = args.skip_import or DEPLOY_ENV == "RENDER"
    if DEPLOY_ENV == "RENDER" and not args.skip_import:
        print('Import: SKIPPED (RENDER — ticks already in Supabase preopen_ticks)')
    elif not args.skip_import:
        if args.verbose:
            print('Importing preopen data into the database...')
        try:
            from Nse_Project.Preopen.import_preopen_to_db import main as import_preopen_main
            exit_code = import_preopen_main()
            import_ok = exit_code in (0, None)
            if import_ok:
                print('Import: SUCCESS')
            else:
                print(f'Import: FAILED (exit code {exit_code})')
        except Exception as exc:
            print(f'Import: FAILED — {exc}')
            import_ok = False

    if not import_ok:
        print('Halting: the database import failed, so publish will not run.')
        _print_summary(fetch_ok=True, import_ok=False, publish_ok=None)
        return 1

    # ---- 3. PUBLISH ----
    publish_ok = args.skip_publish
    if not args.skip_publish:
        if args.verbose:
            print('Publishing the final 09:10 pre-open snapshot to Redis...')
        try:
            from Nse_Project.Live_dashboard.views import publish_preopen_trap_to_redis
            publish_preopen_trap_to_redis()
            publish_ok = True
            print('Publish: SUCCESS')
        except Exception as exc:
            print(f'Publish: FAILED — {exc}')
            publish_ok = False

    _print_summary(fetch_ok=True, import_ok=True, publish_ok=publish_ok)
    return 0 if publish_ok else 1


if __name__ == '__main__':
    try:
        sys.exit(main())
    except Exception as exc:
        print(f'FATAL: unexpected error in orchestrator: {exc}')
        sys.exit(1)

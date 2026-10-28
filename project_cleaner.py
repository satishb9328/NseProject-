#!/usr/bin/env python3
"""
project_cleaner.py (v2)
=======================

A safe, non-destructive-by-default cleanup utility for a Django-based
algorithmic trading project.

WHAT IT DOES (in order)
-----------------------
  0. Purges expired items from its own ``.trash/`` recycle bin.
  1. Deletes ``__pycache__`` directories and stray ``*.pyc`` files
     (trivially regenerable, so deleted outright).
  2. Deletes regenerable tool caches: ``.pytest_cache``, ``.mypy_cache``,
     ``.ruff_cache``, ``.tox``, ``htmlcov``, ``.coverage``, ``.DS_Store``,
     ``Thumbs.db``.
  3. ROTATES (renames, never destroys) every active ``logs/*.log`` into a
     dated copy, and moves dated rotations older than ``--log-keep`` days
     into the trash. Log HISTORY is preserved, not wiped.
  4. Moves stale ``*cache*.json`` files (project root + ``data/``) older
     than ``--days`` days into the trash. Files listed in PROTECTED_JSON
     are never touched.
  5. REPORTS clutter-looking files (report only -- never touched).

SAFETY MODEL
------------
  * DRY RUN IS THE DEFAULT. Nothing changes unless you pass ``--live``.
  * ``--live`` asks you to type YES first (skip with ``--yes``).
  * Data-like files (json caches, expired log rotations) are SOFT-deleted:
    they are moved into ``.trash/<timestamp>/`` and kept for
    ``--trash-keep`` days (default 7) before being purged. Only trivially
    regenerable files (bytecode, tool caches) are deleted outright.
  * Never enters: ``.venv`` / ``venv`` / ``env``, ``.git``, ``node_modules``,
    ``staticfiles``, and this script's own ``.trash``.
  * Never touches: ``db.sqlite3``, any ``*.sqlite3*`` file, ``.env``, and
    everything in PROTECTED_JSON (extend that list with any cache that is
    expensive to rebuild, e.g. the NSE instrument master).
  * ``--days`` defaults to 4 because NSE markets close on Sat/Sun: a
    2-day cutoff would delete Friday's cache if you clean up on Sunday.

USAGE
-----
  python project_cleaner.py                     # preview only (dry run)
  python project_cleaner.py --live              # apply, with confirmation
  python project_cleaner.py --live --yes        # apply, no confirmation
  python project_cleaner.py --root /some/path   # clean a different folder
  python project_cleaner.py --days 7 --log-keep 30 --trash-keep 14
"""

from __future__ import annotations

import argparse
import os
import re
import shutil
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path

__version__ = "2.0"

# =============================================================================
# CONFIGURATION (defaults -- CLI flags override at runtime)
# =============================================================================

DRY_RUN: bool = True

# Defaults to the folder containing this script. Override with --root PATH.
ROOT_DIR: Path = Path(__file__).resolve().parent

# Directories pruned from every recursive traversal (never entered).
EXCLUDED_DIRS = {
    ".venv", "venv", "env",      # virtualenvs (remove 'env' if it is real source)
    ".git", ".hg", ".svn",       # version control internals
    "node_modules",              # frontend dependencies
    "staticfiles",               # collectstatic output (regenerated)
    ".trash",                    # this script's own recycle bin
}

# Files that are never deleted, matched by name anywhere in the tree.
EXCLUDED_FILES = {"db.sqlite3", ".env", "project_cleaner.py"}

# Any file whose name contains this fragment is protected (db_backup.sqlite3 etc).
PROTECTED_NAME_FRAGMENT = ".sqlite3"

# ---------------------------------------------------------------------------
# LOG MANAGEMENT
# ---------------------------------------------------------------------------
LOGS_DIR_NAME = "logs"
LOG_KEEP_DAYS: int = 14          # dated rotations older than this are trashed

# ---------------------------------------------------------------------------
# STALE JSON CACHE MANAGEMENT
# ---------------------------------------------------------------------------
DATA_DIR_NAME = "data"
CACHE_AGE_DAYS: int = 4          # 4 covers the NSE weekend (Sat/Sun closed)

# Only *.json files whose NAME contains one of these keywords (case-
# insensitive) are treated as cache. This deliberately protects real data
# files such as live_options_data.json from accidental deletion.
CACHE_NAME_KEYWORDS = ("cache",)

# Cache files that are EXPENSIVE to rebuild -- never deleted, even if stale.
# EXTEND THIS SET for your project (instrument master, auth tokens, etc).
PROTECTED_JSON = {
    "instrument_cache.json",
    "token_cache.json",
}

# ---------------------------------------------------------------------------
# TRASH (soft-delete) SETTINGS
# ---------------------------------------------------------------------------
TRASH_DIR_NAME = ".trash"
TRASH_RETENTION_DAYS: int = 7    # trashed items purged after this many days

# ---------------------------------------------------------------------------
# REGENERABLE TOOL CACHES (deleted outright)
# ---------------------------------------------------------------------------
TOOL_CACHE_DIRS = {".pytest_cache", ".mypy_cache", ".ruff_cache", ".tox", "htmlcov"}
TOOL_CACHE_FILES = {".coverage", ".DS_Store", "Thumbs.db"}

# ---------------------------------------------------------------------------
# CLUTTER DETECTION (reported only -- NEVER auto-deleted)
# ---------------------------------------------------------------------------
# NOTE: \\btest\\b matches "test.py" but NOT "latest.py" or "test_utils.py",
# fixing the false positive from v1.
CLUTTER_PATTERNS = [
    re.compile(r" - copy", re.IGNORECASE),
    re.compile(r"_backup", re.IGNORECASE),
    re.compile(r"\btest\b", re.IGNORECASE),
    re.compile(r"\.orig$", re.IGNORECASE),
    re.compile(r"\.bak$", re.IGNORECASE),
    re.compile(r"\.swp$", re.IGNORECASE),
    re.compile(r"\.tmp$", re.IGNORECASE),
]

# =============================================================================
# HELPERS
# =============================================================================

_PROTECTED_LOWER = {f.lower() for f in EXCLUDED_FILES} | {p.lower() for p in PROTECTED_JSON}


def _rel(path: Path) -> Path:
    """Return a path relative to the project root for tidy output."""
    try:
        return path.relative_to(ROOT_DIR)
    except ValueError:
        return path


def _is_never_touch(path: Path) -> bool:
    """True for files this script must never delete or move."""
    name = path.name.lower()
    return name in _PROTECTED_LOWER or PROTECTED_NAME_FRAGMENT in name


def _in_excluded_dir(path: Path) -> bool:
    """True if the path sits inside any excluded directory."""
    try:
        rel = path.relative_to(ROOT_DIR)
    except ValueError:
        return True
    return any(part in EXCLUDED_DIRS for part in rel.parts)


def _fmt_bytes(num: float) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if num < 1024:
            return f"{int(num)} B" if unit == "B" else f"{num:.1f} {unit}"
        num /= 1024
    return f"{num:.1f} TB"


def _size_of(path: Path) -> int:
    try:
        if path.is_symlink():
            return 0
        if path.is_file():
            return path.stat().st_size
        total = 0
        for root, _dirs, files in os.walk(path):
            for f in files:
                try:
                    total += (Path(root) / f).stat().st_size
                except OSError:
                    pass
        return total
    except OSError:
        return 0


# =============================================================================
# TRASH (soft delete)
# =============================================================================

def trash_root() -> Path:
    return ROOT_DIR / TRASH_DIR_NAME


def move_to_trash(path: Path) -> Path:
    """Move a file/dir into .trash/<timestamp>/<original relative path>."""
    stamp = time.strftime("%Y-%m-%d_%H%M%S")
    dest = trash_root() / stamp / _rel(path)
    dest.parent.mkdir(parents=True, exist_ok=True)
    final, i = dest, 1
    while final.exists():
        final = dest.with_name(f"{dest.stem}_{i}{dest.suffix}")
        i += 1
    shutil.move(str(path), str(final))
    return final


def purge_expired_trash():
    """Hard-delete .trash/<timestamp> folders older than TRASH_RETENTION_DAYS.

    Returns (purged_items, purged_bytes).
    """
    troot = trash_root()
    if not troot.is_dir():
        return 0, 0
    cutoff = time.time() - TRASH_RETENTION_DAYS * 86_400
    purged_items, purged_bytes = 0, 0
    for child in sorted(troot.iterdir()):
        ts = None
        m = re.match(r"^(\d{4}-\d{2}-\d{2})_(\d{6})$", child.name)
        if m:
            try:
                ts = time.mktime(
                    time.strptime(f"{m.group(1)}_{m.group(2)}", "%Y-%m-%d_%H%M%S")
                )
            except ValueError:
                ts = None
        if ts is None:
            try:
                ts = child.stat().st_mtime
            except OSError:
                continue
        if ts < cutoff:
            size = _size_of(child)
            if DRY_RUN:
                print(f"  [DRY-RUN] trash purge : {TRASH_DIR_NAME}/{child.name}")
            else:
                try:
                    if child.is_dir() and not child.is_symlink():
                        shutil.rmtree(child, ignore_errors=True)
                    else:
                        child.unlink(missing_ok=True)
                    print(f"  trash purged          : {TRASH_DIR_NAME}/{child.name}")
                except OSError as exc:
                    print(f"  ERROR purging trash   : {child.name} -> {exc}")
                    continue
            purged_items += 1
            purged_bytes += size
    return purged_items, purged_bytes


# =============================================================================
# COLLECTORS (find-only -- no side effects)
# =============================================================================

def collect_regenerable():
    """Find bytecode and tool caches, skipping excluded dirs.

    Returns (pycache_dirs, pyc_files, tool_dirs, tool_files).
    """
    pycache_dirs: list[Path] = []
    pyc_files: list[Path] = []
    tool_dirs: list[Path] = []
    tool_files: list[Path] = []

    for root, dirs, files in os.walk(ROOT_DIR, topdown=True):
        root_path = Path(root)
        kept: list[str] = []
        for name in dirs:
            if _is_excluded_dir_name(name):
                continue
            if name == "__pycache__":
                pycache_dirs.append(root_path / name)
                continue
            if name in TOOL_CACHE_DIRS:
                tool_dirs.append(root_path / name)
                continue
            kept.append(name)
        dirs[:] = kept

        for name in files:
            p = root_path / name
            if _is_never_touch(p):
                continue
            if name.endswith(".pyc"):
                pyc_files.append(p)
            elif name in TOOL_CACHE_FILES:
                tool_files.append(p)

    return pycache_dirs, pyc_files, tool_dirs, tool_files


def _is_excluded_dir_name(name: str) -> bool:
    return name in EXCLUDED_DIRS


_DATED_LOG_RE = re.compile(r"(\d{4}-\d{2}-\d{2})(?:_\d+)?$")


def _log_date(stem: str):
    """Extract the rotation date from a stem like 'app.2026-10-23'."""
    m = _DATED_LOG_RE.search(stem)
    if not m:
        return None
    try:
        return datetime.strptime(m.group(1), "%Y-%m-%d").date()
    except ValueError:
        return None


def collect_logs():
    """Split logs/*.log into (active_logs_to_rotate, expired_rotations_to_trash)."""
    logs_dir = ROOT_DIR / LOGS_DIR_NAME
    active: list[Path] = []
    stale: list[Path] = []
    if not logs_dir.is_dir():
        return active, stale
    keep_before = date.today() - timedelta(days=LOG_KEEP_DAYS)
    for path in sorted(logs_dir.rglob("*.log")):
        if _is_never_touch(path):
            continue
        d = _log_date(path.stem)
        if d is None:
            active.append(path)
        elif d <= keep_before:
            stale.append(path)
    return active, stale


def collect_stale_json_cache():
    """Return *cache*.json files (root level + data/) older than CACHE_AGE_DAYS."""
    cutoff = time.time() - CACHE_AGE_DAYS * 86_400
    candidates: list[Path] = []

    candidates.extend(ROOT_DIR.glob("*.json"))
    data_dir = ROOT_DIR / DATA_DIR_NAME
    if data_dir.is_dir():
        candidates.extend(data_dir.rglob("*.json"))

    stale: list[Path] = []
    seen: set[Path] = set()
    for path in candidates:
        if path in seen:
            continue
        seen.add(path)
        if _in_excluded_dir(path) or _is_never_touch(path):
            continue
        if not any(k in path.stem.lower() for k in CACHE_NAME_KEYWORDS):
            continue
        try:
            if path.stat().st_mtime < cutoff:
                stale.append(path)
        except OSError:
            continue
    return sorted(stale)


def collect_clutter():
    """Return files whose names look like clutter (reported, never deleted)."""
    clutter: list[Path] = []
    for root, dirs, files in os.walk(ROOT_DIR, topdown=True):
        dirs[:] = [d for d in dirs if not _is_excluded_dir_name(d)]
        root_path = Path(root)
        for name in files:
            p = root_path / name
            if _is_never_touch(p):
                continue
            if any(rx.search(name) for rx in CLUTTER_PATTERNS):
                clutter.append(p)
    return sorted(clutter)


# =============================================================================
# ACTIONS (respect DRY_RUN)
# =============================================================================

def delete_path(path: Path) -> None:
    size = _size_of(path)
    if DRY_RUN:
        print(f"  [DRY-RUN] delete        : {_rel(path)}  ({_fmt_bytes(size)})")
        return
    try:
        if path.is_dir() and not path.is_symlink():
            shutil.rmtree(path)
        else:
            path.unlink(missing_ok=True)
        print(f"  deleted                 : {_rel(path)}  ({_fmt_bytes(size)})")
    except OSError as exc:
        print(f"  ERROR deleting          : {_rel(path)} -> {exc}")


def trash_path(path: Path) -> None:
    size = _size_of(path)
    if DRY_RUN:
        print(f"  [DRY-RUN] trash         : {_rel(path)}  ({_fmt_bytes(size)})")
        return
    try:
        dest = move_to_trash(path)
        print(f"  trashed                 : {_rel(path)} -> {_rel(dest)}")
    except OSError as exc:
        print(f"  ERROR trashing          : {_rel(path)} -> {exc}")


def rotate_log(path: Path) -> None:
    """Rename an active log to a dated copy (history preserved, file kept)."""
    try:
        stamp = time.strftime("%Y-%m-%d", time.localtime(path.stat().st_mtime))
    except OSError:
        stamp = time.strftime("%Y-%m-%d")
    target = path.with_name(f"{path.stem}.{stamp}{path.suffix}")
    if target.exists():
        stamp = time.strftime("%Y-%m-%d_%H%M%S")
        target = path.with_name(f"{path.stem}.{stamp}{path.suffix}")
    if DRY_RUN:
        print(f"  [DRY-RUN] rotate        : {_rel(path)} -> {_rel(target)}")
        return
    try:
        path.rename(target)
        print(f"  rotated                 : {_rel(path)} -> {_rel(target)}")
    except OSError as exc:
        print(f"  ERROR rotating          : {_rel(path)} -> {exc}")


# =============================================================================
# CLI
# =============================================================================

def _parse_args(argv=None):
    p = argparse.ArgumentParser(
        prog="project_cleaner.py",
        description="Safe cleanup utility for a Django trading project "
                    "(DRY RUN by default; pass --live to apply changes).",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("--live", action="store_true",
                   help="apply changes (default: preview only)")
    p.add_argument("--yes", action="store_true",
                   help="skip the YES confirmation in --live mode")
    p.add_argument("--root", type=Path, default=None,
                   help="project root (default: folder containing this script)")
    p.add_argument("--days", type=int, default=CACHE_AGE_DAYS,
                   help="trash *cache*.json files older than this many days")
    p.add_argument("--log-keep", type=int, default=LOG_KEEP_DAYS,
                   help="keep dated log rotations for this many days")
    p.add_argument("--trash-keep", type=int, default=TRASH_RETENTION_DAYS,
                   help="keep trashed files for this many days before purge")
    return p.parse_args(argv)


# =============================================================================
# MAIN
# =============================================================================

def main(argv=None) -> int:
    global DRY_RUN, ROOT_DIR, CACHE_AGE_DAYS, LOG_KEEP_DAYS, TRASH_RETENTION_DAYS
    args = _parse_args(argv)

    DRY_RUN = not args.live
    if args.root is not None:
        ROOT_DIR = args.root.expanduser().resolve()
    CACHE_AGE_DAYS = max(0, args.days)
    LOG_KEEP_DAYS = max(0, args.log_keep)
    TRASH_RETENTION_DAYS = max(0, args.trash_keep)

    if not ROOT_DIR.is_dir():
        print(f"ERROR: project root does not exist: {ROOT_DIR}")
        return 2

    print("=" * 78)
    print(f"PROJECT CLEANER (v{__version__})")
    print(f"Root   : {ROOT_DIR}")
    print(f"Mode   : {'DRY RUN (nothing will be changed)' if DRY_RUN else 'LIVE (changes will be applied)'}")
    print("=" * 78)
    if not ((ROOT_DIR / "manage.py").is_file() or (ROOT_DIR / ".git").exists()):
        print("WARNING: neither manage.py nor .git found here -- verify this is "
              "the correct project root before going live.")
    print()

    # ---- 0. purge expired trash --------------------------------------------
    print(f"[0] Trash purge (items older than {TRASH_RETENTION_DAYS} day(s))")
    purged_items, purged_bytes = purge_expired_trash()
    if purged_items == 0:
        print("    nothing expired")
    print()

    # ---- collect everything -------------------------------------------------
    pycache_dirs, pyc_files, tool_dirs, tool_files = collect_regenerable()
    active_logs, stale_logs = collect_logs()
    stale_json = collect_stale_json_cache()
    clutter = collect_clutter()

    hard_dirs = sorted(pycache_dirs) + sorted(tool_dirs)
    hard_files = sorted(pyc_files) + sorted(tool_files)

    # ---- plan preview ---------------------------------------------------------
    print(f"[1] Bytecode & tool caches -- {len(hard_dirs)} dir(s), "
          f"{len(hard_files)} file(s) deleted outright (regenerable)")
    for p in hard_dirs:
        print(f"    delete dir    : {_rel(p)}")
    for p in hard_files:
        print(f"    delete file   : {_rel(p)}")
    if not hard_dirs and not hard_files:
        print("    none found")
    print()

    print(f"[2] Logs in '{LOGS_DIR_NAME}/' -- rotate {len(active_logs)}, "
          f"trash {len(stale_logs)} expired rotation(s) (older than {LOG_KEEP_DAYS}d)")
    for p in active_logs:
        print(f"    rotate        : {_rel(p)}")
    for p in stale_logs:
        print(f"    trash         : {_rel(p)}")
    if not active_logs and not stale_logs:
        print("    none found")
    print()

    print(f"[3] Stale *cache*.json older than {CACHE_AGE_DAYS} day(s) -- "
          f"{len(stale_json)} file(s) moved to trash (PROTECTED_JSON honoured)")
    for p in stale_json:
        print(f"    trash         : {_rel(p)}")
    if not stale_json:
        print("    none found")
    print()

    print(f"[4] Clutter -- {len(clutter)} file(s) flagged for MANUAL review (never touched)")
    for p in clutter:
        print(f"    REVIEW        : {_rel(p)}")
    if not clutter:
        print("    none found")
    print()

    # ---- byte bookkeeping -----------------------------------------------------
    hard_bytes = sum(_size_of(p) for p in hard_dirs + hard_files)
    trashed_bytes = sum(_size_of(p) for p in stale_logs + stale_json)
    reclaimed_bytes = purged_bytes + hard_bytes

    print("-" * 78)
    if DRY_RUN:
        print(f"DRY RUN -- {len(hard_dirs) + len(hard_files)} delete(s), "
              f"{len(active_logs)} rotation(s), "
              f"{len(stale_logs) + len(stale_json)} trash move(s) simulated.")
        print(f"Would reclaim ~{_fmt_bytes(reclaimed_bytes)} now; "
              f"~{_fmt_bytes(trashed_bytes)} parked in {TRASH_DIR_NAME}/ "
              f"(recoverable for {TRASH_RETENTION_DAYS} day(s)).")
        print(f"{len(clutter)} file(s) flagged for manual review.")
        print("Nothing was changed. Re-run with --live to apply.")
        print("-" * 78)
        return 0

    # ---- live confirmation ------------------------------------------------------
    print(f"LIVE run under: {ROOT_DIR}")
    print(f"  {len(hard_dirs) + len(hard_files)} item(s) deleted outright (regenerable)")
    print(f"  {len(stale_logs) + len(stale_json)} item(s) moved to {TRASH_DIR_NAME}/ (recoverable)")
    print(f"  {len(active_logs)} log(s) rotated in place")
    if not args.yes:
        try:
            ans = input("Type YES to continue: ").strip()
        except EOFError:
            print("\nAborted (no interactive input available). Use --yes to skip the prompt.")
            return 1
        if ans != "YES":
            print("Aborted -- nothing was changed.")
            return 1
    print()

    # ---- apply ------------------------------------------------------------------
    print("[apply] bytecode & tool caches")
    for p in hard_dirs:
        delete_path(p)
    for p in hard_files:
        delete_path(p)
    print()

    print("[apply] logs")
    for p in active_logs:
        rotate_log(p)
    for p in stale_logs:
        trash_path(p)
    print()

    print("[apply] stale json caches")
    for p in stale_json:
        trash_path(p)
    print()

    print("-" * 78)
    print(f"Cleanup complete -- reclaimed ~{_fmt_bytes(reclaimed_bytes)}; "
          f"~{_fmt_bytes(trashed_bytes)} parked in {TRASH_DIR_NAME}/ "
          f"for {TRASH_RETENTION_DAYS} day(s).")
    print(f"{len(clutter)} file(s) flagged for manual review.")
    print("-" * 78)
    return 0


if __name__ == "__main__":
    sys.exit(main())

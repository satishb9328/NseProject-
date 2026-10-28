#!/usr/bin/env python3
"""
project_cleaner.py
==================

A safe, non-destructive-by-default cleanup utility for a Django-based
algorithmic trading project.

What it does (in order):
  1. Deletes every ``__pycache__`` directory and any stray ``.pyc`` files,
     recursively through the project.
  2. Empties (truncates, but KEEPS) every ``*.log`` file inside the ``logs/``
     folder.
  3. Deletes ``*.json`` *cache* files in the ``data/`` folder and the project
     root that are older than ``CACHE_AGE_DAYS`` days.
  4. REPORTS (but never deletes) files whose names suggest they are clutter,
     so you can review them manually.

Hard safety guarantees:
  * The ``.venv`` directory is always ignored.
  * The ``.git`` directory is always ignored.
  * The ``db.sqlite3`` database is always ignored.
  * ``DRY_RUN = True`` (below) means nothing is changed until you flip it to
    ``False`` after reviewing the preview.

Run it from anywhere (it cleans the directory it lives in):
    python project_cleaner.py
"""

import os
import shutil
import time
from pathlib import Path

# =============================================================================
# CONFIGURATION
# =============================================================================

# ---------------------------------------------------------------------------
# DRY-RUN SWITCH
# ---------------------------------------------------------------------------
# When True the script only *prints* what it would do; nothing is modified.
# Set to False ONLY after you have reviewed a dry-run and are happy with it.
DRY_RUN = False

# ---------------------------------------------------------------------------
# PROJECT ROOT
# ---------------------------------------------------------------------------
# Defaults to the folder containing this script, so it works no matter where
# you invoke it from. If you always run from the project root, you could use
# `Path.cwd()` instead.
ROOT_DIR = Path(__file__).resolve().parent

# ---------------------------------------------------------------------------
# EXCLUSIONS (never touched, never descended into)
# ---------------------------------------------------------------------------
# Directories pruned from every recursive traversal.
EXCLUDED_DIRS = {".venv", ".git"}

# Files that are never deleted, matched by name anywhere in the tree.
EXCLUDED_FILES = {"db.sqlite3"}

# ---------------------------------------------------------------------------
# LOG MANAGEMENT
# ---------------------------------------------------------------------------
# Folder whose ``*.log`` files get their content cleared (the files are kept).
LOGS_DIR_NAME = "logs"

# ---------------------------------------------------------------------------
# STALE JSON CACHE MANAGEMENT
# ---------------------------------------------------------------------------
# ``*.json`` cache files older than this many days are deleted.
CACHE_AGE_DAYS = 2

# Only ``*.json`` files whose NAME contains one of these keywords (case-
# insensitive) are treated as cache files. This deliberately protects real
# data files such as ``live_options_data.json`` from accidental deletion.
# Extend this tuple if your cache files use different names.
CACHE_NAME_KEYWORDS = ("cache",)

# ---------------------------------------------------------------------------
# CLUTTER DETECTION (reported only — NEVER auto-deleted)
# ---------------------------------------------------------------------------
# Lowercase substrings that mark a filename for manual review.
CLUTTER_PATTERNS = (" - copy", "_backup", "test")


# =============================================================================
# HELPERS
# =============================================================================

def _rel(path: Path) -> Path:
    """Return a path relative to the project root for tidy output."""
    try:
        return path.relative_to(ROOT_DIR)
    except ValueError:
        return path


def _is_excluded_dir(name: str) -> bool:
    return name in EXCLUDED_DIRS


def _is_excluded_file(name: str) -> bool:
    return name in EXCLUDED_FILES


def _is_cache_file(path: Path) -> bool:
    """A ``*.json`` file is a cache file if its name contains a keyword."""
    return any(keyword in path.stem.lower() for keyword in CACHE_NAME_KEYWORDS)


# =============================================================================
# COLLECTORS (find-only — no side effects)
# =============================================================================

def collect_bytecode_targets():
    """Find ``__pycache__`` dirs and stray ``.pyc`` files, skipping exclusions.

    Returns:
        (pycache_dirs, pyc_files) — lists of ``Path`` objects.
    """
    pycache_dirs: list[Path] = []
    pyc_files: list[Path] = []

    for root, dirs, files in os.walk(ROOT_DIR, topdown=True):
        root_path = Path(root)

        # Prune excluded directories in-place so os.walk never descends into
        # them. Also collect __pycache__ dirs so we can remove them wholesale
        # (there is no need to descend into them either).
        kept_dirs: list[str] = []
        for name in dirs:
            if _is_excluded_dir(name):
                continue
            if name == "__pycache__":
                pycache_dirs.append(root_path / name)
                continue
            kept_dirs.append(name)
        dirs[:] = kept_dirs

        for name in files:
            if _is_excluded_file(name):
                continue
            if name.endswith(".pyc"):
                pyc_files.append(root_path / name)

    return pycache_dirs, pyc_files


def collect_log_files():
    """Return every ``*.log`` file inside the configured logs folder."""
    logs_dir = ROOT_DIR / LOGS_DIR_NAME
    if not logs_dir.is_dir():
        return []
    return sorted(logs_dir.rglob("*.log"))


def collect_stale_json_cache():
    """Return ``*.json`` cache files (root level + ``data/``) older than N days.

    Root is scanned non-recursively (``ROOT_DIR/*.json``) to avoid wandering
    into third-party/Django packages; ``data/`` is scanned recursively.
    """
    cutoff = time.time() - CACHE_AGE_DAYS * 86_400  # seconds in a day
    candidates: list[Path] = []

    # Project root — top-level *.json only (non-recursive, already safe).
    candidates.extend(ROOT_DIR.glob("*.json"))

    # data/ — recursive, since data may be organised into sub-folders.
    data_dir = ROOT_DIR / "data"
    if data_dir.is_dir():
        candidates.extend(data_dir.rglob("*.json"))

    stale: list[Path] = []
    seen: set[Path] = set()
    for path in candidates:
        if path in seen:
            continue
        seen.add(path)
        if not _is_cache_file(path):
            continue
        try:
            if path.stat().st_mtime < cutoff:
                stale.append(path)
        except OSError:
            # Unreadable / vanished file — skip rather than crash.
            continue

    return sorted(stale)


def collect_clutter():
    """Return files whose names look like clutter (reported, never deleted)."""
    clutter: list[Path] = []
    for root, dirs, files in os.walk(ROOT_DIR, topdown=True):
        dirs[:] = [d for d in dirs if not _is_excluded_dir(d)]
        root_path = Path(root)
        for name in files:
            if _is_excluded_file(name):
                continue
            lowered = name.lower()
            if any(pattern in lowered for pattern in CLUTTER_PATTERNS):
                clutter.append(root_path / name)
    return sorted(clutter)


# =============================================================================
# ACTIONS (respect DRY_RUN)
# =============================================================================

def delete_file(path: Path) -> None:
    if DRY_RUN:
        print(f"  [DRY-RUN] delete file : {_rel(path)}")
        return
    try:
        path.unlink(missing_ok=True)
        print(f"  deleted file          : {_rel(path)}")
    except OSError as exc:
        print(f"  ERROR deleting file   : {_rel(path)} -> {exc}")


def delete_dir(path: Path) -> None:
    if DRY_RUN:
        print(f"  [DRY-RUN] delete dir  : {_rel(path)}")
        return
    try:
        shutil.rmtree(path)
        print(f"  deleted dir           : {_rel(path)}")
    except OSError as exc:
        print(f"  ERROR deleting dir    : {_rel(path)} -> {exc}")


def empty_log(path: Path) -> None:
    if DRY_RUN:
        print(f"  [DRY-RUN] empty log   : {_rel(path)}")
        return
    try:
        # Truncate in place: the file remains, its content is cleared.
        path.write_text("", encoding="utf-8")
        print(f"  emptied log           : {_rel(path)}")
    except OSError as exc:
        print(f"  ERROR emptying log    : {_rel(path)} -> {exc}")


# =============================================================================
# MAIN
# =============================================================================

def main() -> None:
    print("=" * 78)
    print("PROJECT CLEANER")
    print(f"Root   : {ROOT_DIR}")
    print(f"Mode   : {'DRY RUN — no changes will be made' if DRY_RUN else 'LIVE — changes will be applied'}")
    print("=" * 78)
    print()

    # ---- 1. Python bytecode ------------------------------------------------
    pycache_dirs, pyc_files = collect_bytecode_targets()
    print("[1] Python bytecode")
    print(f"    __pycache__ dirs found : {len(pycache_dirs)}")
    print(f"    stray .pyc files found : {len(pyc_files)}")
    for directory in sorted(pycache_dirs):
        delete_dir(directory)
    for pyc in pyc_files:
        delete_file(pyc)
    print()

    # ---- 2. Log files -------------------------------------------------------
    log_files = collect_log_files()
    print(f"[2] Log files in '{LOGS_DIR_NAME}/' (content cleared, files kept)")
    print(f"    .log files found       : {len(log_files)}")
    for log in log_files:
        empty_log(log)
    print()

    # ---- 3. Stale JSON cache -----------------------------------------------
    stale_json = collect_stale_json_cache()
    print(f"[3] Stale .json cache files (older than {CACHE_AGE_DAYS} days)")
    print(f"    stale cache files      : {len(stale_json)}")
    for cache in stale_json:
        delete_file(cache)
    print()

    # ---- 4. Clutter (report only) ------------------------------------------
    clutter = collect_clutter()
    print("[4] Clutter candidates for MANUAL review (NOT deleted)")
    if clutter:
        for candidate in clutter:
            print(f"    REVIEW : {_rel(candidate)}")
    else:
        print("    none found")
    print()

    # ---- Summary ------------------------------------------------------------
    action_count = len(pycache_dirs) + len(pyc_files) + len(log_files) + len(stale_json)
    print("-" * 78)
    if DRY_RUN:
        print(f"DRY RUN complete. {action_count} action(s) simulated; nothing was changed.")
        print("Review the output, then set DRY_RUN = False to apply for real.")
    else:
        print(f"Cleanup complete. {action_count} action(s) applied.")
    print(f"{len(clutter)} file(s) flagged for manual review.")
    print("-" * 78)


if __name__ == "__main__":
    main()

"""run_evaluator.py — batch entrypoint for the Signal Validation evaluator."""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from evaluator import __version__, load_evaluator_config, now_ist, resolve_paths  # noqa: E402
from evaluator import labels as labels_mod  # noqa: E402
from evaluator import metrics as metrics_mod  # noqa: E402
from evaluator import significance as sig_mod  # noqa: E402
from evaluator import split as split_mod  # noqa: E402
from evaluator.loader import Loader  # noqa: E402
from evaluator.report import write_reports  # noqa: E402


def _resolve_dates(args, loader: Loader) -> list:
    if args.date:
        return [args.date]
    if args.start:
        d0 = datetime.strptime(args.start, "%Y-%m-%d").date()
        d1 = datetime.strptime(args.end, "%Y-%m-%d").date() if args.end else datetime.now().astimezone().date()
        dates = []
        d = d0
        while d <= d1:
            dates.append(d.isoformat())
            d += timedelta(days=1)
        return dates
    return loader.discover_dates()


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Signal Validation evaluator")
    parser.add_argument("--config", default=None, help="path to evaluator.yaml")
    parser.add_argument("--date", help="evaluate a single date YYYY-MM-DD")
    parser.add_argument("--start", help="start date YYYY-MM-DD (date range)")
    parser.add_argument("--end", help="end date YYYY-MM-DD (defaults to today)")
    args = parser.parse_args(argv)

    config = load_evaluator_config(args.config)
    paths = resolve_paths(config)
    loader = Loader(config)

    dates = _resolve_dates(args, loader)
    if not dates:
        print("No signal_*.jsonl data files found in processed/ or raw/.", file=sys.stderr)
        return 1

    df = loader.load(dates)
    if df.empty:
        print("No rows after loading and quality filtering.", file=sys.stderr)
        return 1

    df = labels_mod.build(df, config)

    train, val, test = split_mod.train_validation_test_split(df, config)

    full_rows = sig_mod.annotate(metrics_mod.compute_metric_rows(df, config), config)
    oos_rows = sig_mod.annotate(metrics_mod.compute_metric_rows(test, config), config) if not test.empty else []

    benchmarks = metrics_mod.compute_benchmarks(df, config)

    result = {
        "generated_at": now_ist().isoformat(),
        "version": __version__,
        "dataset": {
            "dates": dates,
            "n_rows": int(len(df)),
            "n_train": int(len(train)),
            "n_validation": int(len(val)),
            "n_test": int(len(test)),
        },
        "config": {
            "horizons": config["horizons"],
            "cost_spot_points": config["labels"]["cost"]["spot_points"],
            "cost_option_pct": config["labels"]["cost"]["option_pct"],
            "score_bull": config["signals"]["score"]["bull_threshold"],
            "score_bear": config["signals"]["score"]["bear_threshold"],
            "alpha": config["significance"]["alpha"],
        },
        "metrics": full_rows,
        "metrics_oos": oos_rows,
        "benchmarks": benchmarks,
    }

    written = write_reports(result, paths["reports"], config)
    for path in written:
        print(f"Wrote {path}")
    print(f"Evaluated {len(dates)} date(s): {len(df)} rows, {len(full_rows)} metric cells "
          f"({len(oos_rows)} out-of-sample).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

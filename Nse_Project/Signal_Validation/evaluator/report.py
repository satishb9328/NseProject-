"""Report rendering (Markdown + JSON) and atomic writing."""

from __future__ import annotations

import json
import os
from datetime import datetime
from pathlib import Path

from . import IST


def _fmt(value, nd: int = 3) -> str:
    if isinstance(value, float):
        if value != value:
            return "nan"
        if value == float("inf"):
            return "inf"
        if value == float("-inf"):
            return "-inf"
        return f"{value:.{nd}f}"
    return str(value)


def render_markdown(result: dict) -> str:
    lines = [
        "# Signal Validation Report",
        "",
        f"Generated: {result.get('generated_at', '')}",
        "",
    ]
    ds = result.get("dataset", {})
    lines.append(f"- Rows loaded: {ds.get('n_rows', 0)}  (train {ds.get('n_train', 0)} / "
                 f"val {ds.get('n_validation', 0)} / test {ds.get('n_test', 0)})")
    lines.append(f"- Dates: {', '.join(ds.get('dates', []))}")
    lines.append("")

    def _table(rows, title):
        out = [f"## {title}", ""]
        if not rows:
            out.append("_No metric rows._")
            out.append("")
            return out
        header = ["Signal", "Bucket", "H", "Outcome", "N", "Hit%", "EdgeΔ", "Expect", "PF", "DD", "DDc", "Sharpe", "p(adj)", "Flag"]
        out.append("| " + " | ".join(header) + " |")
        out.append("|" + "---|" * len(header))
        for m in rows:
            hit = (m["hit_rate"] * 100) if isinstance(m["hit_rate"], (int, float)) else float("nan")
            edge = m.get("edge_delta")
            edge_s = "—" if edge is None or (isinstance(edge, float) and edge != edge) else _fmt(edge * 100, 1)
            flag = "Y" if m.get("significant") else ("LOW" if m.get("low_sample") else "")
            out.append(
                f"| {m['signal']} | {m['bucket']} | {m['horizon']} | {m['outcome']} | {m['n_calls']} | "
                f"{_fmt(hit, 1)} | {edge_s} | {_fmt(m['expectancy'], 3)} | {_fmt(m['profit_factor'], 2)} | "
                f"{_fmt(m['max_drawdown'], 1)} | {_fmt(m.get('max_drawdown_cooldown'), 1)} | "
                f"{_fmt(m['sharpe'], 2)} | {_fmt(m['p_binomial_adj'], 4)} | {flag} |"
            )
        out.append("")
        return out

    lines.extend(_table(result.get("metrics", []), "Full-sample metrics"))
    lines.extend(_table(result.get("metrics_oos", []), "Out-of-sample (test) metrics"))

    lines.append("")
    lines.append("_EdgeΔ = signal hit% − always-long hit% (percentage points; positive means the "
                 "signal beats the naive long baseline). DD = raw per-call drawdown; "
                 "DDc = non-overlapping (cooldown) equity-curve drawdown._")

    lines.append("## Benchmarks (spot, cost-adjusted)")
    lines.append("")
    bench = result.get("benchmarks", {})
    if bench:
        lines.append("| H | N | AlwaysLong% | AlwaysShort% | HoldMean(pts) |")
        lines.append("|---|---|---|---|---|")
        for h, b in bench.items():
            lines.append(
                f"| {h} | {b['n']} | {_fmt(b['always_long_hit_rate'] * 100, 1)} | "
                f"{_fmt(b['always_short_hit_rate'] * 100, 1)} | {_fmt(b['hold_spot_mean_pts'], 3)} |"
            )
    lines.append("")
    return "\n".join(lines)


def render_json(result: dict) -> str:
    return json.dumps(result, ensure_ascii=False, indent=2, default=str)


def write_reports(result: dict, reports_dir, config: dict) -> list:
    reports_dir = Path(reports_dir)
    os.makedirs(reports_dir, exist_ok=True)
    formats = config.get("report", {}).get("formats", ["json", "markdown"])
    versioned = config.get("report", {}).get("version_by_timestamp", True)
    stamp = datetime.now(IST).strftime("%Y%m%d_%H%M%S") if versioned else "latest"

    written = []
    if "markdown" in formats:
        path = reports_dir / f"report_{stamp}.md"
        _atomic_write(path, render_markdown(result))
        written.append(str(path))
    if "json" in formats:
        path = reports_dir / f"report_{stamp}.json"
        _atomic_write(path, render_json(result))
        written.append(str(path))
    return written


def _atomic_write(path: Path, text: str) -> None:
    tmp = path.with_name(path.name + ".tmp")
    with open(tmp, "w", encoding="utf-8") as fh:
        fh.write(text)
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, path)

#!/usr/bin/env python3
"""
Aggregate TPC-C concurrency sweep directories and emit CSV + PNG charts.

Expects:
  tpcc-out/concurrency_sweep/c{8,16,...}/tpcc.json
  tpcc-out/concurrency_sweep/c{8,...}/postgres_tpcc.json
  optional: validation.json per step

Usage:
  python3 scripts/tpcc_concurrency_plot.py tpcc-out/concurrency_sweep
"""
from __future__ import annotations

import argparse
import csv
import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

TXN_KINDS = ("new_order", "payment", "order_status", "delivery", "stock_level")
STEP_RE = re.compile(r"^c(\d+)$", re.IGNORECASE)


@dataclass
class StepMetrics:
    concurrency: int
    path: Path
    valid: bool | None = None
    rustdb_tps: float | None = None
    postgres_tps: float | None = None
    ratio_pct: float | None = None
    rustdb_p50_ms: float | None = None
    rustdb_p95_ms: float | None = None
    rustdb_p99_ms: float | None = None
    postgres_p50_ms: float | None = None
    per_kind_rustdb_p50: dict[str, float] = field(default_factory=dict)
    per_kind_postgres_p50: dict[str, float] = field(default_factory=dict)


def read_json(path: Path) -> dict[str, Any] | None:
    if not path.is_file():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None


def discover_steps(sweep_root: Path) -> list[StepMetrics]:
    steps: list[StepMetrics] = []
    for child in sorted(sweep_root.iterdir()):
        if not child.is_dir():
            continue
        m = STEP_RE.match(child.name)
        if not m:
            continue
        c = int(m.group(1))
        val = read_json(child / "validation.json")
        rd = read_json(child / "tpcc.json")
        pg = read_json(child / "postgres_tpcc.json")
        sm = StepMetrics(concurrency=c, path=child)
        if val:
            sm.valid = bool(val.get("valid"))
            mets = val.get("metrics", {})
            sm.rustdb_tps = _f(mets.get("rustdb_txns_per_s"))
            sm.postgres_tps = _f(mets.get("postgres_txns_per_s"))
            sm.ratio_pct = _f(mets.get("ratio_percent_rustdb_over_postgres"))
            rd_log = mets.get("rustdb_txn_log", {}).get("per_kind", {})
            pg_log = mets.get("postgres_txn_log", {}).get("per_kind", {})
            for kind in TXN_KINDS:
                if isinstance(rd_log.get(kind), dict):
                    sm.per_kind_rustdb_p50[kind] = _f(rd_log[kind].get("p50_ms")) or 0.0
                if isinstance(pg_log.get(kind), dict):
                    sm.per_kind_postgres_p50[kind] = _f(pg_log[kind].get("p50_ms")) or 0.0
        if rd:
            sm.rustdb_tps = sm.rustdb_tps or _f(rd.get("txns_per_s"))
            sm.rustdb_p50_ms = sm.rustdb_p50_ms or _f(rd.get("p50_ms"))
            sm.rustdb_p95_ms = sm.rustdb_p95_ms or _f(rd.get("p95_ms"))
            sm.rustdb_p99_ms = sm.rustdb_p99_ms or _f(rd.get("p99_ms"))
        if pg:
            sm.postgres_tps = sm.postgres_tps or _f(pg.get("txns_per_s"))
            sm.postgres_p50_ms = sm.postgres_p50_ms or _f(pg.get("p50_ms"))
        if sm.rustdb_tps and sm.postgres_tps and sm.postgres_tps > 0 and sm.ratio_pct is None:
            sm.ratio_pct = 100.0 * sm.rustdb_tps / sm.postgres_tps
        steps.append(sm)
    return sorted(steps, key=lambda s: s.concurrency)


def _f(v: Any) -> float | None:
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def saturation_concurrency(steps: list[StepMetrics], engine: str, plateau_frac: float = 0.98) -> int | None:
    pts = []
    for s in steps:
        tps = s.rustdb_tps if engine == "rustdb" else s.postgres_tps
        if tps is not None:
            pts.append((s.concurrency, tps))
    if not pts:
        return None
    peak = max(pts, key=lambda x: x[1])
    target = peak[1] * plateau_frac
    for c, tps in sorted(pts):
        if tps >= target:
            return c
    return None


def write_csv(steps: list[StepMetrics], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        header = [
            "concurrency",
            "valid",
            "rustdb_txns_per_s",
            "postgres_txns_per_s",
            "ratio_pct",
            "rustdb_p50_ms",
            "rustdb_p95_ms",
            "rustdb_p99_ms",
        ]
        for kind in TXN_KINDS:
            header.append(f"rustdb_{kind}_p50_ms")
            header.append(f"postgres_{kind}_p50_ms")
        w.writerow(header)
        for s in steps:
            row = [
                s.concurrency,
                s.valid if s.valid is not None else "",
                s.rustdb_tps if s.rustdb_tps is not None else "",
                s.postgres_tps if s.postgres_tps is not None else "",
                s.ratio_pct if s.ratio_pct is not None else "",
                s.rustdb_p50_ms if s.rustdb_p50_ms is not None else "",
                s.rustdb_p95_ms if s.rustdb_p95_ms is not None else "",
                s.rustdb_p99_ms if s.rustdb_p99_ms is not None else "",
            ]
            for kind in TXN_KINDS:
                row.append(s.per_kind_rustdb_p50.get(kind, ""))
                row.append(s.per_kind_postgres_p50.get(kind, ""))
            w.writerow(row)


def plot_all(steps: list[StepMetrics], plots_dir: Path, title_suffix: str) -> list[Path]:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    plots_dir.mkdir(parents=True, exist_ok=True)
    xs = [s.concurrency for s in steps]
    written: list[Path] = []

    # 1) Throughput
    fig, ax = plt.subplots(figsize=(8, 5))
    rd = [s.rustdb_tps for s in steps]
    pg = [s.postgres_tps for s in steps]
    ax.plot(xs, rd, marker="o", label="RustDB")
    ax.plot(xs, pg, marker="s", label="PostgreSQL")
    ax.set_xlabel("concurrency (workers)")
    ax.set_ylabel("transactions/s")
    ax.set_title(f"TPC-C throughput vs concurrency{title_suffix}")
    ax.grid(True, alpha=0.3)
    ax.legend()
    p = plots_dir / "throughput_vs_concurrency.png"
    fig.tight_layout()
    fig.savefig(p, dpi=120)
    plt.close(fig)
    written.append(p)

    # 2) Ratio
    fig, ax = plt.subplots(figsize=(8, 5))
    ratios = [s.ratio_pct for s in steps]
    ax.plot(xs, ratios, marker="o", color="tab:green")
    ax.axhline(100.0, linestyle="--", color="gray", alpha=0.6, label="parity (100%)")
    ax.axhline(105.0, linestyle=":", color="orange", alpha=0.7, label="claim gate (105%)")
    ax.set_xlabel("concurrency")
    ax.set_ylabel("RustDB / PostgreSQL (%)")
    ax.set_title(f"Throughput ratio vs concurrency{title_suffix}")
    ax.grid(True, alpha=0.3)
    ax.legend()
    p = plots_dir / "ratio_vs_concurrency.png"
    fig.tight_layout()
    fig.savefig(p, dpi=120)
    plt.close(fig)
    written.append(p)

    # 3) Per-kind p50 (RustDB) — focus new_order
    fig, axes = plt.subplots(2, 2, figsize=(11, 9))
    focus = [
        ("new_order", axes[0, 0]),
        ("payment", axes[0, 1]),
        ("order_status", axes[1, 0]),
        ("delivery", axes[1, 1]),
    ]
    for kind, ax in focus:
        ry = [s.per_kind_rustdb_p50.get(kind) for s in steps]
        py = [s.per_kind_postgres_p50.get(kind) for s in steps]
        if any(v is not None for v in ry):
            ax.plot(xs, ry, marker="o", label="RustDB")
        if any(v is not None for v in py):
            ax.plot(xs, py, marker="s", label="PostgreSQL")
        ax.set_title(f"{kind} p50 (ms)")
        ax.set_xlabel("concurrency")
        ax.set_ylabel("p50 ms")
        ax.grid(True, alpha=0.3)
        if ax.get_legend_handles_labels()[0]:
            ax.legend(fontsize=8)
    fig.suptitle(f"Per-transaction latency (p50){title_suffix}", fontsize=12)
    p = plots_dir / "per_kind_p50.png"
    fig.tight_layout()
    fig.savefig(p, dpi=120)
    plt.close(fig)
    written.append(p)

    # 4) new_order only — larger
    fig, ax = plt.subplots(figsize=(8, 5))
    ry = [s.per_kind_rustdb_p50.get("new_order") for s in steps]
    py = [s.per_kind_postgres_p50.get("new_order") for s in steps]
    if any(v is not None for v in ry):
        ax.plot(xs, ry, marker="o", linewidth=2, label="RustDB new_order")
    if any(v is not None for v in py):
        ax.plot(xs, py, marker="s", linewidth=2, label="PostgreSQL new_order")
    ax.axhline(110.0, linestyle=":", color="red", alpha=0.5, label="CI soft threshold 110ms")
    ax.set_xlabel("concurrency")
    ax.set_ylabel("p50 ms")
    ax.set_title(f"new_order latency (≈45% of mix){title_suffix}")
    ax.grid(True, alpha=0.3)
    ax.legend()
    p = plots_dir / "new_order_p50.png"
    fig.tight_layout()
    fig.savefig(p, dpi=120)
    plt.close(fig)
    written.append(p)

    # 5) Overall latency percentiles (RustDB)
    fig, ax = plt.subplots(figsize=(8, 5))
    for label, key, mk in (
        ("p50", "rustdb_p50_ms", "o"),
        ("p95", "rustdb_p95_ms", "^"),
        ("p99", "rustdb_p99_ms", "s"),
    ):
        ys = [getattr(s, key) for s in steps]
        if any(y is not None for y in ys):
            ax.plot(xs, ys, marker=mk, label=f"RustDB {label}")
    ax.set_xlabel("concurrency")
    ax.set_ylabel("latency (ms)")
    ax.set_title(f"RustDB overall latency vs concurrency{title_suffix}")
    ax.grid(True, alpha=0.3)
    ax.legend()
    p = plots_dir / "rustdb_overall_latency.png"
    fig.tight_layout()
    fig.savefig(p, dpi=120)
    plt.close(fig)
    written.append(p)

    return written


def write_markdown(
    steps: list[StepMetrics],
    sweep_root: Path,
    plots: list[Path],
    cfg: dict[str, Any] | None,
) -> Path:
    md = sweep_root / "sweep_report.md"
    sat_rd = saturation_concurrency(steps, "rustdb")
    sat_pg = saturation_concurrency(steps, "postgres")
    lines = [
        "# TPC-C concurrency sweep",
        "",
        f"Root: `{sweep_root}`",
        "",
    ]
    if cfg:
        lines.append(f"- Preset: **{cfg.get('preset', '?')}**")
        lines.append(f"- Duration per step: **{cfg.get('duration_secs', '?')} s**")
        lines.append(f"- Steps: **{cfg.get('concurrency_steps', [])}**")
        lines.append("")
    lines.extend(
        [
            "## Saturation (98% of peak TPS)",
            "",
            f"- RustDB knee ≈ concurrency **{sat_rd or '—'}**",
            f"- PostgreSQL knee ≈ concurrency **{sat_pg or '—'}**",
            "",
            "## Summary table",
            "",
            "| c | valid | RustDB TPS | PG TPS | ratio % | new_order p50 (RD) | new_order p50 (PG) |",
            "|---:|:---:|---:|---:|---:|---:|---:|",
        ]
    )
    for s in steps:
        no_rd = s.per_kind_rustdb_p50.get("new_order")
        no_pg = s.per_kind_postgres_p50.get("new_order")
        lines.append(
            f"| {s.concurrency} | {s.valid if s.valid is not None else '?'} | "
            f"{s.rustdb_tps or 0:.1f} | {s.postgres_tps or 0:.1f} | "
            f"{s.ratio_pct or 0:.1f} | "
            f"{no_rd or 0:.1f} | {no_pg or 0:.1f} |"
        )
    lines.append("")
    lines.append("## Charts")
    lines.append("")
    for p in plots:
        lines.append(f"- `{p.name}`")
    lines.append("")
    md.write_text("\n".join(lines), encoding="utf-8")
    return md


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "sweep_root",
        type=Path,
        nargs="?",
        default=Path("tpcc-out/concurrency_sweep"),
    )
    args = ap.parse_args()
    sweep_root = args.sweep_root.resolve()
    if not sweep_root.is_dir():
        print(f"missing sweep root: {sweep_root}")
        return 1

    steps = discover_steps(sweep_root)
    if not steps:
        print(f"no c<N> directories under {sweep_root}")
        return 1

    cfg = read_json(sweep_root / "sweep_config.json")
    title = ""
    if cfg and cfg.get("duration_secs"):
        title = f" ({cfg['duration_secs']}s/step)"

    write_csv(steps, sweep_root / "sweep.csv")
    plots = plot_all(steps, sweep_root / "plots", title)
    md = write_markdown(steps, sweep_root, plots, cfg)

    print(f"Wrote {sweep_root / 'sweep.csv'}")
    print(f"Wrote {md}")
    for p in plots:
        print(f"Wrote {p}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

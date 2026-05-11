#!/usr/bin/env python3
"""Consolidated RustDB tpcc + PostgreSQL pgbench comparison for CI."""

from __future__ import annotations

import html
import json
import pathlib
import shutil
import sys
from typing import Any


def resolve_under(base: pathlib.Path, name: str) -> pathlib.Path:
    """Artifact layout may be flat or include a `tpcc-out/` directory."""
    for candidate in (base / name, base / "tpcc-out" / name):
        if candidate.is_file():
            return candidate
    return base / name


def read_json(path: pathlib.Path) -> dict[str, Any] | None:
    if not path.is_file():
        return None
    try:
        raw = path.read_text(encoding="utf-8").strip()
        if not raw:
            return None
        return json.loads(raw)
    except json.JSONDecodeError:
        return None


def num(d: dict[str, Any] | None, *keys: str, default: float = 0.0) -> float:
    if not d:
        return default
    cur: Any = d
    for k in keys:
        if not isinstance(cur, dict) or k not in cur:
            return default
        cur = cur[k]
    try:
        return float(cur)
    except (TypeError, ValueError):
        return default


def main() -> int:
    if len(sys.argv) != 4:
        print(
            "usage: bench_report_ci.py <rustdb_artifact_dir> <postgres_artifact_dir> <out_dir>",
            file=sys.stderr,
        )
        return 2

    rust_dir = pathlib.Path(sys.argv[1])
    pg_dir = pathlib.Path(sys.argv[2])
    out_dir = pathlib.Path(sys.argv[3])
    raw_root = out_dir / "raw_inputs"
    raw_root.mkdir(parents=True, exist_ok=True)

    # Preserve inputs for auditing
    if rust_dir.is_dir() and any(rust_dir.iterdir()):
        shutil.copytree(rust_dir, raw_root / "bench-rustdb-tpcc-result", dirs_exist_ok=True)
    if pg_dir.is_dir() and any(pg_dir.iterdir()):
        shutil.copytree(pg_dir, raw_root / "bench-postgres-pgbench-result", dirs_exist_ok=True)

    tpcc_path = resolve_under(rust_dir, "tpcc.json")
    pgb_path = resolve_under(pg_dir, "pgbench.json")
    tpcc = read_json(tpcc_path)
    pgb = read_json(pgb_path)

    rust_missing = tpcc is None
    pg_missing = pgb is None

    rd_tps = num(tpcc, "txns_per_s") if tpcc else 0.0
    pg_tps = num(pgb, "tps") if pgb else 0.0

    rd_lat = tpcc.get("overall_latency_ms", {}) if tpcc else {}
    rd_p50 = num(rd_lat, "p50") if isinstance(rd_lat, dict) else 0.0
    rd_p95 = num(rd_lat, "p95") if isinstance(rd_lat, dict) else 0.0
    rd_p99 = num(rd_lat, "p99") if isinstance(rd_lat, dict) else 0.0

    pg_lat_ms = num(pgb, "latency_avg_ms") if pgb else 0.0

    ratio = (100.0 * rd_tps / pg_tps) if (pg_tps > 0 and rd_tps >= 0) else None

    lines: list[str] = []
    lines.append("## RustDB vs PostgreSQL (baseline)")
    lines.append("")
    lines.append("| Metric | RustDB (`rustdb_tpcc`) | PostgreSQL (`pgbench`) |")
    lines.append("| --- | --- | --- |")

    def cell_rust(s: str) -> str:
        return s if not rust_missing else "**(missing — job failed)**"

    def cell_pg(s: str) -> str:
        return s if not pg_missing else "**(missing — job failed)**"

    lines.append(
        f"| txns/s or tps | {cell_rust(f'{rd_tps:.2f}')} | {cell_pg(f'{pg_tps:.2f}')} |"
    )
    lines.append(
        f"| latency p50 (ms) | {cell_rust(f'{rd_p50:.3f}')} | {cell_pg('n/a (see avg)')} |"
    )
    lines.append(
        f"| latency p95 (ms) | {cell_rust(f'{rd_p95:.3f}')} | {cell_pg('n/a')} |"
    )
    lines.append(
        f"| latency p99 (ms) | {cell_rust(f'{rd_p99:.3f}')} | {cell_pg('n/a')} |"
    )
    lines.append(
        f"| pgbench latency avg (ms) | n/a | {cell_pg(f'{pg_lat_ms:.3f}')} |"
    )
    if ratio is not None:
        lines.append("")
        lines.append(f"- **rustdb_tpcc / pgbench tps**: **{ratio:.1f}%**")
    else:
        lines.append("")
        lines.append("- **rustdb_tpcc / pgbench tps**: *(unable to compute — missing side)*")

    lines.append("")
    lines.append("### RustDB (`tpcc.txt` excerpt)")
    lines.append("")
    lines.append("```")
    p_txt = resolve_under(rust_dir, "tpcc.txt")
    if p_txt.is_file() and p_txt.stat().st_size > 0:
        txt = p_txt.read_text(encoding="utf-8", errors="replace")
        lines.append(txt.rstrip())
    else:
        lines.append("(missing tpcc.txt)")
    lines.append("```")

    lines.append("")
    lines.append("### PostgreSQL (`pgbench.txt` excerpt)")
    lines.append("")
    lines.append("```")
    g_txt = resolve_under(pg_dir, "pgbench.txt")
    if g_txt.is_file() and g_txt.stat().st_size > 0:
        txt = g_txt.read_text(encoding="utf-8", errors="replace")
        lines.append(txt.rstrip())
    else:
        lines.append("(missing pgbench.txt)")
    lines.append("```")

    report_md = "\n".join(lines) + "\n"
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "report.md").write_text(report_md, encoding="utf-8")

    report_obj = {
        "rustdb": tpcc,
        "postgres_pgbench": pgb,
        "comparison": {
            "rustdb_txns_per_s": None if rust_missing else rd_tps,
            "postgres_tps": None if pg_missing else pg_tps,
            "ratio_percent_rustdb_over_postgres": ratio,
            "rustdb_overall_latency_ms": rd_lat if tpcc else None,
            "postgres_latency_avg_ms": None if pg_missing else pg_lat_ms,
        },
        "artifacts_missing": {
            "rustdb": rust_missing,
            "postgres": pg_missing,
        },
    }
    (out_dir / "report.json").write_text(
        json.dumps(report_obj, indent=2) + "\n", encoding="utf-8"
    )

    # Minimal static HTML
    safe_md = html.escape(report_md)
    html_body = f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8"><title>Benchmark report</title></head>
<body><pre style="white-space:pre-wrap;font-family:system-ui,Segoe UI,sans-serif">{safe_md}</pre></body></html>
"""
    (out_dir / "report.html").write_text(html_body, encoding="utf-8")

    print(report_md)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

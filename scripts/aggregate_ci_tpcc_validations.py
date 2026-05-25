#!/usr/bin/env python3
"""
Aggregate multiple CI/local TPC-C validation.json files (median TPS + per-kind p50).

Use when comparing single CI runs or downloaded artifacts instead of fair_compare/run-N.

Usage:
  python3 scripts/aggregate_ci_tpcc_validations.py \\
    tpcc-ci-26165808773/validation.json \\
    tpcc-ci-26394263210/validation.json

  python3 scripts/aggregate_ci_tpcc_validations.py tpcc-ci-*/
"""
from __future__ import annotations

import argparse
import json
import statistics
import sys
from pathlib import Path
from typing import Any

TXN_KINDS = ("new_order", "payment", "order_status", "delivery", "stock_level")


def read_json(path: Path) -> dict[str, Any] | None:
    if not path.is_file():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None


def median(xs: list[float]) -> float | None:
    if not xs:
        return None
    return float(statistics.median(xs))


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "paths",
        nargs="+",
        type=Path,
        help="validation.json files or directories containing validation.json",
    )
    ap.add_argument("-o", "--output", type=Path, help="Write report JSON path")
    args = ap.parse_args()

    val_paths: list[Path] = []
    for p in args.paths:
        p = p.resolve()
        if p.is_dir():
            cand = p / "validation.json"
            if cand.is_file():
                val_paths.append(cand)
        elif p.is_file():
            val_paths.append(p)

    if not val_paths:
        print("No validation.json found", file=sys.stderr)
        return 1

    entries: list[dict[str, Any]] = []
    rustdb_tps: list[float] = []
    pg_tps: list[float] = []
    ratios: list[float] = []
    new_order_p50: list[float] = []

    for vp in val_paths:
        val = read_json(vp)
        label = vp.parent.name
        entry: dict[str, Any] = {"label": label, "path": str(vp), "valid": bool(val and val.get("valid"))}
        if not val:
            entries.append(entry)
            continue
        m = val.get("metrics", {})
        rd = m.get("rustdb_txns_per_s")
        pg = m.get("postgres_txns_per_s")
        ratio = m.get("ratio_percent_rustdb_over_postgres")
        if rd is not None:
            rustdb_tps.append(float(rd))
            entry["rustdb_txns_per_s"] = float(rd)
        if pg is not None:
            pg_tps.append(float(pg))
            entry["postgres_txns_per_s"] = float(pg)
        if ratio is not None:
            ratios.append(float(ratio))
            entry["ratio_percent"] = float(ratio)
        no = m.get("rustdb_txn_log", {}).get("per_kind", {}).get("new_order", {})
        if isinstance(no, dict) and "p50_ms" in no:
            p50 = float(no["p50_ms"])
            new_order_p50.append(p50)
            entry["new_order_p50_ms"] = p50
        per_kind: dict[str, Any] = {}
        for kind in TXN_KINDS:
            stats = m.get("rustdb_txn_log", {}).get("per_kind", {}).get(kind)
            if isinstance(stats, dict) and "p50_ms" in stats:
                per_kind[kind] = float(stats["p50_ms"])
        if per_kind:
            entry["rustdb_per_kind_p50_ms"] = per_kind
        entries.append(entry)

    report = {
        "samples": len(entries),
        "valid_samples": sum(1 for e in entries if e.get("valid")),
        "rustdb_txns_per_s_median": median(rustdb_tps),
        "postgres_txns_per_s_median": median(pg_tps),
        "ratio_median_pct": median(ratios),
        "new_order_p50_ms_median": median(new_order_p50),
        "entries": entries,
    }

    text = json.dumps(report, indent=2)
    if args.output:
        args.output.write_text(text + "\n", encoding="utf-8")
    print(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""
Aggregate N fair TPC-C compare iterations into report.json + report.md.

Expects:
  tpcc-out/fair_compare/run-{1..N}/{bench,strict}/validation.json

Usage:
  python3 scripts/aggregate_fair_tpcc.py tpcc-out/fair_compare
  python3 scripts/aggregate_fair_tpcc.py tpcc-out/fair_compare --runs 3
"""
from __future__ import annotations

import argparse
import json
import statistics
from pathlib import Path
from typing import Any

CLAIM_RATIO_MIN = 105.0
CLAIM_VALID_RUNS_MIN = 2

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


def percentile(xs: list[float], q: float) -> float | None:
    if not xs:
        return None
    s = sorted(xs)
    if len(s) == 1:
        return float(s[0])
    idx = (len(s) - 1) * q
    lo = int(idx)
    hi = min(lo + 1, len(s) - 1)
    frac = idx - lo
    return s[lo] * (1.0 - frac) + s[hi] * frac


def per_kind_values(
    val: dict[str, Any],
    log_key: str,
    field: str,
) -> dict[str, float]:
    """One sample per kind from a single validation.json."""
    out: dict[str, float] = {}
    per_kind = val.get("metrics", {}).get(log_key, {}).get("per_kind", {})
    if not isinstance(per_kind, dict):
        return out
    for kind in TXN_KINDS:
        stats = per_kind.get(kind)
        if isinstance(stats, dict) and field in stats:
            out[kind] = float(stats[field])
    return out


def aggregate_per_kind_medians(
    fair_root: Path,
    mode: str,
    run_ids: list[int],
) -> dict[str, Any]:
    rustdb_tps: list[float] = []
    pg_tps: list[float] = []
    rustdb_kind: dict[str, list[float]] = {k: [] for k in TXN_KINDS}
    pg_kind: dict[str, list[float]] = {k: [] for k in TXN_KINDS}

    for rid in run_ids:
        val = read_json(fair_root / f"run-{rid}" / mode / "validation.json")
        if not val or not val.get("valid"):
            continue
        m = val.get("metrics", {})
        if m.get("rustdb_txns_per_s") is not None:
            rustdb_tps.append(float(m["rustdb_txns_per_s"]))
        if m.get("postgres_txns_per_s") is not None:
            pg_tps.append(float(m["postgres_txns_per_s"]))
        rd = per_kind_values(val, "rustdb_txn_log", "p50_ms")
        pg = per_kind_values(val, "postgres_txn_log", "p50_ms")
        for kind in TXN_KINDS:
            if kind in rd:
                rustdb_kind[kind].append(rd[kind])
            if kind in pg:
                pg_kind[kind].append(pg[kind])

    def kind_block(kind_series: dict[str, list[float]]) -> dict[str, Any]:
        block: dict[str, Any] = {}
        for kind in TXN_KINDS:
            med = median(kind_series[kind])
            if med is not None:
                block[kind] = {"p50_ms_median": med, "samples": len(kind_series[kind])}
        return block

    return {
        "rustdb_txns_per_s_median": median(rustdb_tps),
        "postgres_txns_per_s_median": median(pg_tps),
        "ratio_median_pct": median(
            [
                100.0 * r / p
                for r, p in zip(rustdb_tps, pg_tps, strict=False)
                if p > 0
            ]
        ),
        "rustdb_per_kind": kind_block(rustdb_kind),
        "postgres_per_kind": kind_block(pg_kind),
        "focus": {
            "new_order": {
                "rustdb_p50_ms_median": median(rustdb_kind["new_order"]),
                "postgres_p50_ms_median": median(pg_kind["new_order"]),
                "note": "~45% of mix; primary throughput lever for RustDB",
            }
        },
    }


def aggregate_mode(
    fair_root: Path,
    mode: str,
    run_ids: list[int],
) -> dict[str, Any]:
    ratios: list[float] = []
    valid_runs = 0
    per_run: list[dict[str, Any]] = []

    for rid in run_ids:
        run_dir = fair_root / f"run-{rid}" / mode
        val_path = run_dir / "validation.json"
        val = read_json(val_path)
        entry: dict[str, Any] = {"run_id": rid, "path": str(run_dir), "validation": val}
        if val and val.get("valid"):
            valid_runs += 1
            ratio = val.get("metrics", {}).get("ratio_percent_rustdb_over_postgres")
            if ratio is not None:
                ratios.append(float(ratio))
                entry["ratio_percent"] = float(ratio)
            rd_tps = val.get("metrics", {}).get("rustdb_txns_per_s")
            if rd_tps is not None:
                entry["rustdb_txns_per_s"] = float(rd_tps)
            pk = val.get("metrics", {}).get("rustdb_txn_log", {}).get("per_kind", {})
            if isinstance(pk, dict) and "new_order" in pk:
                entry["new_order_p50_ms"] = pk["new_order"].get("p50_ms")
        per_run.append(entry)

    ratio_median = median(ratios)
    claim = (
        ratio_median is not None
        and ratio_median > CLAIM_RATIO_MIN
        and valid_runs >= CLAIM_VALID_RUNS_MIN
    )

    return {
        "valid_runs": valid_runs,
        "runs_attempted": len(run_ids),
        "ratio_median_pct": ratio_median,
        "ratio_p25_pct": percentile(ratios, 0.25),
        "ratio_p75_pct": percentile(ratios, 0.75),
        "ratios_pct": ratios,
        "claim_faster_than_pg": claim,
        "per_run": per_run,
        "per_kind_medians": aggregate_per_kind_medians(fair_root, mode, run_ids),
    }


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "fair_root",
        type=Path,
        nargs="?",
        default=Path("tpcc-out/fair_compare"),
    )
    ap.add_argument("--runs", type=int, default=3, help="Number of run-{i} directories")
    args = ap.parse_args()
    fair_root = args.fair_root.resolve()
    run_ids = list(range(1, args.runs + 1))

    bench = aggregate_mode(fair_root, "bench", run_ids)
    strict = aggregate_mode(fair_root, "strict", run_ids)

    report: dict[str, Any] = {
        "runs": args.runs,
        "fair_root": str(fair_root),
        "bench": bench,
        "strict": strict,
        "interpretation": {
            "bench_win": (
                "harness defer commit; not production-comparable to PostgreSQL durability"
            ),
            "strict_win": (
                f"claim faster_than_pg only if median ratio > {CLAIM_RATIO_MIN}% "
                f"on >= {CLAIM_VALID_RUNS_MIN} valid runs"
            ),
            "per_kind": (
                "Use per_kind_medians.new_order — not overall txns_per_s alone — "
                "when diagnosing regressions"
            ),
        },
    }

    fair_root.mkdir(parents=True, exist_ok=True)
    json_path = fair_root / "report.json"
    json_path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")

    lines = [
        "# Fair TPC-C compare report",
        "",
        f"Aggregated **{args.runs}** iteration(s) under `{fair_root}`.",
        "",
        "## Bench mode (defer heap flush on commit)",
        "",
        f"- Valid runs: **{bench['valid_runs']}** / {bench['runs_attempted']}",
    ]
    if bench["ratio_median_pct"] is not None:
        lines.append(f"- Median rustdb/postgres ratio: **{bench['ratio_median_pct']:.1f}%**")
        if bench["ratio_p25_pct"] is not None and bench["ratio_p75_pct"] is not None:
            lines.append(
                f"- IQR (p25–p75): {bench['ratio_p25_pct']:.1f}% – {bench['ratio_p75_pct']:.1f}%"
            )
    pk = bench.get("per_kind_medians", {})
    if pk.get("rustdb_txns_per_s_median") is not None:
        lines.append(
            f"- Median RustDB TPS: **{pk['rustdb_txns_per_s_median']:.1f}** "
            f"(PG **{pk.get('postgres_txns_per_s_median', 0):.1f}**)"
        )
    focus = pk.get("focus", {}).get("new_order", {})
    if focus.get("rustdb_p50_ms_median") is not None:
        lines.append(
            f"- **new_order p50 (median)**: RustDB **{focus['rustdb_p50_ms_median']:.1f} ms** "
            f"vs PG **{focus.get('postgres_p50_ms_median', 0):.1f} ms**"
        )
    lines.append(f"- **claim_faster_than_pg**: {bench['claim_faster_than_pg']}")
    lines.append("")
    lines.append("## Strict mode (sync heap flush on commit)")
    lines.append("")
    lines.append(f"- Valid runs: **{strict['valid_runs']}** / {strict['runs_attempted']}")
    if strict["ratio_median_pct"] is not None:
        lines.append(f"- Median rustdb/postgres ratio: **{strict['ratio_median_pct']:.1f}%**")
    lines.append(f"- **claim_faster_than_pg**: {strict['claim_faster_than_pg']}")
    lines.append("")
    lines.append("## Interpretation")
    lines.append("")
    lines.append(f"- {report['interpretation']['bench_win']}")
    lines.append(f"- {report['interpretation']['strict_win']}")
    lines.append(f"- {report['interpretation']['per_kind']}")
    lines.append("")

    md_path = fair_root / "report.md"
    md_path.write_text("\n".join(lines), encoding="utf-8")

    print(md_path.read_text(encoding="utf-8"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

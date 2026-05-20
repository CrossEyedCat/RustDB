#!/usr/bin/env python3
"""
Validate a paired TPC-C run directory (postgres_tpcc + rustdb_tpcc artifacts).

Usage:
  python3 scripts/validate_tpcc_run.py --mode bench tpcc-out
  python3 scripts/validate_tpcc_run.py --mode strict tpcc-out/fair_compare/run-1/strict

Writes <out_dir>/validation.json and exits 0 when valid, 1 when invalid.
"""
from __future__ import annotations

import argparse
import csv
import json
import re
import statistics
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any

# Gate thresholds (single source for CI + nightly)
GATES = {
    "pg_txns_per_s_min": 800.0,
    "pg_payment_p95_ms_max": 700.0,
    "success_rate_pct_min": 100.0,
    "err_max": 0,
    "new_order_share_min": 0.42,
    "new_order_share_max": 0.48,
    "bench_commit_flush_p50_us_max": 1000.0,
    "bench_heap_flush_skipped_pct_min": 90.0,
    "strict_commit_flush_p50_us_min": 1.0,
    "strict_heap_flush_skipped_pct_max": 10.0,
    "ratio_claim_min_pct": 105.0,
}


def quantile_ns(samples: list[int], q: float) -> float:
    if not samples:
        return 0.0
    s = sorted(samples)
    if len(s) == 1:
        return float(s[0])
    idx = (len(s) - 1) * q
    lo = int(idx)
    hi = min(lo + 1, len(s) - 1)
    frac = idx - lo
    return s[lo] * (1.0 - frac) + s[hi] * frac


def read_json(path: Path) -> dict[str, Any] | None:
    if not path.is_file():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None


def txn_kind_stats(log_path: Path, only_ok: bool = True) -> dict[str, Any]:
    """Per-kind latency (us) and counts; mirrors analyze_tpcc_txn_log.py."""
    if not log_path.is_file():
        return {"error": f"missing log: {log_path}"}

    text = log_path.read_text(encoding="utf-8", errors="replace")
    lines = [ln for ln in text.splitlines() if ln and not ln.startswith("#")]
    if not lines:
        return {"error": "empty log"}

    reader = csv.DictReader(lines)
    if not reader.fieldnames or "kind" not in reader.fieldnames:
        return {"error": f"unexpected header: {reader.fieldnames}"}

    by_kind: dict[str, list[int]] = defaultdict(list)
    kind_counts: dict[str, int] = defaultdict(int)
    total_ok = 0
    for row in reader:
        ok = (row.get("ok") or "").strip().lower()
        is_ok = ok in ("true", "1", "t", "yes")
        if only_ok and not is_ok:
            continue
        if only_ok:
            total_ok += 1
        kind = (row.get("kind") or "?").strip()
        kind_counts[kind] += 1
        try:
            us = int(row["elapsed_us"])
        except (KeyError, ValueError):
            continue
        by_kind[kind].append(us)

    per_kind: dict[str, dict[str, float]] = {}
    for kind, xs in by_kind.items():
        per_kind[kind] = {
            "n": len(xs),
            "p50_ms": quantile_ns(xs, 0.50) / 1000.0,
            "p95_ms": quantile_ns(xs, 0.95) / 1000.0,
            "p99_ms": quantile_ns(xs, 0.99) / 1000.0,
        }

    return {
        "total_ok": total_ok,
        "kind_counts": dict(kind_counts),
        "per_kind": per_kind,
    }


def new_order_share(kind_counts: dict[str, int], total: int) -> float | None:
    if total <= 0:
        return None
    return kind_counts.get("new_order", 0) / total


def parse_server_commit_metrics(server_log: Path) -> dict[str, Any]:
    """commit_flush_us p50 and heap_flush_skipped fraction from sql.commit lines."""
    if not server_log.is_file():
        return {"error": f"missing server log: {server_log}"}

    flush_us: list[int] = []
    skipped_flags: list[int] = []
    for line in server_log.read_text(encoding="utf-8", errors="replace").splitlines():
        if "sql.commit" not in line:
            continue
        m_flush = re.search(r"commit_flush_us=(\d+)", line)
        if m_flush:
            flush_us.append(int(m_flush.group(1)))
        m_skip = re.search(r"commit_heap_flush_skipped=(\d+)", line)
        if m_skip:
            skipped_flags.append(int(m_skip.group(1)))

    n = len(skipped_flags)
    skipped_pct = (
        100.0 * sum(1 for x in skipped_flags if x == 1) / n if n > 0 else None
    )
    return {
        "commit_flush_samples": len(flush_us),
        "commit_flush_p50_us": quantile_ns(flush_us, 0.50) if flush_us else None,
        "commit_flush_p50_ms": (quantile_ns(flush_us, 0.50) / 1000.0) if flush_us else None,
        "heap_flush_skipped_samples": n,
        "heap_flush_skipped_pct": skipped_pct,
    }


def num(d: dict[str, Any] | None, key: str, default: float = 0.0) -> float:
    if not d:
        return default
    try:
        return float(d.get(key, default))
    except (TypeError, ValueError):
        return default


def validate_run(out_dir: Path, mode: str) -> tuple[bool, dict[str, Any]]:
    reasons: list[str] = []
    gate_results: dict[str, bool] = {}

    pg_json = read_json(out_dir / "postgres_tpcc.json")
    rd_json = read_json(out_dir / "tpcc.json")
    pg_log = out_dir / "postgres_tpcc_txn.log"
    rd_log = out_dir / "tpcc_txn.log"
    server_log = out_dir / "server_full.log"

    pg_tps = num(pg_json, "txns_per_s")
    rd_tps = num(rd_json, "txns_per_s")
    ratio = (100.0 * rd_tps / pg_tps) if pg_tps > 0 else None

    pg_stats = txn_kind_stats(pg_log)
    rd_stats = txn_kind_stats(rd_log)
    pg_payment_p95 = None
    if "per_kind" in pg_stats and "payment" in pg_stats["per_kind"]:
        pg_payment_p95 = pg_stats["per_kind"]["payment"].get("p95_ms")

    pg_share = new_order_share(
        pg_stats.get("kind_counts", {}),
        pg_stats.get("total_ok", 0),
    )
    rd_share = new_order_share(
        rd_stats.get("kind_counts", {}),
        rd_stats.get("total_ok", 0),
    )

    server_metrics = parse_server_commit_metrics(server_log)

    def check(name: str, ok: bool, msg: str) -> None:
        gate_results[name] = ok
        if not ok:
            reasons.append(msg)

    check(
        "pg_txns_per_s",
        pg_tps >= GATES["pg_txns_per_s_min"],
        f"PG txns_per_s {pg_tps:.1f} < {GATES['pg_txns_per_s_min']}",
    )
    if pg_payment_p95 is not None:
        check(
            "pg_payment_p95_ms",
            pg_payment_p95 < GATES["pg_payment_p95_ms_max"],
            f"PG payment p95 {pg_payment_p95:.1f}ms >= {GATES['pg_payment_p95_ms_max']}ms",
        )
    else:
        check("pg_payment_p95_ms", False, "PG payment p95 unavailable (missing txn log)")

    for label, j in (("postgres", pg_json), ("rustdb", rd_json)):
        sr = num(j, "success_rate_pct")
        err = int(num(j, "err", default=0))
        check(
            f"{label}_success_rate",
            sr >= GATES["success_rate_pct_min"],
            f"{label} success_rate_pct {sr:.2f} < 100",
        )
        check(
            f"{label}_err",
            err <= GATES["err_max"],
            f"{label} err={err} (expected 0)",
        )

    for label, share in (("postgres", pg_share), ("rustdb", rd_share)):
        if share is None:
            check(f"{label}_new_order_share", False, f"{label} new_order share unavailable")
        else:
            ok = GATES["new_order_share_min"] <= share <= GATES["new_order_share_max"]
            check(
                f"{label}_new_order_share",
                ok,
                f"{label} new_order share {share:.3f} outside "
                f"[{GATES['new_order_share_min']}, {GATES['new_order_share_max']}]",
            )

    flush_p50_us = server_metrics.get("commit_flush_p50_us")
    skipped_pct = server_metrics.get("heap_flush_skipped_pct")

    if mode == "bench":
        flush_ok = (
            flush_p50_us is not None
            and flush_p50_us <= GATES["bench_commit_flush_p50_us_max"]
        )
        if flush_p50_us is None:
            check("bench_commit_flush_p50", False, "bench: commit_flush_us p50 unavailable")
        else:
            check(
                "bench_commit_flush_p50",
                flush_ok,
                f"bench: commit_flush_us p50 {flush_p50_us:.0f}us > "
                f"{GATES['bench_commit_flush_p50_us_max']}us",
            )
        if skipped_pct is not None:
            check(
                "bench_heap_flush_skipped",
                skipped_pct >= GATES["bench_heap_flush_skipped_pct_min"],
                f"bench: heap_flush_skipped {skipped_pct:.1f}% < "
                f"{GATES['bench_heap_flush_skipped_pct_min']}%",
            )
        elif flush_ok:
            gate_results["bench_heap_flush_skipped"] = True
        else:
            check(
                "bench_heap_flush_skipped",
                False,
                "bench: heap_flush_skipped stats unavailable and commit_flush not near-zero",
            )
    elif mode == "strict":
        strict_ok = False
        if flush_p50_us is not None and flush_p50_us >= GATES["strict_commit_flush_p50_us_min"]:
            strict_ok = True
        if skipped_pct is not None and skipped_pct <= GATES["strict_heap_flush_skipped_pct_max"]:
            strict_ok = True
        check(
            "strict_mode_consistency",
            strict_ok,
            "strict: expected commit_flush p50 > 0 or heap_flush_skipped < 10% "
            f"(got flush_p50_us={flush_p50_us}, skipped_pct={skipped_pct})",
        )
    else:
        reasons.append(f"unknown mode: {mode}")
        gate_results["mode"] = False

    valid = len(reasons) == 0
    claim_faster = (
        valid
        and ratio is not None
        and ratio > GATES["ratio_claim_min_pct"]
    )

    report: dict[str, Any] = {
        "valid": valid,
        "mode": mode,
        "out_dir": str(out_dir),
        "reasons": reasons,
        "gates": gate_results,
        "thresholds": GATES,
        "metrics": {
            "postgres_txns_per_s": pg_tps,
            "rustdb_txns_per_s": rd_tps,
            "ratio_percent_rustdb_over_postgres": ratio,
            "pg_payment_p95_ms": pg_payment_p95,
            "postgres_new_order_share": pg_share,
            "rustdb_new_order_share": rd_share,
            "postgres_txn_log": pg_stats if "error" not in pg_stats else pg_stats,
            "rustdb_txn_log": rd_stats if "error" not in rd_stats else rd_stats,
            "server_commit": server_metrics,
        },
        "claim_faster_than_pg": claim_faster,
    }
    return valid, report


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("out_dir", type=Path, help="Directory with tpcc.json, postgres_tpcc.json, logs")
    ap.add_argument("--mode", required=True, choices=("bench", "strict"))
    ap.add_argument(
        "--output",
        type=Path,
        default=None,
        help="validation.json path (default: <out_dir>/validation.json)",
    )
    args = ap.parse_args()
    out_dir = args.out_dir.resolve()
    if not out_dir.is_dir():
        print(f"not a directory: {out_dir}", file=sys.stderr)
        return 1

    valid, report = validate_run(out_dir, args.mode)
    out_path = args.output or (out_dir / "validation.json")
    out_path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")

    print(json.dumps(report, indent=2))
    if valid:
        print("valid: true")
    else:
        print("valid: false")
        for r in report["reasons"]:
            print(f"  - {r}", file=sys.stderr)
    return 0 if valid else 1


if __name__ == "__main__":
    raise SystemExit(main())

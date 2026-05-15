#!/usr/bin/env python3
"""
Summarize rustdb_tpcc / postgres_tpcc txn CSV (header: worker_id,global_attempt_id,kind,ok,elapsed_us,error).

Usage:
  python3 scripts/analyze_tpcc_txn_log.py path/to/tpcc_txn.log
  python3 scripts/analyze_tpcc_txn_log.py path/to/tpcc_txn.log --only-ok
"""
from __future__ import annotations

import argparse
import csv
import statistics
import sys
from collections import defaultdict
from pathlib import Path


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


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("path", type=Path, help="CSV txn log (with header line)")
    ap.add_argument(
        "--only-ok",
        action="store_true",
        help="Only rows where ok is true/1",
    )
    args = ap.parse_args()
    p: Path = args.path
    if not p.is_file():
        print(f"not a file: {p}", file=sys.stderr)
        return 1

    text = p.read_text(encoding="utf-8", errors="replace")
    lines = [ln for ln in text.splitlines() if ln and not ln.startswith("#")]
    if not lines:
        print("empty log")
        return 0

    r = csv.DictReader(lines)
    if not r.fieldnames or "kind" not in r.fieldnames or "elapsed_us" not in r.fieldnames:
        print(f"unexpected header: {r.fieldnames}", file=sys.stderr)
        return 1

    by_kind: dict[str, list[int]] = defaultdict(list)
    skipped = 0
    for row in r:
        if args.only_ok:
            ok = row.get("ok", "").strip().lower()
            if ok not in ("true", "1", "t", "yes"):
                skipped += 1
                continue
        try:
            us = int(row["elapsed_us"])
        except ValueError:
            skipped += 1
            continue
        kind = (row.get("kind") or "?").strip()
        by_kind[kind].append(us)

    print(f"file: {p}")
    if skipped:
        print(f"skipped_rows: {skipped}")
    print()
    hdr = f"{'kind':<14} {'n':>8} {'p50_ms':>10} {'p95_ms':>10} {'p99_ms':>10} {'mean_ms':>10}"
    print(hdr)
    print("-" * len(hdr))
    for kind in sorted(by_kind.keys(), key=lambda k: (-len(by_kind[k]), k)):
        xs = by_kind[kind]
        n = len(xs)
        p50 = quantile_ns(xs, 0.50) / 1000.0
        p95 = quantile_ns(xs, 0.95) / 1000.0
        p99 = quantile_ns(xs, 0.99) / 1000.0
        mean = (statistics.fmean(xs) if xs else 0.0) / 1000.0
        print(f"{kind:<14} {n:>8} {p50:>10.3f} {p95:>10.3f} {p99:>10.3f} {mean:>10.3f}")

    all_lat = [x for xs in by_kind.values() for x in xs]
    if all_lat:
        print("-" * len(hdr))
        p50 = quantile_ns(all_lat, 0.50) / 1000.0
        p95 = quantile_ns(all_lat, 0.95) / 1000.0
        p99 = quantile_ns(all_lat, 0.99) / 1000.0
        mean = statistics.fmean(all_lat) / 1000.0
        print(
            f"{'ALL':<14} {len(all_lat):>8} {p50:>10.3f} {p95:>10.3f} {p99:>10.3f} {mean:>10.3f}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

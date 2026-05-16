#!/usr/bin/env python3
"""
Aggregate rustdb::sql_phases / sql_parse lines from a server stderr log (tracing fmt layer).

Looks for:
  - message `sql_parse` and field parse_us=...
  - message `table_storage_lock` with lock_wait_us=...
  - message `update` with scan_us=..., row_loop_us=...
  - message `delete` with same fields
  - span `sql.execute_script` with wall_us=...

Usage:
  python3 scripts/summarize_sql_phase_log.py path/to/server.log
"""
from __future__ import annotations

import argparse
import re
import statistics
import sys
from pathlib import Path


def extract_parse_us(line: str) -> int | None:
    if "sql_parse" not in line:
        return None
    m = re.search(r"parse_us=(\d+)", line)
    if m:
        return int(m.group(1))
    return None


def extract_lock_wait_us(line: str) -> int | None:
    if "table_storage_lock" not in line:
        return None
    m = re.search(r"lock_wait_us=(\d+)", line)
    if m:
        return int(m.group(1))
    return None


def extract_execute_script_wall_us(line: str) -> int | None:
    if "sql.execute_script" not in line:
        return None
    m = re.search(r"wall_us=(\d+)", line)
    if m:
        return int(m.group(1))
    return None


def extract_update_pair(line: str) -> tuple[int, int] | None:
    if "scan_us=" not in line or "row_loop_us=" not in line:
        return None
    if not re.search(r"\bupdate\b|\bdelete\b", line, re.I):
        return None
    ms = re.search(r"scan_us=(\d+)", line)
    mr = re.search(r"row_loop_us=(\d+)", line)
    if ms and mr:
        return int(ms.group(1)), int(mr.group(1))
    return None


def quantile(xs: list[float], q: float) -> float:
    if not xs:
        return 0.0
    s = sorted(xs)
    idx = (len(s) - 1) * q
    lo = int(idx)
    hi = min(lo + 1, len(s) - 1)
    frac = idx - lo
    return s[lo] * (1.0 - frac) + s[hi] * frac


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("path", type=Path)
    args = ap.parse_args()
    p = args.path
    if not p.is_file():
        print(f"not a file: {p}", file=sys.stderr)
        return 1

    parse_us: list[float] = []
    lock_wait_us: list[float] = []
    execute_script_wall_us: list[float] = []
    scan_us: list[float] = []
    row_loop_us: list[float] = []
    phase_lines = 0

    for line in p.read_text(encoding="utf-8", errors="replace").splitlines():
        if "rustdb::sql_phases" in line:
            pu = extract_parse_us(line)
            if pu is not None:
                parse_us.append(float(pu))
                phase_lines += 1
            lw = extract_lock_wait_us(line)
            if lw is not None:
                lock_wait_us.append(float(lw))
                phase_lines += 1
            up = extract_update_pair(line)
            if up is not None:
                scan_us.append(float(up[0]))
                row_loop_us.append(float(up[1]))
                phase_lines += 1
        ew = extract_execute_script_wall_us(line)
        if ew is not None:
            execute_script_wall_us.append(float(ew))
            phase_lines += 1

    print(f"file: {p}")
    print(f"matched_lines: {phase_lines}")
    if parse_us:
        print(
            f"sql_parse parse_us: n={len(parse_us)} "
            f"p50={quantile(parse_us,0.5)/1000:.3f}ms p95={quantile(parse_us,0.95)/1000:.3f}ms "
            f"p99={quantile(parse_us,0.99)/1000:.3f}ms mean={statistics.fmean(parse_us)/1000:.3f}ms"
        )
    else:
        print("sql_parse: (no matches — set RUSTDB_SQL_PHASE_LOG=1 and RUST_LOG=info or rustdb::sql_phases=info)")
    if lock_wait_us:
        print(
            f"table_storage_lock lock_wait_us: n={len(lock_wait_us)} "
            f"p50={quantile(lock_wait_us,0.5)/1000:.3f}ms p95={quantile(lock_wait_us,0.95)/1000:.3f}ms "
            f"p99={quantile(lock_wait_us,0.99)/1000:.3f}ms mean={statistics.fmean(lock_wait_us)/1000:.3f}ms"
        )
    else:
        print("table_storage_lock: (no matches)")
    if execute_script_wall_us:
        print(
            f"sql.execute_script wall_us: n={len(execute_script_wall_us)} "
            f"p50={quantile(execute_script_wall_us,0.5)/1000:.3f}ms p95={quantile(execute_script_wall_us,0.95)/1000:.3f}ms "
            f"p99={quantile(execute_script_wall_us,0.99)/1000:.3f}ms mean={statistics.fmean(execute_script_wall_us)/1000:.3f}ms"
        )
    else:
        print("sql.execute_script: (no matches — requires RUST_LOG=info or tracing filter including sql.execute_script)")
    if scan_us:
        print(
            f"update/delete scan_us: n={len(scan_us)} "
            f"p50={quantile(scan_us,0.5)/1000:.3f}ms p95={quantile(scan_us,0.95)/1000:.3f}ms "
            f"p99={quantile(scan_us,0.99)/1000:.3f}ms"
        )
        print(
            f"update/delete row_loop_us: n={len(row_loop_us)} "
            f"p50={quantile(row_loop_us,0.5)/1000:.3f}ms p95={quantile(row_loop_us,0.95)/1000:.3f}ms "
            f"p99={quantile(row_loop_us,0.99)/1000:.3f}ms"
        )
    else:
        print("update/delete phase: (no matches — workload may not hit UPDATE/DELETE logs in sample)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""
Aggregate rustdb::sql_phases / sql_parse lines from a server stderr log (tracing fmt layer).

Looks for:
  - message `sql_parse` and field parse_us=...
  - message `table_storage_lock` / `row_storage_lock` with lock_wait_us=..., table=..., mode=...
  - message `sql.dml.lock_path` with lock_path=row|table|skip
  - message `sql.commit` with flush_us=..., wal_us=..., flush_tables_count=...
  - message `update` with scan_us=..., row_loop_us=...
  - message `delete` with same fields
  - message `sql.execute_script` with wall_us=...

Usage:
  python3 scripts/summarize_sql_phase_log.py path/to/server.log
  python3 scripts/summarize_sql_phase_log.py --warn-lock-p99-ms 50 path/to/server.log
"""
from __future__ import annotations

import argparse
import re
import statistics
import sys
from collections import defaultdict
from pathlib import Path


def extract_parse_us(line: str) -> int | None:
    if "sql_parse" not in line:
        return None
    m = re.search(r"parse_us=(\d+)", line)
    if m:
        return int(m.group(1))
    return None


def extract_storage_lock_wait(line: str, message: str) -> tuple[str, str, int] | None:
    if message not in line:
        return None
    m_wait = re.search(r"lock_wait_us=(\d+)", line)
    m_table = re.search(r"table=([^\s}]+)", line)
    m_mode = re.search(r'mode="?([\w_]+)"?', line)
    if m_wait and m_table and m_mode:
        return m_table.group(1), m_mode.group(1), int(m_wait.group(1))
    return None


def extract_lock_path(line: str) -> tuple[str, str] | None:
    if "rustdb::sql_phases" not in line or "sql.dml.lock_path" not in line:
        return None
    m_table = re.search(r"table=([^\s}]+)", line)
    m_path = re.search(r"lock_path=([^\s}]+)", line)
    if m_table and m_path:
        return m_table.group(1), m_path.group(1)
    return None


def extract_lock_wait(line: str) -> tuple[str, str, int] | None:
    return extract_storage_lock_wait(line, "table_storage_lock")


def extract_row_lock_wait(line: str) -> tuple[str, str, int] | None:
    return extract_storage_lock_wait(line, "row_storage_lock")


def extract_commit_metrics(line: str) -> tuple[int, int, int] | None:
    if "rustdb::sql_phases" not in line or "sql.commit" not in line:
        return None
    m_flush = re.search(r"flush_us=(\d+)", line)
    m_wal = re.search(r"wal_us=(\d+)", line)
    m_count = re.search(r"flush_tables_count=(\d+)", line)
    if m_flush and m_wal and m_count:
        return int(m_flush.group(1)), int(m_wal.group(1)), int(m_count.group(1))
    return None


def extract_execute_script_wall_us(line: str) -> tuple[int, int] | None:
    if "wall_us=" not in line or "sql.execute_script" not in line:
        return None
    m_wall = re.search(r"wall_us=(\d+)", line)
    if not m_wall:
        return None
    m_count = re.search(r"statement_count=(\d+)", line)
    stmt_count = int(m_count.group(1)) if m_count else 0
    return int(m_wall.group(1)), stmt_count


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


def print_us_stats(label: str, xs: list[float]) -> None:
    print(
        f"{label}: n={len(xs)} "
        f"p50={quantile(xs, 0.5) / 1000:.3f}ms p95={quantile(xs, 0.95) / 1000:.3f}ms "
        f"p99={quantile(xs, 0.99) / 1000:.3f}ms mean={statistics.fmean(xs) / 1000:.3f}ms"
    )


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--warn-lock-p99-ms",
        type=float,
        default=0.0,
        metavar="MS",
        help="emit GitHub ::warning:: when table_storage_lock aggregate p99 exceeds MS (0=off)",
    )
    ap.add_argument(
        "--warn-flush-p99-ms",
        type=float,
        default=0.0,
        metavar="MS",
        help="emit GitHub ::warning:: when sql.commit flush_us aggregate p99 exceeds MS (0=off)",
    )
    ap.add_argument(
        "--warn-district-row-lock-pct",
        type=float,
        default=90.0,
        metavar="PCT",
        help="emit ::warning:: when district sql.dml.lock_path=row is below PCT (0=off)",
    )
    ap.add_argument("path", type=Path)
    args = ap.parse_args()
    p = args.path
    if not p.is_file():
        print(f"not a file: {p}", file=sys.stderr)
        return 1

    parse_us: list[float] = []
    lock_wait_us: list[float] = []
    lock_by_table_mode: dict[tuple[str, str], list[float]] = defaultdict(list)
    row_lock_wait_us: list[float] = []
    row_lock_by_table_mode: dict[tuple[str, str], list[float]] = defaultdict(list)
    commit_flush_us: list[float] = []
    commit_wal_us: list[float] = []
    commit_flush_tables_count: list[float] = []
    execute_script_wall_us: list[float] = []
    execute_script_by_stmt_count: dict[int, list[float]] = defaultdict(list)
    lock_path_counts: dict[str, int] = defaultdict(int)
    lock_path_by_table: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    scan_us: list[float] = []
    row_loop_us: list[float] = []
    phase_lines = 0

    for line in p.read_text(encoding="utf-8", errors="replace").splitlines():
        if "rustdb::sql_phases" not in line:
            ew = extract_execute_script_wall_us(line)
            if ew is not None:
                wall_us, stmt_count = ew
                execute_script_wall_us.append(float(wall_us))
                if stmt_count > 0:
                    execute_script_by_stmt_count[stmt_count].append(float(wall_us))
            continue

        pu = extract_parse_us(line)
        if pu is not None:
            parse_us.append(float(pu))
            phase_lines += 1
        lk = extract_lock_wait(line)
        if lk is not None:
            table, mode, wait = lk
            lock_wait_us.append(float(wait))
            lock_by_table_mode[(table, mode)].append(float(wait))
            phase_lines += 1
        rlk = extract_row_lock_wait(line)
        if rlk is not None:
            table, mode, wait = rlk
            row_lock_wait_us.append(float(wait))
            row_lock_by_table_mode[(table, mode)].append(float(wait))
            phase_lines += 1
        cm = extract_commit_metrics(line)
        if cm is not None:
            commit_flush_us.append(float(cm[0]))
            commit_wal_us.append(float(cm[1]))
            commit_flush_tables_count.append(float(cm[2]))
            phase_lines += 1
        ew = extract_execute_script_wall_us(line)
        if ew is not None:
            wall_us, stmt_count = ew
            execute_script_wall_us.append(float(wall_us))
            if stmt_count > 0:
                execute_script_by_stmt_count[stmt_count].append(float(wall_us))
            phase_lines += 1
        lp = extract_lock_path(line)
        if lp is not None:
            table, path = lp
            lock_path_counts[path] += 1
            lock_path_by_table[table][path] += 1
            phase_lines += 1
        up = extract_update_pair(line)
        if up is not None:
            scan_us.append(float(up[0]))
            row_loop_us.append(float(up[1]))
            phase_lines += 1

    print(f"file: {p}")
    print(f"matched_lines: {phase_lines}")
    if parse_us:
        print_us_stats("sql_parse parse_us", parse_us)
    else:
        print("sql_parse: (no matches — set RUSTDB_SQL_PHASE_LOG=1 and RUST_LOG=info or rustdb::sql_phases=info)")
    if lock_wait_us:
        print_us_stats("table_storage_lock lock_wait_us (all)", lock_wait_us)
        print("table_storage_lock by table+mode:")
        for (table, mode), xs in sorted(
            lock_by_table_mode.items(),
            key=lambda kv: quantile(kv[1], 0.99),
            reverse=True,
        ):
            print(
                f"  {table} mode={mode}: n={len(xs)} "
                f"p50={quantile(xs, 0.5) / 1000:.3f}ms p95={quantile(xs, 0.95) / 1000:.3f}ms "
                f"p99={quantile(xs, 0.99) / 1000:.3f}ms mean={statistics.fmean(xs) / 1000:.3f}ms"
            )
        if args.warn_lock_p99_ms > 0:
            p99_ms = quantile(lock_wait_us, 0.99) / 1000.0
            if p99_ms > args.warn_lock_p99_ms:
                print(
                    f"::warning::table_storage_lock aggregate p99 {p99_ms:.3f}ms "
                    f"exceeds {args.warn_lock_p99_ms:.0f}ms soft threshold",
                    flush=True,
                )
    else:
        print("table_storage_lock: (no matches)")
    if row_lock_wait_us:
        print_us_stats("row_storage_lock lock_wait_us (all)", row_lock_wait_us)
        print("row_storage_lock by table+mode:")
        for (table, mode), xs in sorted(
            row_lock_by_table_mode.items(),
            key=lambda kv: quantile(kv[1], 0.99),
            reverse=True,
        ):
            print(
                f"  {table} mode={mode}: n={len(xs)} "
                f"p50={quantile(xs, 0.5) / 1000:.3f}ms p95={quantile(xs, 0.95) / 1000:.3f}ms "
                f"p99={quantile(xs, 0.99) / 1000:.3f}ms mean={statistics.fmean(xs) / 1000:.3f}ms"
            )
    else:
        print("row_storage_lock: (no matches)")
    if lock_path_counts:
        total_lp = sum(lock_path_counts.values())
        print("sql.dml.lock_path (all tables):")
        for path in ("row", "table", "skip"):
            n = lock_path_counts.get(path, 0)
            pct = 100.0 * n / total_lp if total_lp else 0.0
            print(f"  {path}: n={n} ({pct:.1f}%)")
        district = lock_path_by_table.get("district", {})
        if district:
            d_total = sum(district.values())
            d_row = district.get("row", 0)
            d_pct = 100.0 * d_row / d_total if d_total else 0.0
            print(
                f"sql.dml.lock_path district: row={d_row} table={district.get('table', 0)} "
                f"skip={district.get('skip', 0)} row_pct={d_pct:.1f}%"
            )
            if (
                args.warn_district_row_lock_pct > 0
                and d_total > 0
                and d_pct < args.warn_district_row_lock_pct
            ):
                print(
                    f"::warning::district row lock_path {d_pct:.1f}% is below "
                    f"{args.warn_district_row_lock_pct:.0f}% soft threshold",
                    flush=True,
                )
    else:
        print("sql.dml.lock_path: (no matches)")
    if commit_flush_us:
        print_us_stats("sql.commit flush_us", commit_flush_us)
        print_us_stats("sql.commit wal_us", commit_wal_us)
        mean_tables = statistics.fmean(commit_flush_tables_count)
        print(
            f"sql.commit flush_tables_count: n={len(commit_flush_tables_count)} "
            f"mean={mean_tables:.2f} p50={quantile(commit_flush_tables_count, 0.5):.0f}"
        )
        if args.warn_flush_p99_ms > 0:
            p99_ms = quantile(commit_flush_us, 0.99) / 1000.0
            if p99_ms > args.warn_flush_p99_ms:
                print(
                    f"::warning::sql.commit flush_us aggregate p99 {p99_ms:.3f}ms "
                    f"exceeds {args.warn_flush_p99_ms:.0f}ms soft threshold",
                    flush=True,
                )
    else:
        print("sql.commit: (no matches — COMMIT path with RUSTDB_SQL_PHASE_LOG=1)")
    if execute_script_wall_us:
        print_us_stats("sql.execute_script wall_us", execute_script_wall_us)
        if execute_script_by_stmt_count:
            print("sql.execute_script wall_us by statement_count:")
            for sc in sorted(execute_script_by_stmt_count.keys()):
                xs = execute_script_by_stmt_count[sc]
                print(
                    f"  stmt_count={sc}: n={len(xs)} "
                    f"p50={quantile(xs, 0.5) / 1000:.3f}ms "
                    f"p99={quantile(xs, 0.99) / 1000:.3f}ms"
                )
    else:
        print(
            "sql.execute_script: (no matches — set RUSTDB_SQL_PHASE_LOG=1; "
            "requires rustdb::sql_phases execute_script events or span close with wall_us)"
        )
    if scan_us:
        print_us_stats("update/delete scan_us", scan_us)
        print_us_stats("update/delete row_loop_us", row_loop_us)
    else:
        print("update/delete phase: (no matches — workload may not hit UPDATE/DELETE logs in sample)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

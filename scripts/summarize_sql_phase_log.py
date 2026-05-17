#!/usr/bin/env python3
"""
Aggregate rustdb::sql_phases / sql_parse lines from a server stderr log (tracing fmt layer).

Looks for:
  - message `sql_parse` and field parse_us=...
  - message `table_storage_lock` / `row_storage_lock` with lock_wait_us=..., table=..., mode=...
  - message `sql.dml.lock_path` with lock_path=row|table|skip
  - message `sql.commit` with flush_us=..., wal_us=..., commit_*_us sub-phases, flush_tables_count=...
  - message `update` with scan_us=..., row_loop_us=...
  - message `delete` with same fields
  - message `sql.execute_script` with wall_us=...
  - message `sql.execute_tpcc` with wall_us=..., kind=...
  - message `sql.execute_tpcc.new_order` with district_us, insert_*_us, stock_us
  - message `sql.execute_tpcc.commit` with commit_us=...

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


def extract_commit_subphase_us(line: str, name: str) -> int | None:
    if "rustdb::sql_phases" not in line or "sql.commit" not in line:
        return None
    m = re.search(rf"{name}=(\d+)", line)
    return int(m.group(1)) if m else None


def extract_commit_sub_phases(line: str) -> dict[str, int] | None:
    if "rustdb::sql_phases" not in line or "sql.commit" not in line:
        return None
    out: dict[str, int] = {}
    for phase in COMMIT_SUB_PHASES:
        m = re.search(rf"{phase}=(\d+)", line)
        if m:
            out[phase] = int(m.group(1))
    return out if out else None


def extract_execute_script_wall_us(line: str) -> tuple[int, int] | None:
    if "wall_us=" not in line or "sql.execute_script" not in line:
        return None
    m_wall = re.search(r"wall_us=(\d+)", line)
    if not m_wall:
        return None
    m_count = re.search(r"statement_count=(\d+)", line)
    stmt_count = int(m_count.group(1)) if m_count else 0
    return int(m_wall.group(1)), stmt_count


def extract_execute_tpcc_wall_us(line: str) -> tuple[int, int, int] | None:
    if "wall_us=" not in line or "sql.execute_tpcc" not in line:
        return None
    if "sql.execute_tpcc.new_order" in line or "sql.execute_tpcc.commit" in line:
        return None
    m_wall = re.search(r"wall_us=(\d+)", line)
    if not m_wall:
        return None
    m_kind = re.search(r"kind=(\d+)", line)
    kind = int(m_kind.group(1)) if m_kind else -1
    m_q = re.search(r"queue_wait_us=(\d+)", line)
    queue_wait_us = int(m_q.group(1)) if m_q else 0
    return int(m_wall.group(1)), kind, queue_wait_us


def extract_execute_tpcc_new_order_phases(line: str) -> dict[str, int] | None:
    if "sql.execute_tpcc.new_order" not in line:
        return None
    out: dict[str, int] = {}
    for name in NEW_ORDER_PHASES:
        m = re.search(rf"{name}=(\d+)", line)
        if m:
            out[name] = int(m.group(1))
    return out if out else None


def extract_execute_tpcc_commit_us(line: str) -> tuple[int, int, dict[str, int]] | None:
    if "sql.execute_tpcc.commit" not in line:
        return None
    m = re.search(r"commit_us=(\d+)", line)
    if not m:
        return None
    m_kind = re.search(r"tpcc_kind=(\d+)", line)
    kind = int(m_kind.group(1)) if m_kind else -1
    extra: dict[str, int] = {}
    for name in (
        "commit_transaction_wall_us",
        "commit_gap_us",
        "commit_engine_gap_us",
        "commit_wal_us",
        "commit_index_batch_us",
        "commit_log_append_us",
        "commit_flush_us",
        "commit_pm_lock_wait_us",
        "commit_pm_lock_scan_us",
        "commit_pm_lock_flush_us",
        "commit_heap_fsync_us",
        "flush_pm_count",
        "dirty_pages_flushed",
    ):
        m_extra = re.search(rf"{name}=(\d+)", line)
        if m_extra:
            extra[name] = int(m_extra.group(1))
    return int(m.group(1)), kind, extra


def extract_sql_commit_by_kind(line: str) -> tuple[int, dict[str, int]] | None:
    if "rustdb::sql_phases" not in line or "sql.commit" not in line:
        return None
    m_kind = re.search(r"tpcc_kind=(\d+)", line)
    if not m_kind:
        return None
    kind = int(m_kind.group(1))
    out: dict[str, int] = {}
    for phase in COMMIT_SUB_PHASES:
        m = re.search(rf"{phase}=(\d+)", line)
        if m:
            out[phase] = int(m.group(1))
    m_flush = re.search(r"commit_flush_us=(\d+)", line)
    if m_flush:
        out["commit_flush_us"] = int(m_flush.group(1))
    return kind, out if out else None


NEW_ORDER_PHASES = (
    "district_us",
    "district_update_us",
    "insert_oorder_us",
    "insert_new_order_us",
    "insert_order_line_us",
    "stock_us",
    "wal_insert_us",
    "index_sync_us",
)

COMMIT_SUB_PHASES = (
    "commit_wal_us",
    "commit_flush_us",
    "commit_index_batch_us",
    "commit_log_append_us",
    "commit_log_commit_wait_us",
    "commit_table_map_lock_us",
    "commit_pm_lock_scan_us",
    "commit_pm_lock_flush_us",
    "commit_pm_lock_wait_us",
    "commit_heap_fsync_us",
    "flush_pm_count",
    "dirty_pages_flushed",
)

COMMIT_KIND_REPORT_PHASES = (
    "commit_table_map_lock_us",
    "commit_pm_lock_scan_us",
    "commit_pm_lock_flush_us",
    "commit_pm_lock_wait_us",
    "commit_wal_us",
    "commit_flush_us",
)

TPCC_KIND_NAMES = {
    0: "new_order",
    1: "payment",
    2: "order_status",
    3: "delivery",
    4: "stock_level",
}


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
    commit_sub_phases: dict[str, list[float]] = defaultdict(list)
    commit_flush_tables_count: list[float] = []
    execute_script_wall_us: list[float] = []
    execute_script_by_stmt_count: dict[int, list[float]] = defaultdict(list)
    execute_tpcc_wall_us: list[float] = []
    execute_tpcc_by_kind: dict[int, list[float]] = defaultdict(list)
    execute_tpcc_new_order_phases: dict[str, list[float]] = defaultdict(list)
    execute_tpcc_commit_us: list[float] = []
    execute_tpcc_commit_by_kind: dict[int, list[float]] = defaultdict(list)
    execute_tpcc_commit_gap_by_kind: dict[int, list[float]] = defaultdict(list)
    execute_tpcc_queue_wait_by_kind: dict[int, list[float]] = defaultdict(list)
    sql_commit_sub_by_kind: dict[int, dict[str, list[float]]] = defaultdict(
        lambda: defaultdict(list)
    )
    new_order_pre_commit_us: list[float] = []
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
            et = extract_execute_tpcc_wall_us(line)
            if et is not None:
                wall_us, kind, queue_wait_us = et
                execute_tpcc_wall_us.append(float(wall_us))
                if kind >= 0:
                    execute_tpcc_by_kind[kind].append(float(wall_us))
                    if queue_wait_us > 0:
                        execute_tpcc_queue_wait_by_kind[kind].append(float(queue_wait_us))
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
        csp = extract_commit_sub_phases(line)
        if csp is not None:
            for name, us in csp.items():
                commit_sub_phases[name].append(float(us))
            phase_lines += 1
        ew = extract_execute_script_wall_us(line)
        if ew is not None:
            wall_us, stmt_count = ew
            execute_script_wall_us.append(float(wall_us))
            if stmt_count > 0:
                execute_script_by_stmt_count[stmt_count].append(float(wall_us))
            phase_lines += 1
        et = extract_execute_tpcc_wall_us(line)
        if et is not None:
            wall_us, kind, queue_wait_us = et
            execute_tpcc_wall_us.append(float(wall_us))
            if kind >= 0:
                execute_tpcc_by_kind[kind].append(float(wall_us))
                if queue_wait_us > 0:
                    execute_tpcc_queue_wait_by_kind[kind].append(float(queue_wait_us))
            phase_lines += 1
        no_phases = extract_execute_tpcc_new_order_phases(line)
        if no_phases is not None:
            for name, us in no_phases.items():
                execute_tpcc_new_order_phases[name].append(float(us))
            phase_lines += 1
        cu = extract_execute_tpcc_commit_us(line)
        if cu is not None:
            commit_us, kind, extra = cu
            execute_tpcc_commit_us.append(float(commit_us))
            if kind >= 0:
                execute_tpcc_commit_by_kind[kind].append(float(commit_us))
                gap_key = (
                    "commit_engine_gap_us"
                    if "commit_engine_gap_us" in extra
                    else "commit_gap_us"
                )
                if gap_key in extra:
                    execute_tpcc_commit_gap_by_kind[kind].append(float(extra[gap_key]))
            phase_lines += 1
        sc = extract_sql_commit_by_kind(line)
        if sc is not None:
            kind, phases = sc
            for name, us in phases.items():
                sql_commit_sub_by_kind[kind][name].append(float(us))
            phase_lines += 1
        if "sql.execute_tpcc.new_order_pre_commit" in line:
            m = re.search(r"pre_commit_us=(\d+)", line)
            if m:
                new_order_pre_commit_us.append(float(m.group(1)))
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
        if commit_sub_phases:
            print("sql.commit sub-phase breakdown:")
            for phase in COMMIT_SUB_PHASES:
                xs = commit_sub_phases.get(phase, [])
                if xs:
                    print_us_stats(f"  {phase}", xs)
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
    if execute_tpcc_wall_us:
        print_us_stats("sql.execute_tpcc wall_us (all kinds)", execute_tpcc_wall_us)
        if execute_tpcc_by_kind:
            kind_names = {
                0: "new_order",
                1: "payment",
                2: "order_status",
                3: "delivery",
                4: "stock_level",
            }
            print("sql.execute_tpcc wall_us by kind:")
            for kind in sorted(execute_tpcc_by_kind.keys()):
                xs = execute_tpcc_by_kind[kind]
                label = kind_names.get(kind, f"kind_{kind}")
                print(
                    f"  {label}: n={len(xs)} "
                    f"p50={quantile(xs, 0.5) / 1000:.3f}ms "
                    f"p99={quantile(xs, 0.99) / 1000:.3f}ms"
                )
    else:
        print("sql.execute_tpcc: (no matches)")
    if execute_tpcc_new_order_phases:
        print("sql.execute_tpcc.new_order phase breakdown:")
        for name in NEW_ORDER_PHASES:
            xs = execute_tpcc_new_order_phases.get(name, [])
            if xs:
                print_us_stats(f"  {name}", xs)
    else:
        print(
            "sql.execute_tpcc.new_order: (no matches — native new_order with RUSTDB_SQL_PHASE_LOG=1)"
        )
    if execute_tpcc_commit_us:
        print_us_stats("sql.execute_tpcc.commit commit_us (all kinds)", execute_tpcc_commit_us)
    else:
        print("sql.execute_tpcc.commit: (no matches)")
    if execute_tpcc_commit_by_kind:
        print("sql.execute_tpcc.commit commit_us by kind:")
        for kind in sorted(execute_tpcc_commit_by_kind.keys()):
            xs = execute_tpcc_commit_by_kind[kind]
            label = TPCC_KIND_NAMES.get(kind, f"kind_{kind}")
            print(
                f"  {label}: n={len(xs)} "
                f"p50={quantile(xs, 0.5) / 1000:.3f}ms "
                f"p99={quantile(xs, 0.99) / 1000:.3f}ms"
            )
    if execute_tpcc_queue_wait_by_kind:
        print("sql.execute_tpcc queue_wait_us by kind:")
        for kind in sorted(execute_tpcc_queue_wait_by_kind.keys()):
            xs = execute_tpcc_queue_wait_by_kind[kind]
            label = TPCC_KIND_NAMES.get(kind, f"kind_{kind}")
            print(
                f"  {label}: n={len(xs)} "
                f"p50={quantile(xs, 0.5) / 1000:.3f}ms "
                f"p95={quantile(xs, 0.95) / 1000:.3f}ms"
            )
    if sql_commit_sub_by_kind:
        print("sql.commit sub-phases by tpcc_kind (p50 ms):")
        for kind in sorted(sql_commit_sub_by_kind.keys()):
            label = TPCC_KIND_NAMES.get(kind, f"kind_{kind}")
            parts = []
            for phase in COMMIT_KIND_REPORT_PHASES:
                xs = sql_commit_sub_by_kind[kind].get(phase, [])
                if xs:
                    parts.append(f"{phase}={quantile(xs, 0.5) / 1000:.3f}")
            if parts:
                print(f"  {label}: {' '.join(parts)}")
    if execute_tpcc_new_order_phases and execute_tpcc_by_kind.get(0):
        phase_sum_p50 = sum(
            quantile(execute_tpcc_new_order_phases.get(name, []), 0.5)
            for name in NEW_ORDER_PHASES
            if execute_tpcc_new_order_phases.get(name)
        )
        wall_p50 = quantile(execute_tpcc_by_kind[0], 0.5)
        commit_p50 = quantile(execute_tpcc_commit_by_kind.get(0, []), 0.5)
        pre_p50 = quantile(new_order_pre_commit_us, 0.5) if new_order_pre_commit_us else 0.0
        commit_gap_p50 = quantile(execute_tpcc_commit_gap_by_kind.get(0, []), 0.5)
        gap = wall_p50 - phase_sum_p50 - commit_p50 - pre_p50
        print(
            "new_order server accounting (p50 us): "
            f"wall={wall_p50 / 1000:.3f}ms "
            f"phases_sum={phase_sum_p50 / 1000:.3f}ms "
            f"pre_commit={pre_p50 / 1000:.3f}ms "
            f"commit_us={commit_p50 / 1000:.3f}ms "
            f"commit_engine_gap_us={commit_gap_p50 / 1000:.3f}ms "
            f"unaccounted_gap={gap / 1000:.3f}ms"
        )
    if scan_us:
        print_us_stats("update/delete scan_us", scan_us)
        print_us_stats("update/delete row_loop_us", row_loop_us)
    else:
        print("update/delete phase: (no matches — workload may not hit UPDATE/DELETE logs in sample)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

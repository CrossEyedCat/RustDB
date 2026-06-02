#!/usr/bin/env python3
"""
Generate a Markdown report for native TPC-C new_order + commit chain timings from server_full.log.

Parses `rustdb::sql_phases` lines containing `sql.execute_tpcc.new_order` and extracts:
  - district_us, district_update_us, insert_oorder_us, insert_new_order_us,
    insert_order_line_us (+ sub-phases), stock_us, wal_insert_us, index_sync_us

Also parses native commit lines (`sql.execute_tpcc.commit`, `tpcc_kind=0`) to extract:
  - commit_us
  - queue_wait_us (from `network.queue_wait{queue_wait_us=...}` prefix when present)
  - commit_pm_lock_wait_us (if logged)

Also parses `sql.commit` lines for native new_order (`tpcc_kind=0`) to extract:
  - commit_wal_us, commit_flush_us
  - commit_* subphases (pm lock scan/flush/wait, table map lock, etc.)
  - flush counters (flush_pm_count, dirty_pages_flushed)

Outputs Markdown tables with p50/p95/p99/mean (ms) per field.

Usage:
  python3 scripts/summarize_tpcc_native_new_order_md.py server_full.log > phases_native_new_order.md
  python3 scripts/summarize_tpcc_native_new_order_md.py server_full.log --out tpcc-out/phases_native_new_order.md
"""

from __future__ import annotations

import argparse
import re
import statistics
import sys
from pathlib import Path


NEW_ORDER_FIELDS = (
    "district_us",
    "district_update_us",
    "insert_oorder_us",
    "insert_new_order_us",
    "insert_order_line_us",
    "insert_order_line_encode_us",
    "insert_order_line_heap_us",
    "insert_order_line_pm_lock_wait_us",
    "insert_order_line_pm_insert_us",
    "insert_order_line_wal_us",
    "insert_order_line_pending_index_us",
    "insert_order_line_undo_us",
    "order_line_cnt",
    "stock_us",
    "wal_insert_us",
    "index_sync_us",
)

COMMIT_FIELDS = (
    "queue_wait_us",
    "commit_us",
    "commit_pm_lock_wait_us",
)

SQL_COMMIT_FIELDS = (
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


def quantile(xs: list[int], q: float) -> float:
    if not xs:
        return 0.0
    s = sorted(xs)
    idx = (len(s) - 1) * q
    lo = int(idx)
    hi = min(lo + 1, len(s) - 1)
    frac = idx - lo
    return s[lo] * (1.0 - frac) + s[hi] * frac


def fmt_ms(us: float) -> str:
    return f"{us / 1000.0:.3f}"


def parse_file(path: Path) -> tuple[dict[str, int], dict[str, dict[str, list[int]]]]:
    out: dict[str, dict[str, list[int]]] = {
        "new_order": {k: [] for k in NEW_ORDER_FIELDS},
        "commit": {k: [] for k in COMMIT_FIELDS},
        "sql_commit": {k: [] for k in SQL_COMMIT_FIELDS},
    }
    counts = {"new_order": 0, "commit": 0, "sql_commit": 0}
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        if "rustdb::sql_phases" not in line:
            continue
        if "sql.execute_tpcc.new_order" in line:
            counts["new_order"] += 1
            for k in NEW_ORDER_FIELDS:
                m = re.search(rf"{k}=(\d+)", line)
                if m:
                    out["new_order"][k].append(int(m.group(1)))
            continue
        if "sql.execute_tpcc.commit" in line and "tpcc_kind=0" in line:
            counts["commit"] += 1
            m_q = re.search(r"queue_wait_us=(\d+)", line)
            if m_q:
                out["commit"]["queue_wait_us"].append(int(m_q.group(1)))
            m_commit = re.search(r"commit_us=(\d+)", line)
            if m_commit:
                out["commit"]["commit_us"].append(int(m_commit.group(1)))
            m_pm = re.search(r"commit_pm_lock_wait_us=(\d+)", line)
            if m_pm:
                out["commit"]["commit_pm_lock_wait_us"].append(int(m_pm.group(1)))
            continue
        if "sql.commit" in line and "tpcc_kind=0" in line:
            counts["sql_commit"] += 1
            for k in SQL_COMMIT_FIELDS:
                m = re.search(rf"{k}=(\d+)", line)
                if m:
                    out["sql_commit"][k].append(int(m.group(1)))
            continue
    return counts, out


def render_table(
    fields: tuple[str, ...],
    by_field: dict[str, list[int]],
    *,
    count_fields: frozenset[str] = frozenset({"order_line_cnt"}),
) -> list[str]:
    rows: list[str] = []
    rows.append("| field | samples | p50_ms | p95_ms | p99_ms | mean_ms |\n")
    rows.append("|-------|---------:|-------:|-------:|-------:|--------:|\n")
    for k in fields:
        xs = by_field.get(k, [])
        if not xs:
            rows.append(f"| `{k}` | 0 | - | - | - | - |\n")
            continue
        p50 = quantile(xs, 0.50)
        p95 = quantile(xs, 0.95)
        p99 = quantile(xs, 0.99)
        mean = statistics.fmean(xs)
        if k in count_fields:
            rows.append(
                f"| `{k}` | {len(xs)} | {p50:.0f} | {p95:.0f} | {p99:.0f} | {mean:.1f} |\n"
            )
        else:
            rows.append(
                f"| `{k}` | {len(xs)} | {fmt_ms(p50)} | {fmt_ms(p95)} | {fmt_ms(p99)} | {fmt_ms(mean)} |\n"
            )
    return rows


def render_md(
    source: Path, counts: dict[str, int], sections: dict[str, dict[str, list[int]]]
) -> str:
    rows: list[str] = []
    rows.append("# Native new_order -> commit chain breakdown\n")
    rows.append(f"- Source: `{source}`\n")
    rows.append(
        f"- Matched lines: `sql.execute_tpcc.new_order` **{counts.get('new_order', 0)}**, "
        f"`sql.execute_tpcc.commit tpcc_kind=0` **{counts.get('commit', 0)}**\n"
    )
    rows.append("\n")
    rows.append("## new_order phases (`sql.execute_tpcc.new_order`)\n\n")
    rows.append(
        "Timing fields are in microseconds in the log and milliseconds here. "
        "`order_line_cnt` is a row count (not a duration).\n\n"
    )
    rows.extend(render_table(NEW_ORDER_FIELDS, sections.get("new_order", {})))
    rows.append("\n")
    rows.append("## commit phases (`sql.execute_tpcc.commit`, `tpcc_kind=0`)\n\n")
    rows.extend(render_table(COMMIT_FIELDS, sections.get("commit", {})))
    rows.append("\n")
    rows.append("## sql.commit phases (`sql.commit`, `tpcc_kind=0`)\n\n")
    rows.append(
        "These fields come from `rustdb::sql_phases: sql.commit ...` lines. "
        "Times are logged in microseconds and reported here in milliseconds.\n\n"
    )
    rows.extend(render_table(SQL_COMMIT_FIELDS, sections.get("sql_commit", {})))
    return "".join(rows)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("server_log", type=Path)
    ap.add_argument("--out", type=Path, default=None)
    args = ap.parse_args()

    p = args.server_log
    if not p.is_file():
        print(f"not a file: {p}", file=sys.stderr)
        return 1

    counts, sections = parse_file(p)
    md = render_md(p, counts, sections)
    if args.out is None:
        sys.stdout.write(md)
    else:
        args.out.write_text(md, encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


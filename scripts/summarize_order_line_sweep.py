#!/usr/bin/env python3
"""Aggregate ORDER_LINE_CNT microbench runs into one Markdown table."""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path


FIELDS = (
    "insert_order_line_us",
    "insert_order_line_heap_us",
    "insert_order_line_pm_lock_wait_us",
    "insert_order_line_pm_insert_us",
    "insert_order_line_wal_us",
    "insert_order_line_encode_us",
    "insert_order_line_pending_index_us",
)


def parse_phases_md(path: Path) -> dict[str, tuple[float, float]]:
    text = path.read_text(encoding="utf-8", errors="replace")
    out: dict[str, tuple[float, float]] = {}
    for field in FIELDS + ("order_line_cnt",):
        m = re.search(
            rf"\| `{field}` \| \d+ \| ([^|]+) \| ([^|]+) \|",
            text,
        )
        if not m:
            continue
        p50_s, p95_s = m.group(1).strip(), m.group(2).strip()
        if p50_s == "-":
            continue
        out[field] = (float(p50_s), float(p95_s))
    return out


def main() -> int:
    root = Path(sys.argv[1] if len(sys.argv) > 1 else "tpcc-out/order_line_insert_micro/v3")
    rows: list[str] = []
    rows.append("# ORDER_LINE_CNT microbench sweep\n\n")
    rows.append(f"- Root: `{root.resolve()}`\n\n")
    rows.append(
        "| OL_CNT | tpmC | txns/s | order_line_cnt (p50) | "
        "insert_order_line p50/p95 (ms) | pm_lock p50/p95 (ms) | pm_insert p50/p95 (ms) | wal p50/p95 (ms) |\n"
    )
    rows.append(
        "|-------:|-----:|-------:|---------------------:|---------------------------:|-------------------:|--------------------:|-----------------:|\n"
    )

    for d in sorted(root.glob("ol*"), key=lambda p: int(p.name[2:]) if p.name[2:].isdigit() else 0):
        n = d.name[2:]
        phases = d / "phases_native_new_order.md"
        tpcc = d / "tpcc.json"
        if not phases.is_file():
            continue
        stats = parse_phases_md(phases)
        tpmc = txns = "—"
        if tpcc.is_file():
            j = json.loads(tpcc.read_text(encoding="utf-8"))
            tpmc = f"{j.get('tpmC', 0):.0f}"
            txns = f"{j.get('txns_per_s', 0):.1f}"
        ol_cnt = stats.get("order_line_cnt", (0, 0))[0]
        ins = stats.get("insert_order_line_us", (0, 0))
        lock_wait = stats.get("insert_order_line_pm_lock_wait_us", (0, 0))
        pm_ins = stats.get("insert_order_line_pm_insert_us", (0, 0))
        wal = stats.get("insert_order_line_wal_us", (0, 0))
        rows.append(
            f"| {n} | {tpmc} | {txns} | {ol_cnt:.0f} | "
            f"{ins[0]:.2f} / {ins[1]:.2f} | {lock_wait[0]:.2f} / {lock_wait[1]:.2f} | "
            f"{pm_ins[0]:.2f} / {pm_ins[1]:.2f} | {wal[0]:.2f} / {wal[1]:.2f} |\n"
        )

    sys.stdout.write("".join(rows))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""Post-run data check for the TPC-C comparison.

Throughput alone cannot distinguish a fast engine from one that quietly does less work, so this
compares the *final table state* of each engine against what its own client-side txn log recorded.
Both engines are held to the same invariants, derived from `scripts/tpcc_seed.sql` (w_ytd=0,
d_ytd=0, d_next_o_id=1 per district, c_balance=0, s_order_cnt=0) and from `txn_sql` /
`tpcc_native.rs` (one oorder + one new_order + one order_line row per new_order; +1 on w_ytd,
d_ytd and -1 on c_balance per payment).

Inputs per engine, written by the bench scripts:
  postgres: tpcc-out/postgres_tpcc_data.json  (psql, exact aggregates)
  rustdb:   tpcc-out/rustdb_data_check.txt    (raw `rustdb query --batch-file` output)

The two sides are NOT read under the same conditions and a mismatch has to be read accordingly:
PostgreSQL is queried live, on the still-running server, so it is purely a "did the engine apply
the work" check. RustDB is queried by a fresh engine over the volume after `docker rm -f` killed
the server, so it is simultaneously a crash-recovery check — the only place the bench preset's
deferred heap flush has to make good on "WAL + commits.log remain durable". Rows missing on the
RustDB side therefore mean either work not done or work not recovered; the legs in
`docs/tpcc-fair-compare.md` tell the two apart.

Usage:
  python3 scripts/validate_tpcc_data.py tpcc-out
  python3 scripts/validate_tpcc_data.py tpcc-out --engine postgres
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

DISTRICTS = 5
CUSTOMERS_PER_DISTRICT = 5
ITEMS = 5


@dataclass
class TxnCounts:
    """Successful transactions per kind, from the client-side txn log."""

    new_order: int = 0
    payment: int = 0
    delivery: int = 0
    order_status: int = 0
    stock_level: int = 0


@dataclass
class Observed:
    """Final table state, as read back from the engine."""

    w_ytd: int | None = None
    d_ytd_sum: int | None = None
    d_next_o_id_sum: int | None = None
    c_balance_sum: int | None = None
    s_order_cnt_sum: int | None = None
    oorder_count: int | None = None
    order_line_count: int | None = None
    new_order_count: int | None = None


def txn_counts(*txn_logs: Path) -> TxnCounts:
    """Successful transactions across every load-generator leg that touched the same database.

    `tpcc_throughput_ci.sh` runs an extra native micro leg after the main run, so its transactions
    have to be counted too or the final table state legitimately exceeds the main log.
    """
    counts = TxnCounts()
    for txn_log in txn_logs:
        if not txn_log.is_file():
            continue
        with txn_log.open(encoding="utf-8", errors="replace", newline="") as fh:
            for row in csv.DictReader(fh):
                if (row.get("ok") or "").strip().lower() not in ("1", "true"):
                    continue
                kind = (row.get("kind") or "").strip()
                if hasattr(counts, kind):
                    setattr(counts, kind, getattr(counts, kind) + 1)
    return counts


def check(observed: Observed, counts: TxnCounts) -> list[str]:
    """Return one message per violated invariant (empty list == data matches the txn log)."""
    problems: list[str] = []

    def eq(name: str, got: int | None, want: int) -> None:
        if got is None:
            problems.append(f"{name}: not collected")
        elif got != want:
            problems.append(f"{name}: {got} != {want} (delta {got - want})")

    def bounded(name: str, got: int | None, hi: int) -> None:
        if got is None:
            problems.append(f"{name}: not collected")
        elif not 0 <= got <= hi:
            problems.append(f"{name}: {got} outside [0, {hi}]")

    eq("warehouse.w_ytd", observed.w_ytd, counts.payment)
    eq("sum(district.d_ytd)", observed.d_ytd_sum, counts.payment)
    eq("sum(district.d_next_o_id)", observed.d_next_o_id_sum, DISTRICTS + counts.new_order)
    eq("count(oorder)", observed.oorder_count, counts.new_order)
    eq("count(order_line)", observed.order_line_count, counts.new_order)
    eq("sum(stock.s_order_cnt)", observed.s_order_cnt_sum, counts.new_order)

    # delivery deletes whole districts' worth of rows, so only a bound holds here.
    bounded("count(new_order)", observed.new_order_count, counts.new_order)

    # `tpcc_seed.sql` seeds customers only in district 1 while `txn_params` picks d_id in 1..5, so
    # roughly 4 out of 5 payments update no customer row at all (symmetric across engines — both
    # execute the same no-op). Only a bound is checkable without the per-txn d_id.
    bounded(
        "-sum(customer.c_balance)",
        None if observed.c_balance_sum is None else -observed.c_balance_sum,
        counts.payment,
    )

    return problems


def observed_from_postgres(data: dict[str, Any]) -> Observed:
    def get(key: str) -> int | None:
        v = data.get(key)
        return None if v is None else int(v)

    return Observed(
        w_ytd=get("w_ytd"),
        d_ytd_sum=get("d_ytd_sum"),
        d_next_o_id_sum=get("d_next_o_id_sum"),
        c_balance_sum=get("c_balance_sum"),
        s_order_cnt_sum=get("s_order_cnt_sum"),
        oorder_count=get("oorder_count"),
        order_line_count=get("order_line_count"),
        new_order_count=get("new_order_count"),
    )


# `rustdb query --batch-file` prints, per statement:
#   Information [batch:N]: <sql>
#   columns: ["id", "w_ytd", ...]
#   ["BigInt(2)", "Integer(7)", ...]
_BATCH_RE = re.compile(r"^Information \[batch:(\d+)\]:\s*(.*)$")
_COLUMNS_RE = re.compile(r'^columns:\s*\[(.*)\]\s*$')
_ROW_RE = re.compile(r'^\[(.*)\]\s*$')
_SCALAR_RE = re.compile(r"^(?:Integer|BigInt|SmallInt|Float|Double)\((-?\d+)")


@dataclass
class CliStatement:
    sql: str
    columns: list[str] = field(default_factory=list)
    rows: list[list[str]] = field(default_factory=list)

    def scalar(self, column: str) -> int | None:
        """First row's value for `column`, as an int."""
        if column not in self.columns or not self.rows:
            return None
        idx = self.columns.index(column)
        row = self.rows[0]
        if idx >= len(row):
            return None
        m = _SCALAR_RE.match(row[idx])
        return int(m.group(1)) if m else None


def _split_quoted(body: str) -> list[str]:
    """Split a `"a", "b"` list body, unescaping the inner quotes rustdb's Debug output emits."""
    out: list[str] = []
    for part in re.findall(r'"((?:[^"\\]|\\.)*)"', body):
        out.append(part.replace('\\"', '"').replace("\\\\", "\\"))
    return out


def parse_rustdb_cli(text: str) -> list[CliStatement]:
    statements: list[CliStatement] = []
    current: CliStatement | None = None
    for line in text.splitlines():
        line = line.rstrip()
        m = _BATCH_RE.match(line)
        if m:
            current = CliStatement(sql=m.group(2).strip())
            statements.append(current)
            continue
        if current is None:
            continue
        m = _COLUMNS_RE.match(line)
        if m:
            current.columns = _split_quoted(m.group(1))
            continue
        m = _ROW_RE.match(line)
        if m:
            current.rows.append(_split_quoted(m.group(1)))
    return statements


def observed_from_rustdb(text: str) -> Observed:
    """Read the invariants out of the CLI transcript produced by `rustdb_data_check_sql()`.

    Only full-key equality lookups and full scans are used: RustDB's SQL path returns no rows for
    index-prefix or range predicates, and does not evaluate ungrouped COUNT/SUM, so the aggregation
    happens here instead of in the engine.
    """
    by_sql = {s.sql: s for s in parse_rustdb_cli(text)}

    def stmt(sql: str) -> CliStatement | None:
        return by_sql.get(sql)

    def row_count(sql: str) -> int | None:
        s = stmt(sql)
        return None if s is None else len(s.rows)

    def summed(sqls: list[str], column: str) -> int | None:
        total = 0
        for sql in sqls:
            s = stmt(sql)
            if s is None:
                return None
            if not s.rows:
                # Key not present in the seed (e.g. customers exist only in district 1).
                continue
            v = s.scalar(column)
            if v is None:
                return None
            total += v
        return total

    warehouse = stmt("SELECT * FROM warehouse WHERE w_id = 1")
    district_sqls = [
        f"SELECT * FROM district WHERE d_w_id = 1 AND d_id = {d}" for d in range(1, DISTRICTS + 1)
    ]
    customer_sqls = [
        f"SELECT * FROM customer WHERE c_w_id = 1 AND c_d_id = {d} AND c_id = {c}"
        for d in range(1, DISTRICTS + 1)
        for c in range(1, CUSTOMERS_PER_DISTRICT + 1)
    ]
    stock_sqls = [f"SELECT * FROM stock WHERE s_w_id = 1 AND s_i_id = {i}" for i in range(1, ITEMS + 1)]

    return Observed(
        w_ytd=None if warehouse is None else warehouse.scalar("w_ytd"),
        d_ytd_sum=summed(district_sqls, "d_ytd"),
        d_next_o_id_sum=summed(district_sqls, "d_next_o_id"),
        c_balance_sum=summed(customer_sqls, "c_balance"),
        s_order_cnt_sum=summed(stock_sqls, "s_order_cnt"),
        oorder_count=row_count("SELECT * FROM oorder"),
        order_line_count=row_count("SELECT * FROM order_line"),
        new_order_count=row_count("SELECT * FROM new_order"),
    )


def rustdb_data_check_sql() -> str:
    """The batch `rustdb query` script whose transcript `observed_from_rustdb` reads back."""
    lines = ["SELECT * FROM warehouse WHERE w_id = 1"]
    lines += [f"SELECT * FROM district WHERE d_w_id = 1 AND d_id = {d}" for d in range(1, DISTRICTS + 1)]
    lines += [
        f"SELECT * FROM customer WHERE c_w_id = 1 AND c_d_id = {d} AND c_id = {c}"
        for d in range(1, DISTRICTS + 1)
        for c in range(1, CUSTOMERS_PER_DISTRICT + 1)
    ]
    lines += [f"SELECT * FROM stock WHERE s_w_id = 1 AND s_i_id = {i}" for i in range(1, ITEMS + 1)]
    lines += [
        "SELECT * FROM oorder",
        "SELECT * FROM order_line",
        "SELECT * FROM new_order",
    ]
    return "\n".join(lines) + "\n"


def engine_report(engine: str, out_dir: Path) -> dict[str, Any] | None:
    """None when the engine's inputs are absent (leg not run)."""
    if engine == "postgres":
        data_path = out_dir / "postgres_tpcc_data.json"
        log_paths = [out_dir / "postgres_tpcc_txn.log"]
        if not data_path.is_file() or not log_paths[0].is_file():
            return None
        observed = observed_from_postgres(json.loads(data_path.read_text(encoding="utf-8")))
    else:
        data_path = out_dir / "rustdb_data_check.txt"
        log_paths = [out_dir / "tpcc_txn.log", out_dir / "tpcc_native_micro_txn.log"]
        if not data_path.is_file() or not log_paths[0].is_file():
            return None
        observed = observed_from_rustdb(data_path.read_text(encoding="utf-8", errors="replace"))

    counts = txn_counts(*log_paths)
    problems = check(observed, counts)
    return {
        "engine": engine,
        "valid": not problems,
        "problems": problems,
        "txn_counts": counts.__dict__,
        "observed": observed.__dict__,
    }


def main(argv: list[str]) -> int:
    ap = argparse.ArgumentParser(description="TPC-C post-run data check")
    ap.add_argument("out_dir", type=Path)
    ap.add_argument("--engine", choices=["postgres", "rustdb", "both"], default="both")
    ap.add_argument(
        "--warn-only",
        action="store_true",
        help="report mismatches without failing (first rollout / investigating)",
    )
    args = ap.parse_args(argv)

    engines = ["postgres", "rustdb"] if args.engine == "both" else [args.engine]
    reports = [r for r in (engine_report(e, args.out_dir) for e in engines) if r is not None]

    out = {"valid": all(r["valid"] for r in reports), "engines": reports}
    print(json.dumps(out, indent=2, sort_keys=True))
    (args.out_dir / "validation_data.json").write_text(
        json.dumps(out, indent=2, sort_keys=True), encoding="utf-8"
    )

    if not reports:
        print("no engine inputs found — nothing checked", file=sys.stderr)
        return 0
    if out["valid"]:
        return 0
    print("post-run data check FAILED", file=sys.stderr)
    return 0 if args.warn_only else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))

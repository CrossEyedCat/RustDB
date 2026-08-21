#!/usr/bin/env python3
"""Unit tests for validate_tpcc_data.py (pytest or unittest)."""

from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))

from validate_tpcc_data import (  # noqa: E402
    DISTRICTS,
    Observed,
    TxnCounts,
    check,
    engine_report,
    observed_from_postgres,
    observed_from_rustdb,
    parse_rustdb_cli,
    rustdb_data_check_sql,
    txn_counts,
)

# Verbatim shape of `rustdb query --batch-file` output (see scripts/tpcc_throughput_ci.sh).
CLI_SAMPLE = """Information [batch:1]: SELECT * FROM warehouse WHERE w_id = 1
columns: ["id", "w_id", "w_name", "w_tax", "w_ytd"]
["BigInt(1)", "Integer(1)", "Varchar(\\"'W1'\\")", "Integer(8)", "Integer(2)"]
Information [batch:2]: SELECT * FROM oorder
columns: ["id"]
["BigInt(7)"]
["BigInt(8)"]
Information [batch:3]: SELECT * FROM stock WHERE s_w_id = 1 AND s_i_id = 1
columns: []
Success
"""


def _consistent(payments: int = 10, new_orders: int = 4) -> Observed:
    return Observed(
        w_ytd=payments,
        d_ytd_sum=payments,
        d_next_o_id_sum=DISTRICTS + new_orders,
        c_balance_sum=-payments,
        s_order_cnt_sum=new_orders,
        oorder_count=new_orders,
        order_line_count=new_orders,
        new_order_count=new_orders,
    )


class ParseCliTests(unittest.TestCase):
    def test_statements_columns_and_rows(self) -> None:
        stmts = parse_rustdb_cli(CLI_SAMPLE)
        self.assertEqual(len(stmts), 3)
        self.assertEqual(stmts[0].sql, "SELECT * FROM warehouse WHERE w_id = 1")
        self.assertEqual(stmts[0].scalar("w_ytd"), 2)
        self.assertEqual(stmts[0].scalar("w_id"), 1)
        # Full scans project only `id`, which is still enough to count rows.
        self.assertEqual(len(stmts[1].rows), 2)
        # An empty result must not look like a missing statement.
        self.assertEqual(stmts[2].rows, [])
        self.assertIsNone(stmts[2].scalar("s_order_cnt"))

    def test_missing_column_is_none(self) -> None:
        stmts = parse_rustdb_cli(CLI_SAMPLE)
        self.assertIsNone(stmts[1].scalar("o_ol_cnt"))

    def test_data_check_sql_covers_every_observed_table(self) -> None:
        sql = rustdb_data_check_sql()
        for table in ("warehouse", "district", "customer", "stock", "oorder", "order_line", "new_order"):
            self.assertIn(f"FROM {table}", sql)
        # Only full-key lookups and full scans: RustDB returns no rows for index-prefix or range
        # predicates, so a prefix here would silently read as "engine applied nothing".
        for line in sql.splitlines():
            self.assertNotIn("<", line)
            if "WHERE" in line and "district" in line:
                self.assertIn("d_w_id = 1 AND d_id =", line)


class CheckTests(unittest.TestCase):
    def test_consistent_state_passes(self) -> None:
        self.assertEqual(check(_consistent(), TxnCounts(new_order=4, payment=10)), [])

    def test_missing_rows_are_reported(self) -> None:
        observed = _consistent()
        observed.oorder_count = 3
        problems = check(observed, TxnCounts(new_order=4, payment=10))
        self.assertEqual(len(problems), 1)
        self.assertIn("count(oorder): 3 != 4", problems[0])

    def test_uncollected_value_is_reported(self) -> None:
        observed = _consistent()
        observed.w_ytd = None
        problems = check(observed, TxnCounts(new_order=4, payment=10))
        self.assertIn("warehouse.w_ytd: not collected", problems)

    def test_delivery_may_shrink_new_order_but_not_grow_it(self) -> None:
        observed = _consistent()
        observed.new_order_count = 0
        self.assertEqual(check(observed, TxnCounts(new_order=4, payment=10)), [])
        observed.new_order_count = 5
        self.assertTrue(check(observed, TxnCounts(new_order=4, payment=10)))

    def test_customer_balance_is_a_bound(self) -> None:
        # Customers exist only in district 1, so most payments update no customer row.
        observed = _consistent()
        observed.c_balance_sum = -2
        self.assertEqual(check(observed, TxnCounts(new_order=4, payment=10)), [])
        observed.c_balance_sum = -11
        self.assertTrue(check(observed, TxnCounts(new_order=4, payment=10)))


class TxnCountsTests(unittest.TestCase):
    def test_failed_attempts_are_not_counted(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            log = Path(tmp) / "txn.log"
            log.write_text(
                "worker_id,global_attempt_id,kind,ok,elapsed_us,error\n"
                "0,0,payment,1,10,\n"
                "0,1,payment,0,10,boom\n"
                "0,2,new_order,1,10,\n",
                encoding="utf-8",
            )
            counts = txn_counts(log)
        self.assertEqual(counts.payment, 1)
        self.assertEqual(counts.new_order, 1)


class EngineReportTests(unittest.TestCase):
    def test_missing_inputs_report_nothing(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            self.assertIsNone(engine_report("postgres", Path(tmp)))
            self.assertIsNone(engine_report("rustdb", Path(tmp)))

    def test_postgres_round_trip(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            d = Path(tmp)
            (d / "postgres_tpcc_txn.log").write_text(
                "worker_id,global_attempt_id,kind,ok,elapsed_us,error\n"
                "0,0,payment,1,10,\n"
                "0,1,new_order,1,10,\n",
                encoding="utf-8",
            )
            (d / "postgres_tpcc_data.json").write_text(
                json.dumps(
                    {
                        "w_ytd": 1,
                        "d_ytd_sum": 1,
                        "d_next_o_id_sum": DISTRICTS + 1,
                        "c_balance_sum": -1,
                        "s_order_cnt_sum": 1,
                        "oorder_count": 1,
                        "order_line_count": 1,
                        "new_order_count": 1,
                    }
                ),
                encoding="utf-8",
            )
            report = engine_report("postgres", d)
        assert report is not None
        self.assertTrue(report["valid"], report["problems"])

    def test_observed_from_postgres_accepts_strings(self) -> None:
        observed = observed_from_postgres({"w_ytd": "7", "oorder_count": "3"})
        self.assertEqual(observed.w_ytd, 7)
        self.assertEqual(observed.oorder_count, 3)
        self.assertIsNone(observed.d_ytd_sum)

    def test_observed_from_rustdb_reads_the_transcript(self) -> None:
        observed = observed_from_rustdb(CLI_SAMPLE)
        self.assertEqual(observed.w_ytd, 2)
        self.assertEqual(observed.oorder_count, 2)
        # Statements absent from the transcript stay None rather than defaulting to zero.
        self.assertIsNone(observed.order_line_count)


if __name__ == "__main__":
    try:
        import pytest  # noqa: F401

        raise SystemExit(pytest.main([__file__, "-v"]))
    except ImportError:
        unittest.main()

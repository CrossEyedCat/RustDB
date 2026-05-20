#!/usr/bin/env python3
"""Unit tests for validate_tpcc_run.py (pytest or unittest)."""

from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))

from validate_tpcc_run import GATES, validate_run  # noqa: E402

FIXTURES = ROOT / "tests" / "fixtures" / "tpcc_validation"


def _write_pair(
    d: Path,
    *,
    pg_tps: float = 850.0,
    rd_tps: float = 960.0,
    pg_payment_p95_us: int = 200_000,
    degraded_payment: bool = False,
    new_order_share: float = 0.45,
    server_lines: list[str] | None = None,
) -> None:
    n = 1000
    no_count = int(n * new_order_share)
    pay_count = n - no_count
    pg_json = {
        "txns_per_s": pg_tps,
        "success_rate_pct": 100.0,
        "err": 0,
    }
    rd_json = {
        "txns_per_s": rd_tps,
        "success_rate_pct": 100.0,
        "err": 0,
    }
    (d / "postgres_tpcc.json").write_text(json.dumps(pg_json), encoding="utf-8")
    (d / "tpcc.json").write_text(json.dumps(rd_json), encoding="utf-8")

    def txn_log(path: Path, payment_us: int) -> None:
        rows = ["worker_id,global_attempt_id,kind,ok,elapsed_us,error"]
        gid = 0
        for _ in range(no_count):
            rows.append(f"0,{gid},new_order,true,50000,")
            gid += 1
        for _ in range(pay_count):
            rows.append(f"0,{gid},payment,true,{payment_us},")
            gid += 1
        path.write_text("\n".join(rows) + "\n", encoding="utf-8")

    pay_us = 1_300_000 if degraded_payment else pg_payment_p95_us
    txn_log(d / "postgres_tpcc_txn.log", pay_us)
    txn_log(d / "tpcc_txn.log", 50_000)

    if server_lines is not None:
        (d / "server_full.log").write_text("\n".join(server_lines) + "\n", encoding="utf-8")


def _bench_server_lines(n: int = 200) -> list[str]:
    lines = []
    for _ in range(n):
        lines.append(
            "rustdb::sql_phases: sql.commit commit_flush_us=0 "
            "commit_heap_flush_skipped=1 tpcc_kind=0"
        )
    return lines


def _strict_server_lines(n: int = 200) -> list[str]:
    lines = []
    for _ in range(n):
        lines.append(
            "rustdb::sql_phases: sql.commit commit_flush_us=50000 "
            "commit_heap_flush_skipped=0 tpcc_kind=0"
        )
    return lines


class ValidateTpccRunTests(unittest.TestCase):
    def test_fixture_valid_bench_passes(self) -> None:
        d = FIXTURES / "valid_bench"
        valid, report = validate_run(d, "bench")
        self.assertTrue(valid, report.get("reasons"))
        self.assertGreater(
            report["metrics"]["ratio_percent_rustdb_over_postgres"],
            GATES["ratio_claim_min_pct"],
        )

    def test_fixture_degraded_pg_fails(self) -> None:
        d = FIXTURES / "degraded_pg"
        valid, report = validate_run(d, "bench")
        self.assertFalse(valid)
        self.assertTrue(any("payment" in r for r in report["reasons"]))

    def test_synthetic_valid_bench(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            d = Path(tmp)
            _write_pair(d, server_lines=_bench_server_lines())
            valid, _ = validate_run(d, "bench")
            self.assertTrue(valid)

    def test_synthetic_degraded_pg_payment(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            d = Path(tmp)
            _write_pair(d, degraded_payment=True, server_lines=_bench_server_lines())
            valid, report = validate_run(d, "bench")
            self.assertFalse(valid)
            self.assertTrue(any("payment" in r for r in report["reasons"]))

    def test_synthetic_strict_mode_passes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            d = Path(tmp)
            _write_pair(
                d,
                pg_tps=820.0,
                rd_tps=500.0,
                server_lines=_strict_server_lines(),
            )
            valid, _ = validate_run(d, "strict")
            self.assertTrue(valid)

    def test_bench_mode_fails_with_strict_server_profile(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            d = Path(tmp)
            _write_pair(d, server_lines=_strict_server_lines())
            valid, report = validate_run(d, "bench")
            self.assertFalse(valid)
            self.assertTrue(
                any("skipped" in r or "flush" in r for r in report["reasons"])
            )


if __name__ == "__main__":
    try:
        import pytest  # noqa: F401

        raise SystemExit(pytest.main([__file__, "-v"]))
    except ImportError:
        unittest.main()

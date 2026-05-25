#!/usr/bin/env python3
"""Unit tests for tpcc_concurrency_plot.py."""

from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))

import tpcc_concurrency_plot as plot  # noqa: E402

try:
    import matplotlib  # noqa: F401

    HAS_MATPLOTLIB = True
except ImportError:
    HAS_MATPLOTLIB = False


def _step(
    root: Path,
    c: int,
    *,
    rd_tps: float,
    pg_tps: float,
    no_rd_ms: float,
    no_pg_ms: float,
    valid: bool = True,
) -> None:
    d = root / f"c{c}"
    d.mkdir(parents=True)
    (d / "tpcc.json").write_text(
        json.dumps(
            {
                "txns_per_s": rd_tps,
                "p50_ms": 80.0 + c,
                "p95_ms": 120.0 + c,
                "p99_ms": 200.0 + c,
            }
        ),
        encoding="utf-8",
    )
    (d / "postgres_tpcc.json").write_text(
        json.dumps({"txns_per_s": pg_tps, "p50_ms": 70.0}),
        encoding="utf-8",
    )
    ratio = 100.0 * rd_tps / pg_tps if pg_tps else 0.0
    (d / "validation.json").write_text(
        json.dumps(
            {
                "valid": valid,
                "metrics": {
                    "rustdb_txns_per_s": rd_tps,
                    "postgres_txns_per_s": pg_tps,
                    "ratio_percent_rustdb_over_postgres": ratio,
                    "rustdb_txn_log": {
                        "per_kind": {
                            "new_order": {"p50_ms": no_rd_ms},
                            "payment": {"p50_ms": 5.0},
                        }
                    },
                    "postgres_txn_log": {
                        "per_kind": {
                            "new_order": {"p50_ms": no_pg_ms},
                            "payment": {"p50_ms": 4.0},
                        }
                    },
                },
            }
        ),
        encoding="utf-8",
    )


class TestTpccConcurrencyPlot(unittest.TestCase):
    def test_discover_steps_sorted(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _step(root, 32, rd_tps=900, pg_tps=920, no_rd_ms=140, no_pg_ms=120)
            _step(root, 8, rd_tps=400, pg_tps=450, no_rd_ms=90, no_pg_ms=85)
            _step(root, 16, rd_tps=700, pg_tps=750, no_rd_ms=110, no_pg_ms=100)
            steps = plot.discover_steps(root)
            self.assertEqual([s.concurrency for s in steps], [8, 16, 32])
            self.assertAlmostEqual(steps[2].ratio_pct, 100.0 * 900 / 920, places=3)
            self.assertAlmostEqual(steps[0].per_kind_rustdb_p50["new_order"], 90.0)

    def test_saturation_knee(self) -> None:
        steps = [
            plot.StepMetrics(8, Path("."), None, rustdb_tps=400.0),
            plot.StepMetrics(16, Path("."), None, rustdb_tps=900.0),
            plot.StepMetrics(32, Path("."), None, rustdb_tps=880.0),
        ]
        self.assertEqual(plot.saturation_concurrency(steps, "rustdb", 0.98), 16)

    def test_write_csv_and_markdown(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _step(root, 8, rd_tps=500, pg_tps=520, no_rd_ms=100, no_pg_ms=95)
            steps = plot.discover_steps(root)
            csv_path = root / "sweep.csv"
            plot.write_csv(steps, csv_path)
            text = csv_path.read_text(encoding="utf-8")
            self.assertIn("concurrency", text)
            self.assertIn("8", text)
            md = plot.write_markdown(steps, root, [], {"duration_secs": 60})
            self.assertTrue(md.is_file())
            self.assertIn("Saturation", md.read_text(encoding="utf-8"))

    @unittest.skipUnless(HAS_MATPLOTLIB, "matplotlib not installed")
    def test_plot_all_writes_pngs(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _step(root, 8, rd_tps=500, pg_tps=520, no_rd_ms=100, no_pg_ms=95)
            _step(root, 16, rd_tps=800, pg_tps=820, no_rd_ms=120, no_pg_ms=110)
            steps = plot.discover_steps(root)
            plots = plot.plot_all(steps, root / "plots", " (test)")
            self.assertGreaterEqual(len(plots), 4)
            for p in plots:
                self.assertTrue(p.is_file(), p)
                self.assertGreater(p.stat().st_size, 500)


if __name__ == "__main__":
    unittest.main()

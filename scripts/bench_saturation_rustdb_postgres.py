#!/usr/bin/env python3
"""
Throughput saturation sweep: RustDB (QUIC via rustdb_load) vs PostgreSQL.

Runs the same SQL at increasing concurrency levels, records QPS/latency, estimates
"saturation" (first concurrency at which QPS reaches a fraction of the observed peak),
and writes CSV + Markdown + PNG charts.
"""
from __future__ import annotations

import argparse
import csv
import sys
from dataclasses import dataclass
from pathlib import Path

# Reuse benchmark helpers from the main comparison script (same repo).
_SCRIPTS = Path(__file__).resolve().parent
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

import bench_sqlite_vs_rustdb as bench  # noqa: E402


@dataclass
class SaturationSummary:
    system: str
    peak_qps: float
    peak_concurrency: int
    """Smallest concurrency with QPS >= plateau_frac * peak_qps."""
    saturation_concurrency: int | None


def parse_concurrencies(raw: str) -> list[int]:
    out = [int(x.strip()) for x in raw.split(",") if x.strip()]
    if not out:
        raise SystemExit("empty --concurrency-steps")
    for c in out:
        if c < 1:
            raise SystemExit(f"invalid concurrency: {c}")
    return sorted(set(out))


def summarize_series(rows: list[bench.Point], system_name: str, plateau_frac: float) -> SaturationSummary:
    rows = sorted(rows, key=lambda p: p.concurrency)
    if not rows:
        return SaturationSummary(system=system_name, peak_qps=0.0, peak_concurrency=0, saturation_concurrency=None)
    peak = max(rows, key=lambda p: p.qps)
    peak_qps = peak.qps
    peak_c = peak.concurrency
    target = peak_qps * plateau_frac
    sat_c: int | None = None
    for p in rows:
        if p.qps >= target:
            sat_c = p.concurrency
            break
    return SaturationSummary(
        system=system_name,
        peak_qps=peak_qps,
        peak_concurrency=peak_c,
        saturation_concurrency=sat_c,
    )


def plot_saturation(
    rustdb_pts: list[bench.Point],
    pg_pts: list[bench.Point],
    out_png: Path,
    title: str,
) -> None:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    fig, axes = plt.subplots(1, 2, figsize=(12, 5))

    def series(pts: list[bench.Point]):
        pts = sorted(pts, key=lambda p: p.concurrency)
        return [p.concurrency for p in pts], [p.qps for p in pts], [p.p99_ms for p in pts]

    rx, rq, rp99 = series(rustdb_pts)
    px, pq, pp99 = series(pg_pts)

    ax = axes[0]
    ax.set_title(f"{title} — throughput (QPS)")
    ax.set_xlabel("concurrency")
    ax.set_ylabel("QPS")
    ax.plot(rx, rq, marker="o", label="RustDB (QUIC)")
    ax.plot(px, pq, marker="s", label="PostgreSQL (TCP)")
    ax.grid(True, alpha=0.3)
    ax.legend()

    ax = axes[1]
    ax.set_title(f"{title} — tail latency (p99)")
    ax.set_xlabel("concurrency")
    ax.set_ylabel("p99 (ms)")
    ax.plot(rx, rp99, marker="o", label="RustDB")
    ax.plot(px, pp99, marker="s", label="PostgreSQL")
    ax.grid(True, alpha=0.3)
    ax.legend()

    fig.tight_layout()
    fig.savefig(out_png)


def main() -> int:
    ap = argparse.ArgumentParser(description="RustDB vs Postgres throughput saturation sweep")
    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--cert", required=True)
    ap.add_argument("--addr", default="127.0.0.1:15432")
    ap.add_argument("--server-name", default="localhost")
    ap.add_argument("--postgres-dsn", required=True)
    ap.add_argument(
        "--sql",
        default="SELECT 1",
        help="Single statement for both engines (default: SELECT 1).",
    )
    ap.add_argument(
        "--concurrency-steps",
        default="1,2,4,8,16,32,64,128,256,512",
        help="Comma-separated concurrency levels (sorted, deduped).",
    )
    ap.add_argument("--queries-per-step", type=int, default=12_000, help="Total queries per engine per step.")
    ap.add_argument(
        "--plateau-frac",
        type=float,
        default=0.98,
        help="Report saturation as smallest c where QPS >= this fraction of peak QPS (per engine).",
    )
    ap.add_argument(
        "--rustdb-connection-mode",
        default="shared",
        choices=["shared", "per-worker"],
    )
    ap.add_argument("--rustdb-stream-batch", type=int, default=8)
    ap.add_argument("--rustdb-quic-max-streams", type=int, default=512)
    ap.add_argument("--rustdb-quic-idle-secs", type=int, default=30)
    args = ap.parse_args()

    repo_root = Path(__file__).resolve().parents[1]
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    cert_path = Path(args.cert)
    steps = parse_concurrencies(args.concurrency_steps)
    sql = args.sql
    setup_pg: list[str] = []

    rustdb_label = bench.rustdb_system_label(
        args.rustdb_connection_mode,
        args.rustdb_stream_batch,
        distinguish_batches=True,
    )

    points: list[bench.Point] = []

    for c in steps:
        q = max(args.queries_per_step, 50 * c)
        pg = bench.postgres_bench(args.postgres_dsn, sql, c, q, setup_pg, suite="saturation")
        pg.scenario = "saturation"
        points.append(pg)

        rd = bench.rustdb_bench(
            repo_root,
            cert_path,
            args.addr,
            args.server_name,
            sql,
            c,
            q,
            args.rustdb_connection_mode,
            stream_batch=args.rustdb_stream_batch,
            quic_max_streams=args.rustdb_quic_max_streams,
            quic_idle_secs=args.rustdb_quic_idle_secs,
            distinguish_batches=True,
            suite="saturation",
        )
        rd.scenario = "saturation"
        points.append(rd)

    rustdb_pts = [p for p in points if p.system.startswith("rustdb")]
    pg_pts = [p for p in points if p.system == "postgres"]

    sum_r = summarize_series(rustdb_pts, rustdb_label, args.plateau_frac)
    sum_p = summarize_series(pg_pts, "postgres", args.plateau_frac)

    csv_path = out_dir / "saturation.csv"
    with csv_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(
            [
                "system",
                "concurrency",
                "queries",
                "qps",
                "p50_ms",
                "p95_ms",
                "p99_ms",
                "max_ms",
                "mean_ms",
                "wall_ms",
                "ok",
                "err",
            ]
        )
        for p in sorted(points, key=lambda x: (x.system, x.concurrency)):
            w.writerow(
                [
                    p.system,
                    p.concurrency,
                    p.ok_count + p.err_count,
                    f"{p.qps:.4f}",
                    f"{p.p50_ms:.4f}",
                    f"{p.p95_ms:.4f}",
                    f"{p.p99_ms:.4f}",
                    f"{p.max_ms:.4f}",
                    f"{p.mean_ms:.4f}",
                    f"{p.wall_ms:.4f}",
                    p.ok_count,
                    p.err_count,
                ]
            )

    md_path = out_dir / "saturation.md"
    with md_path.open("w", encoding="utf-8") as f:
        f.write("## Throughput saturation: RustDB vs PostgreSQL\n\n")
        f.write(
            "Load generator: **RustDB** — `rustdb_load` over QUIC (same flags as this run); "
            "**PostgreSQL** — `psycopg` over TCP, one connection per worker thread.\n\n"
        )
        f.write(f"- SQL: `{sql}`\n")
        f.write(f"- Concurrency steps: **{', '.join(map(str, steps))}**\n")
        f.write(f"- Queries per step (min `{args.queries_per_step}`, scaled with `max(…, 50 * c)`): see CSV `queries` column.\n")
        f.write(
            f"- RustDB: **connection_mode={args.rustdb_connection_mode}**, **stream_batch={args.rustdb_stream_batch}**, "
            f"**quic_max_streams={args.rustdb_quic_max_streams}**.\n\n"
        )
        f.write("### Estimated saturation (plateau)\n\n")
        f.write(
            f"We report the **smallest concurrency** where QPS reaches **≥ {args.plateau_frac:.0%}** "
            "of the observed **peak QPS** for that engine (rough saturation knee).\n\n"
        )
        f.write("| Engine | Peak QPS | @ concurrency | Saturation (≥ plateau) @ c |\n")
        f.write("|---|---:|---:|---:|\n")
        f.write(
            f"| {sum_r.system} | {sum_r.peak_qps:.1f} | {sum_r.peak_concurrency} | "
            f"{sum_r.saturation_concurrency or '—'} |\n"
        )
        f.write(
            f"| PostgreSQL | {sum_p.peak_qps:.1f} | {sum_p.peak_concurrency} | "
            f"{sum_p.saturation_concurrency or '—'} |\n\n"
        )
        f.write("### Raw table\n\n")
        f.write("| system | c | qps | p99 (ms) | ok | err |\n|---|---:|---:|---:|---:|---:|\n")
        for p in sorted(points, key=lambda x: (x.system, x.concurrency)):
            f.write(f"| {p.system} | {p.concurrency} | {p.qps:.1f} | {p.p99_ms:.3f} | {p.ok_count} | {p.err_count} |\n")
        f.write("\n### Artifacts\n\n")
        f.write("- `saturation.csv` — machine-readable series\n")
        f.write("- `saturation.png` — QPS and p99 vs concurrency\n")

    png_path = out_dir / "saturation.png"
    plot_saturation(rustdb_pts, pg_pts, png_path, sql[:40] + ("…" if len(sql) > 40 else ""))

    print(f"Wrote {md_path}")
    print(f"Wrote {csv_path}")
    print(f"Wrote {png_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

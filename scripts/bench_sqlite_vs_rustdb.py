import argparse
import json
import os
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
import threading


@dataclass
class Point:
    system: str  # "rustdb" | "sqlite"
    scenario: str
    concurrency: int
    qps: float
    p50_ms: float
    p95_ms: float
    p99_ms: float


def run(cmd: list[str], *, check=True, capture=True, cwd: Path | None = None) -> subprocess.CompletedProcess:
    return subprocess.run(
        cmd,
        check=check,
        cwd=str(cwd) if cwd else None,
        capture_output=capture,
        text=True,
    )


def quantile(sorted_vals, q: float):
    if not sorted_vals:
        return 0.0
    q = max(0.0, min(1.0, q))
    idx = round((len(sorted_vals) - 1) * q)
    return float(sorted_vals[int(idx)])


def sqlite_bench(db_path: Path, sql: str, concurrency: int, total: int, setup_sql: list[str]) -> Point:
    import sqlite3

    if db_path.exists():
        db_path.unlink()

    # One-time setup
    con = sqlite3.connect(str(db_path), check_same_thread=False)
    try:
        con.execute("PRAGMA journal_mode=WAL;")
        con.execute("PRAGMA synchronous=NORMAL;")
        for s in setup_sql:
            con.execute(s)
        con.commit()
    finally:
        con.close()

    is_select = sql.lstrip().upper().startswith("SELECT")

    # One connection per thread (thread-local). Avoid connect/close overhead on each query.
    tls = threading.local()

    def get_conn():
        cx = getattr(tls, "cx", None)
        if cx is None:
            cx = sqlite3.connect(str(db_path), check_same_thread=True)
            # Keep settings aligned with setup
            cx.execute("PRAGMA journal_mode=WAL;")
            cx.execute("PRAGMA synchronous=NORMAL;")
            tls.cx = cx
        return cx

    def one_call(i: int) -> float:
        t0 = time.perf_counter()
        cx = get_conn()
        cx.execute(sql)
        # For SELECT, commit() adds overhead and is unnecessary.
        if not is_select:
            cx.commit()
        return (time.perf_counter() - t0) * 1000.0

    lat_ms = []
    t_start = time.perf_counter()
    with ThreadPoolExecutor(max_workers=concurrency) as ex:
        futs = [ex.submit(one_call, i) for i in range(total)]
        for f in as_completed(futs):
            lat_ms.append(f.result())
    wall = time.perf_counter() - t_start
    lat_ms.sort()
    qps = total / wall if wall > 0 else 0.0

    return Point(
        system="sqlite",
        scenario="",
        concurrency=concurrency,
        qps=qps,
        p50_ms=quantile(lat_ms, 0.50),
        p95_ms=quantile(lat_ms, 0.95),
        p99_ms=quantile(lat_ms, 0.99),
    )


def rustdb_bench(repo_root: Path, cert_path: Path, addr: str, server_name: str, sql: str, concurrency: int, total: int) -> Point:
    exe = repo_root / "target" / "debug" / ("rustdb_load.exe" if os.name == "nt" else "rustdb_load")
    if not exe.exists():
        raise RuntimeError(f"rustdb_load not built at {exe}")

    # Allow toggling connection strategy to make QUIC results comparable to real clients.
    # shared: one QUIC connection (many streams); per-worker: one connection per worker.
    rustdb_mode = os.environ.get("RUSTDB_CONNECTION_MODE", "shared")

    cmd = [
        str(exe),
        "--addr",
        addr,
        "--cert",
        str(cert_path),
        "--server-name",
        server_name,
        "--concurrency",
        str(concurrency),
        "--queries",
        str(total),
        "--sql",
        sql,
        "--connection-mode",
        rustdb_mode,
        "--json",
    ]

    try:
        cp = run(cmd, check=True, capture=True, cwd=repo_root)
    except subprocess.CalledProcessError as e:
        out = (e.stdout or "").strip()
        err = (e.stderr or "").strip()
        raise RuntimeError(
            "rustdb_load failed\n"
            f"cmd: {' '.join(cmd)}\n"
            f"exit: {e.returncode}\n"
            f"stdout:\n{out}\n\nstderr:\n{err}\n"
        ) from None

    line = cp.stdout.strip().splitlines()[-1].strip()
    try:
        data = json.loads(line)
    except Exception as e:
        raise RuntimeError(f"failed to parse rustdb_load JSON: {e}\nstdout:\n{cp.stdout}\nstderr:\n{cp.stderr}")

    return Point(
        system="rustdb",
        scenario="",
        concurrency=concurrency,
        qps=float(data["qps"]),
        p50_ms=float(data["p50_us"]) / 1000.0,
        p95_ms=float(data["p95_us"]) / 1000.0,
        p99_ms=float(data["p99_us"]) / 1000.0,
    )


def plot(points: list[Point], out_png: Path):
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    scenarios = sorted(set(p.scenario for p in points))
    systems = ["rustdb", "sqlite"]

    fig, axes = plt.subplots(len(scenarios), 2, figsize=(12, 4 * len(scenarios)))
    if len(scenarios) == 1:
        axes = [axes]  # normalize

    for r, sc in enumerate(scenarios):
        for c, metric in enumerate(["qps", "p95_ms"]):
            ax = axes[r][c]
            ax.set_title(f"{sc} — {metric}")
            ax.set_xlabel("concurrency")
            ax.set_ylabel(metric)
            for sysname in systems:
                pts = [p for p in points if p.scenario == sc and p.system == sysname]
                pts.sort(key=lambda p: p.concurrency)
                xs = [p.concurrency for p in pts]
                ys = [getattr(p, metric) for p in pts]
                ax.plot(xs, ys, marker="o", label=sysname)
            ax.grid(True, alpha=0.3)
            ax.legend()

    fig.tight_layout()
    fig.savefig(out_png)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--addr", default="127.0.0.1:15432")
    ap.add_argument("--server-name", default="localhost")
    ap.add_argument("--cert", required=True)
    ap.add_argument("--concurrency", default="1,8,32,128")
    ap.add_argument("--queries", type=int, default=5000)
    ap.add_argument(
        "--rustdb-connection-mode",
        default="shared",
        choices=["shared", "per-worker"],
        help="QUIC connection mode for RustDB load generator.",
    )
    args = ap.parse_args()

    repo_root = Path(__file__).resolve().parents[1]
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    cert_path = Path(args.cert)
    conc = [int(x.strip()) for x in args.concurrency.split(",") if x.strip()]
    os.environ["RUSTDB_CONNECTION_MODE"] = args.rustdb_connection_mode

    scenarios = [
        ("select_literal", "SELECT 1", [], "SELECT 1"),
        ("select_table", "SELECT a FROM bench_t WHERE a = 1", ["CREATE TABLE bench_t (a INTEGER)", "INSERT INTO bench_t (a) VALUES (1)"], "SELECT 1"),
    ]

    points: list[Point] = []

    # SQLite: for select_literal we still execute a SQL statement against sqlite (SELECT 1).
    for name, sqlite_sql, setup_sql, rustdb_sql in scenarios:
        # RustDB side: `rustdb_load` needs the workload SQL.
        rustdb_workload = rustdb_sql if name == "select_literal" else "SELECT a FROM bench_t WHERE a = 1"
        for c in conc:
            p = rustdb_bench(
                repo_root,
                cert_path,
                args.addr,
                args.server_name,
                rustdb_workload,
                c,
                args.queries,
            )
            p.scenario = name
            points.append(p)

        for c in conc:
            db_path = out_dir / f"sqlite_{name}_{c}.db"
            p2 = sqlite_bench(db_path, sqlite_sql, c, args.queries, setup_sql)
            p2.scenario = name
            points.append(p2)

    # Write CSV
    csv_path = out_dir / "bench.csv"
    with csv_path.open("w", encoding="utf-8") as f:
        f.write("system,scenario,concurrency,qps,p50_ms,p95_ms,p99_ms\n")
        for p in points:
            f.write(f"{p.system},{p.scenario},{p.concurrency},{p.qps:.3f},{p.p50_ms:.3f},{p.p95_ms:.3f},{p.p99_ms:.3f}\n")

    # Markdown summary
    md_path = out_dir / "bench.md"
    by_scenario = {}
    for p in points:
        by_scenario.setdefault(p.scenario, []).append(p)

    with md_path.open("w", encoding="utf-8") as f:
        f.write("## SQLite vs RustDB benchmark (smoke)\n\n")
        f.write(f"- queries per point: **{args.queries}**\n")
        f.write(f"- concurrency: **{', '.join(map(str, conc))}**\n\n")
        for sc, pts in by_scenario.items():
            f.write(f"### {sc}\n\n")
            f.write("| system | concurrency | qps | p50 (ms) | p95 (ms) | p99 (ms) |\n")
            f.write("|---|---:|---:|---:|---:|---:|\n")
            for sysname in ["rustdb", "sqlite"]:
                rows = [p for p in pts if p.system == sysname]
                rows.sort(key=lambda p: p.concurrency)
                for p in rows:
                    f.write(f"| {p.system} | {p.concurrency} | {p.qps:.1f} | {p.p50_ms:.3f} | {p.p95_ms:.3f} | {p.p99_ms:.3f} |\n")
            f.write("\n")

        f.write("### Graphs\n\n")
        f.write("- `bench.png` contains QPS and p95 latency vs concurrency for each scenario.\n")

    # Plot
    png_path = out_dir / "bench.png"
    plot(points, png_path)

    print(f"Wrote: {md_path}")
    print(f"Wrote: {csv_path}")
    print(f"Wrote: {png_path}")


if __name__ == "__main__":
    sys.exit(main())


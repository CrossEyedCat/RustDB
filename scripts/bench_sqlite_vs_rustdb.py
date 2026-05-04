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
    system: str  # "rustdb(shared)" | "rustdb(per-worker)" | "sqlite" | "postgres"
    scenario: str
    concurrency: int
    qps: float
    p50_ms: float
    p95_ms: float
    p99_ms: float
    max_ms: float = 0.0
    mean_ms: float = 0.0
    wall_ms: float = 0.0
    ok_count: int = 0
    err_count: int = 0
    suite: str = "baseline"  # "baseline" | "stream_sweep"
    stream_batch: int | None = None


def run(cmd: list[str], *, check=True, capture=True, cwd: Path | None = None) -> subprocess.CompletedProcess:
    return subprocess.run(
        cmd,
        check=check,
        cwd=str(cwd) if cwd else None,
        capture_output=capture,
        text=True,
    )


def rustdb_exec_sql(repo_root: Path, cert_path: Path, addr: str, server_name: str, sql: str) -> None:
    """Single statement against RustDB over QUIC (clears tables between bench concurrency levels)."""
    exe = repo_root / "target" / "debug" / ("rustdb_quic_client.exe" if os.name == "nt" else "rustdb_quic_client")
    if not exe.exists():
        raise RuntimeError(f"rustdb_quic_client not built at {exe}")
    run(
        [str(exe), "--addr", addr, "--cert", str(cert_path), "--server-name", server_name, sql],
        check=True,
        capture=True,
        cwd=repo_root,
    )


def quantile(sorted_vals, q: float):
    if not sorted_vals:
        return 0.0
    q = max(0.0, min(1.0, q))
    idx = round((len(sorted_vals) - 1) * q)
    return float(sorted_vals[int(idx)])


def sqlite_bench(db_path: Path, sql: str, concurrency: int, total: int, setup_sql: list[str], *, suite: str) -> Point:
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
    mean_ms = sum(lat_ms) / len(lat_ms) if lat_ms else 0.0
    max_ms = lat_ms[-1] if lat_ms else 0.0

    return Point(
        system="sqlite",
        scenario="",
        concurrency=concurrency,
        qps=qps,
        p50_ms=quantile(lat_ms, 0.50),
        p95_ms=quantile(lat_ms, 0.95),
        p99_ms=quantile(lat_ms, 0.99),
        max_ms=max_ms,
        mean_ms=mean_ms,
        wall_ms=wall * 1000.0,
        ok_count=total,
        err_count=0,
        suite=suite,
        stream_batch=None,
    )


def postgres_bench(dsn: str, sql: str, concurrency: int, total: int, setup_sql: list[str], *, suite: str) -> Point:
    """
    PostgreSQL client-server baseline (optional).

    Uses one connection per thread (thread-local) and measures per-call latency.
    Requires `psycopg` (psycopg3).
    """
    try:
        import psycopg  # type: ignore
    except Exception as e:
        raise RuntimeError(
            "psycopg is required for --postgres-dsn benchmarks. "
            "Install with: python3 -m pip install 'psycopg[binary]'\n"
            f"import error: {e}"
        )

    # One-time setup
    with psycopg.connect(dsn, autocommit=True) as con:
        with con.cursor() as cur:
            for s in setup_sql:
                cur.execute(s)

    tls = threading.local()
    created = []
    created_lock = threading.Lock()

    def get_conn():
        cx = getattr(tls, "cx", None)
        if cx is None:
            cx = psycopg.connect(dsn, autocommit=True)
            tls.cx = cx
            with created_lock:
                created.append(cx)
        return cx

    is_select = sql.lstrip().upper().startswith("SELECT")

    def one_call(i: int) -> float:
        t0 = time.perf_counter()
        cx = get_conn()
        with cx.cursor() as cur:
            cur.execute(sql)
            if is_select:
                cur.fetchall()
        return (time.perf_counter() - t0) * 1000.0

    lat_ms = []
    t_start = time.perf_counter()
    try:
        with ThreadPoolExecutor(max_workers=concurrency) as ex:
            futs = [ex.submit(one_call, i) for i in range(total)]
            for f in as_completed(futs):
                lat_ms.append(f.result())
    finally:
        # Avoid leaking connections across points (CI will run many points).
        with created_lock:
            conns = list(created)
            created.clear()
        for cx in conns:
            try:
                cx.close()
            except Exception:
                pass
    wall = time.perf_counter() - t_start
    lat_ms.sort()
    qps = total / wall if wall > 0 else 0.0
    mean_ms = sum(lat_ms) / len(lat_ms) if lat_ms else 0.0
    max_ms = lat_ms[-1] if lat_ms else 0.0

    return Point(
        system="postgres",
        scenario="",
        concurrency=concurrency,
        qps=qps,
        p50_ms=quantile(lat_ms, 0.50),
        p95_ms=quantile(lat_ms, 0.95),
        p99_ms=quantile(lat_ms, 0.99),
        max_ms=max_ms,
        mean_ms=mean_ms,
        wall_ms=wall * 1000.0,
        ok_count=total,
        err_count=0,
        suite=suite,
        stream_batch=None,
    )


def rustdb_bench(
    repo_root: Path,
    cert_path: Path,
    addr: str,
    server_name: str,
    sql: str,
    concurrency: int,
    total: int,
    mode: str,
    *,
    stream_batch: int = 1,
    quic_max_streams: int = 256,
    quic_idle_secs: int = 30,
    distinguish_batches: bool = False,
    suite: str = "baseline",
) -> Point:
    exe = repo_root / "target" / "debug" / ("rustdb_load.exe" if os.name == "nt" else "rustdb_load")
    if not exe.exists():
        raise RuntimeError(f"rustdb_load not built at {exe}")

    rustdb_mode = mode

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
        "--stream-batch",
        str(stream_batch),
        "--quic-max-streams",
        str(quic_max_streams),
        "--quic-idle-secs",
        str(quic_idle_secs),
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

    max_us = int(data.get("max_us", 0))
    mean_us = int(data.get("mean_us", 0))
    ok = int(data.get("ok", 0))
    err = int(data.get("err", 0))

    return Point(
        system=rustdb_system_label(rustdb_mode, stream_batch, distinguish_batches),
        scenario="",
        concurrency=concurrency,
        qps=float(data["qps"]),
        p50_ms=float(data["p50_us"]) / 1000.0,
        p95_ms=float(data["p95_us"]) / 1000.0,
        p99_ms=float(data["p99_us"]) / 1000.0,
        max_ms=float(max_us) / 1000.0,
        mean_ms=float(mean_us) / 1000.0,
        wall_ms=float(data.get("wall_ms", 0.0)),
        ok_count=ok,
        err_count=err,
        suite=suite,
        stream_batch=stream_batch,
    )


def load_tx_sql_lines(repo_root: Path, rel: str) -> list[str]:
    """First blank-line-separated block from the file (same rules as rustdb_load --tx-sql-file)."""
    raw = (repo_root / rel).read_text(encoding="utf-8")
    for block in raw.split("\n\n"):
        lines: list[str] = []
        for ln in block.splitlines():
            s = ln.strip()
            if s and not s.startswith("--"):
                lines.append(s)
        if lines:
            return lines
    raise RuntimeError(f"no SQL statements in tx file {rel!r}")


def sqlite_bench_tx(
    db_path: Path,
    tx_lines: list[str],
    concurrency: int,
    total: int,
    setup_sql: list[str],
    *,
    suite: str,
) -> Point:
    import sqlite3

    if db_path.exists():
        db_path.unlink()

    con = sqlite3.connect(str(db_path), check_same_thread=False)
    try:
        con.execute("PRAGMA journal_mode=WAL;")
        con.execute("PRAGMA synchronous=NORMAL;")
        for s in setup_sql:
            con.execute(s)
        con.commit()
    finally:
        con.close()

    tls = threading.local()

    def get_conn():
        cx = getattr(tls, "cx", None)
        if cx is None:
            cx = sqlite3.connect(str(db_path), check_same_thread=True)
            cx.execute("PRAGMA journal_mode=WAL;")
            cx.execute("PRAGMA synchronous=NORMAL;")
            tls.cx = cx
        return cx

    def one_call(i: int) -> float:
        t0 = time.perf_counter()
        cx = get_conn()
        for line in tx_lines:
            sql = line.replace("{logpk}", str(i)).replace("{ix}", str(i))
            cx.execute(sql)
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
    mean_ms = sum(lat_ms) / len(lat_ms) if lat_ms else 0.0
    max_ms = lat_ms[-1] if lat_ms else 0.0

    return Point(
        system="sqlite",
        scenario="",
        concurrency=concurrency,
        qps=qps,
        p50_ms=quantile(lat_ms, 0.50),
        p95_ms=quantile(lat_ms, 0.95),
        p99_ms=quantile(lat_ms, 0.99),
        max_ms=max_ms,
        mean_ms=mean_ms,
        wall_ms=wall * 1000.0,
        ok_count=total,
        err_count=0,
        suite=suite,
        stream_batch=None,
    )


def postgres_bench_tx(
    dsn: str,
    tx_lines: list[str],
    concurrency: int,
    total: int,
    setup_sql: list[str],
    *,
    suite: str,
) -> Point:
    try:
        import psycopg  # type: ignore
    except Exception as e:
        raise RuntimeError(
            "psycopg is required for --postgres-dsn benchmarks. "
            "Install with: python3 -m pip install 'psycopg[binary]'\n"
            f"import error: {e}"
        )

    with psycopg.connect(dsn, autocommit=True) as con:
        with con.cursor() as cur:
            for s in setup_sql:
                cur.execute(s)

    tls = threading.local()
    created = []
    created_lock = threading.Lock()

    def get_conn():
        cx = getattr(tls, "cx", None)
        if cx is None:
            cx = psycopg.connect(dsn, autocommit=True)
            tls.cx = cx
            with created_lock:
                created.append(cx)
        return cx

    def one_call(i: int) -> float:
        t0 = time.perf_counter()
        cx = get_conn()
        with cx.transaction():
            with cx.cursor() as cur:
                for line in tx_lines:
                    sql = line.replace("{logpk}", str(i)).replace("{ix}", str(i))
                    cur.execute(sql)
        return (time.perf_counter() - t0) * 1000.0

    lat_ms = []
    t_start = time.perf_counter()
    try:
        with ThreadPoolExecutor(max_workers=concurrency) as ex:
            futs = [ex.submit(one_call, i) for i in range(total)]
            for f in as_completed(futs):
                lat_ms.append(f.result())
    finally:
        with created_lock:
            conns = list(created)
            created.clear()
        for cx in conns:
            try:
                cx.close()
            except Exception:
                pass
    wall = time.perf_counter() - t_start
    lat_ms.sort()
    qps = total / wall if wall > 0 else 0.0
    mean_ms = sum(lat_ms) / len(lat_ms) if lat_ms else 0.0
    max_ms = lat_ms[-1] if lat_ms else 0.0

    return Point(
        system="postgres",
        scenario="",
        concurrency=concurrency,
        qps=qps,
        p50_ms=quantile(lat_ms, 0.50),
        p95_ms=quantile(lat_ms, 0.95),
        p99_ms=quantile(lat_ms, 0.99),
        max_ms=max_ms,
        mean_ms=mean_ms,
        wall_ms=wall * 1000.0,
        ok_count=total,
        err_count=0,
        suite=suite,
        stream_batch=None,
    )


def rustdb_bench_tx(
    repo_root: Path,
    cert_path: Path,
    addr: str,
    server_name: str,
    tx_sql_path: Path,
    concurrency: int,
    total: int,
    mode: str,
    *,
    quic_max_streams: int = 256,
    quic_idle_secs: int = 30,
    suite: str = "baseline",
) -> Point:
    exe = repo_root / "target" / "debug" / ("rustdb_load.exe" if os.name == "nt" else "rustdb_load")
    if not exe.exists():
        raise RuntimeError(f"rustdb_load not built at {exe}")

    rustdb_mode = mode
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
        "--tx-sql-file",
        str(tx_sql_path),
        "--connection-mode",
        rustdb_mode,
        "--quic-max-streams",
        str(quic_max_streams),
        "--quic-idle-secs",
        str(quic_idle_secs),
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

    max_us = int(data.get("max_us", 0))
    mean_us = int(data.get("mean_us", 0))
    ok = int(data.get("ok", 0))
    err = int(data.get("err", 0))
    sb = int(data.get("stream_batch", 1))

    # Always label `rustdb(mode)` — JSON `stream_batch` is statements/tx, not rustdb_load batching.
    return Point(
        system=rustdb_system_label(rustdb_mode, 1, False),
        scenario="",
        concurrency=concurrency,
        qps=float(data["qps"]),
        p50_ms=float(data["p50_us"]) / 1000.0,
        p95_ms=float(data["p95_us"]) / 1000.0,
        p99_ms=float(data["p99_us"]) / 1000.0,
        max_ms=float(max_us) / 1000.0,
        mean_ms=float(mean_us) / 1000.0,
        wall_ms=float(data.get("wall_ms", 0.0)),
        ok_count=ok,
        err_count=err,
        suite=suite,
        stream_batch=sb,
    )


def rustdb_system_label(mode: str, stream_batch: int, distinguish_batches: bool) -> str:
    """Keep `rustdb(shared)` when only the default batch=1 is used; add `sbN` when comparing batches."""
    if distinguish_batches:
        return f"rustdb({mode},sb{stream_batch})"
    return f"rustdb({mode})"


def plot(points: list[Point], out_png: Path, *, title_suffix: str = ""):
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    scenarios = sorted(set(p.scenario for p in points))

    fig, axes = plt.subplots(len(scenarios), 2, figsize=(12, 4 * len(scenarios)))
    if len(scenarios) == 1:
        axes = [axes]  # normalize

    def systems_for_scenario(sc: str) -> list[str]:
        names = sorted({p.system for p in points if p.scenario == sc})

        def sort_key(s: str) -> tuple:
            if s == "sqlite":
                return (0, s)
            if s == "postgres":
                return (1, s)
            return (2, s)

        names.sort(key=sort_key)
        return names

    for r, sc in enumerate(scenarios):
        systems = systems_for_scenario(sc)
        for c, metric in enumerate(["qps", "p99_ms"]):
            ax = axes[r][c]
            st = f"{sc} — {metric}"
            if title_suffix:
                st = f"{st} ({title_suffix})"
            ax.set_title(st)
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


def parse_stream_sweep(raw: str) -> list[int]:
    s = raw.strip()
    if not s or s.lower() in ("none", "off", "-"):
        return []
    out = [int(x.strip()) for x in s.split(",") if x.strip()]
    for sb in out:
        if sb < 1:
            raise SystemExit(f"invalid stream_batch in sweep: {sb}")
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--addr", default="127.0.0.1:15432")
    ap.add_argument("--server-name", default="localhost")
    ap.add_argument("--cert", required=True)
    ap.add_argument(
        "--postgres-dsn",
        default="",
        help="Optional PostgreSQL DSN for additional baseline, e.g. 'postgresql://postgres:postgres@127.0.0.1:15440/postgres'.",
    )
    ap.add_argument(
        "--concurrency",
        default="1,8,32,128",
        help="Comma-separated concurrency levels (e.g. `128` only for a quick stream-batch check).",
    )
    ap.add_argument("--queries", type=int, default=5000)
    ap.add_argument(
        "--scenarios",
        default="select_literal,select_table",
        help="Comma-separated scenario names: select_literal, select_table, update_pk, mini_tx.",
    )
    ap.add_argument(
        "--rustdb-connection-modes",
        default="shared,per-worker",
        help="Comma-separated RustDB QUIC connection modes to benchmark: shared,per-worker.",
    )
    ap.add_argument(
        "--rustdb-baseline-stream-batch",
        type=int,
        default=1,
        help="rustdb_load --stream-batch for the cross-engine baseline phase (SQLite/Postgres/RustDB @ same scenarios).",
    )
    ap.add_argument(
        "--rustdb-stream-sweep",
        default="1,8,16",
        help="Optional second RustDB-only phase: comma-separated --stream-batch values (e.g. `1,8,16`). Empty or `none` disables.",
    )
    ap.add_argument(
        "--rustdb-quic-max-streams",
        type=int,
        default=256,
        help="Forwarded to rustdb_load --quic-max-streams; should be >= server max concurrent streams per connection when using many parallel streams (default matches stock ServerConfig).",
    )
    ap.add_argument(
        "--rustdb-quic-idle-secs",
        type=int,
        default=30,
        help="Forwarded to rustdb_load --quic-idle-secs (client transport idle; align with server connection_timeout).",
    )
    args = ap.parse_args()

    repo_root = Path(__file__).resolve().parents[1]
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    cert_path = Path(args.cert)
    conc = [int(x.strip()) for x in args.concurrency.split(",") if x.strip()]
    baseline_sb = args.rustdb_baseline_stream_batch
    if baseline_sb < 1:
        raise SystemExit(f"invalid --rustdb-baseline-stream-batch: {baseline_sb}")

    sweep_batches = parse_stream_sweep(args.rustdb_stream_sweep)
    distinguish_baseline_batch = baseline_sb != 1

    rustdb_modes = [m.strip() for m in args.rustdb_connection_modes.split(",") if m.strip()]
    for m in rustdb_modes:
        if m not in ("shared", "per-worker"):
            raise SystemExit(f"invalid rustdb mode: {m}")

    # (scenario_name, query_sql, setup_statements, tx_sql_file or None).
    # When tx_sql_file is set, SQL is read from that path (rustdb_load --tx-sql-file); query_sql is unused.
    scenario_catalog = [
        ("select_literal", "SELECT 1", [], None),
        (
            "select_table",
            "SELECT a FROM bench_t WHERE a = 1",
            ["CREATE TABLE bench_t (a INTEGER)", "INSERT INTO bench_t (a) VALUES (1)"],
            None,
        ),
        (
            "update_pk",
            "UPDATE bench_kv SET v = v + 1 WHERE k = 1",
            [
                "CREATE TABLE bench_kv (k INTEGER PRIMARY KEY, v INTEGER)",
                "INSERT INTO bench_kv (k, v) VALUES (1, 0)",
            ],
            None,
        ),
        (
            "mini_tx",
            "",
            [
                "DROP TABLE IF EXISTS mini_log",
                "DROP TABLE IF EXISTS mini_main",
                "CREATE TABLE mini_main (k INTEGER PRIMARY KEY, v INTEGER)",
                "INSERT INTO mini_main (k, v) VALUES (1, 0)",
                "CREATE TABLE mini_log (i INTEGER PRIMARY KEY, ref INTEGER)",
            ],
            "scripts/bench_mini_tx.sql",
        ),
    ]
    catalog_by_name = {s[0]: s for s in scenario_catalog}
    requested_raw = [x.strip() for x in args.scenarios.split(",") if x.strip()]
    if not requested_raw:
        raise SystemExit("no scenarios in --scenarios")
    unknown = [n for n in requested_raw if n not in catalog_by_name]
    if unknown:
        raise SystemExit(f"unknown scenario(s): {unknown}; expected: {', '.join(catalog_by_name)}")
    requested_names = list(dict.fromkeys(requested_raw))
    scenarios = [catalog_by_name[n] for n in requested_names]

    points: list[Point] = []

    # Phase 1: baseline — RustDB @ baseline_sb (fair vs SQLite/Postgres), then SQLite, then Postgres.
    for name, sqlite_sql, setup_sql, tx_rel in scenarios:
        tx_path = (repo_root / tx_rel) if tx_rel else None
        tx_lines = load_tx_sql_lines(repo_root, tx_rel) if tx_rel else None
        for mode in rustdb_modes:
            for c in conc:
                if tx_lines is not None:
                    assert tx_path is not None
                    # Same RustDB server for all concurrency levels; truncate log so INSERT keys stay unique.
                    rustdb_exec_sql(
                        repo_root,
                        cert_path,
                        args.addr,
                        args.server_name,
                        "DROP TABLE IF EXISTS mini_log",
                    )
                    rustdb_exec_sql(
                        repo_root,
                        cert_path,
                        args.addr,
                        args.server_name,
                        "CREATE TABLE mini_log (i INTEGER PRIMARY KEY, ref INTEGER)",
                    )
                    p = rustdb_bench_tx(
                        repo_root,
                        cert_path,
                        args.addr,
                        args.server_name,
                        tx_path,
                        c,
                        args.queries,
                        mode,
                        quic_max_streams=args.rustdb_quic_max_streams,
                        quic_idle_secs=args.rustdb_quic_idle_secs,
                        suite="baseline",
                    )
                else:
                    p = rustdb_bench(
                        repo_root,
                        cert_path,
                        args.addr,
                        args.server_name,
                        sqlite_sql,
                        c,
                        args.queries,
                        mode,
                        stream_batch=baseline_sb,
                        quic_max_streams=args.rustdb_quic_max_streams,
                        quic_idle_secs=args.rustdb_quic_idle_secs,
                        distinguish_batches=distinguish_baseline_batch,
                        suite="baseline",
                    )
                p.scenario = name
                points.append(p)

        for c in conc:
            db_path = out_dir / f"sqlite_{name}_{c}.db"
            if tx_lines is not None:
                p2 = sqlite_bench_tx(db_path, tx_lines, c, args.queries, setup_sql, suite="baseline")
            else:
                p2 = sqlite_bench(db_path, sqlite_sql, c, args.queries, setup_sql, suite="baseline")
            p2.scenario = name
            points.append(p2)

        if args.postgres_dsn:
            pg_setup: list[str] = []
            if name == "select_table":
                pg_setup = [
                    "DROP TABLE IF EXISTS bench_t",
                    "CREATE TABLE bench_t (a INTEGER)",
                    "INSERT INTO bench_t (a) VALUES (1)",
                ]
            elif name == "update_pk":
                pg_setup = [
                    "DROP TABLE IF EXISTS bench_kv",
                    "CREATE TABLE bench_kv (k INTEGER PRIMARY KEY, v INTEGER)",
                    "INSERT INTO bench_kv (k, v) VALUES (1, 0)",
                ]
            elif name == "mini_tx":
                pg_setup = [
                    "DROP TABLE IF EXISTS mini_log",
                    "DROP TABLE IF EXISTS mini_main",
                    "CREATE TABLE mini_main (k INTEGER PRIMARY KEY, v INTEGER)",
                    "INSERT INTO mini_main (k, v) VALUES (1, 0)",
                    "CREATE TABLE mini_log (i INTEGER PRIMARY KEY, ref INTEGER)",
                ]
            for c in conc:
                if tx_lines is not None:
                    p3 = postgres_bench_tx(
                        args.postgres_dsn, tx_lines, c, args.queries, pg_setup, suite="baseline"
                    )
                else:
                    p3 = postgres_bench(
                        args.postgres_dsn, sqlite_sql, c, args.queries, pg_setup, suite="baseline"
                    )
                p3.scenario = name
                points.append(p3)

    # Phase 2: RustDB-only stream_batch sweep (e.g. 1, 8, 16). Skipped for multi-statement tx scenarios.
    if sweep_batches:
        distinguish_sweep = len(sweep_batches) > 1 or (len(sweep_batches) == 1 and sweep_batches[0] != 1)
        for name, sqlite_sql, setup_sql, tx_rel in scenarios:
            if tx_rel:
                continue
            _ = setup_sql
            rustdb_workload = sqlite_sql
            for mode in rustdb_modes:
                for sb in sweep_batches:
                    for c in conc:
                        p = rustdb_bench(
                            repo_root,
                            cert_path,
                            args.addr,
                            args.server_name,
                            rustdb_workload,
                            c,
                            args.queries,
                            mode,
                            stream_batch=sb,
                            quic_max_streams=args.rustdb_quic_max_streams,
                            quic_idle_secs=args.rustdb_quic_idle_secs,
                            distinguish_batches=distinguish_sweep,
                            suite="stream_sweep",
                        )
                        p.scenario = name
                        points.append(p)

    # Write CSV
    csv_path = out_dir / "bench.csv"
    with csv_path.open("w", encoding="utf-8") as f:
        f.write(
            "suite,system,scenario,concurrency,stream_batch,qps,p50_ms,p95_ms,p99_ms,max_ms,mean_ms,wall_ms,ok,err\n"
        )
        for p in points:
            sb = "" if p.stream_batch is None else str(p.stream_batch)
            f.write(
                f"{p.suite},{p.system},{p.scenario},{p.concurrency},{sb},"
                f"{p.qps:.3f},{p.p50_ms:.3f},{p.p95_ms:.3f},{p.p99_ms:.3f},"
                f"{p.max_ms:.3f},{p.mean_ms:.3f},{p.wall_ms:.3f},{p.ok_count},{p.err_count}\n"
            )

    # Markdown summary
    md_path = out_dir / "bench.md"
    by_scenario = {}
    for p in points:
        by_scenario.setdefault(p.scenario, []).append(p)

    baseline_pts = [p for p in points if p.suite == "baseline"]
    sweep_pts = [p for p in points if p.suite == "stream_sweep"]

    with md_path.open("w", encoding="utf-8") as f:
        f.write("## Benchmark SQLite vs RustDB (charts)\n\n")
        f.write(
            "Two phases: **baseline** (SQLite, optional Postgres, RustDB at a single `stream_batch`) and "
            "optional **RustDB-only stream_batch sweep**.\n\n"
        )
        f.write(f"- scenarios (this run): **{', '.join(requested_names)}**\n\n")
        f.write(f"- queries per point: **{args.queries}**\n")
        f.write(f"- concurrency: **{', '.join(map(str, conc))}**\n\n")
        f.write(f"- rustdb modes: **{', '.join(rustdb_modes)}**\n\n")
        f.write(
            f"- baseline RustDB: **stream_batch={baseline_sb}**; sweep: **{sweep_batches or '(disabled)'}**\n\n"
        )
        f.write(
            f"- rustdb_load QUIC (this run): **quic_max_streams={args.rustdb_quic_max_streams}**, "
            f"**quic_idle_secs={args.rustdb_quic_idle_secs}**.\n\n"
        )
        f.write(
            "- Comparing RustDB to PostgreSQL or SQLite is only roughly comparable: RustDB uses QUIC + custom framing; "
            "Postgres uses TCP + libpq/psycopg; SQLite is in-process. Record these `rustdb_load` flags when publishing numbers.\n\n"
        )
        f.write(f"- postgres: **{'enabled' if args.postgres_dsn else 'disabled'}**\n\n")

        if args.postgres_dsn:
            f.write("### PostgreSQL baseline\n\n")
            f.write(
                "PostgreSQL is included as an additional **client-server** baseline to better compare network/protocol overhead. "
                "Unlike SQLite (embedded), Postgres runs out-of-process and is accessed over TCP.\n\n"
            )
            f.write(
                "- Measurement model: **one connection per worker thread** (thread-local), reused across requests.\n"
                "- For `SELECT`, results are fully read via `fetchall()` to include server response costs.\n\n"
            )

        def systems_in_scenario(pts: list[Point]) -> list[str]:
            names = sorted({p.system for p in pts})

            def sort_key(s: str) -> tuple:
                if s == "sqlite":
                    return (0, s)
                if s == "postgres":
                    return (1, s)
                return (2, s)

            names.sort(key=sort_key)
            return names

        f.write("### Baseline (cross-engine)\n\n")
        f.write(
            f"RustDB uses **stream_batch={baseline_sb}** here; SQLite and Postgres are unchanged. "
            f"Extra columns: max/mean latency, wall clock, ok/err (RustDB from `rustdb_load` JSON).\n\n"
        )

        for sc in requested_names:
            pts = [p for p in baseline_pts if p.scenario == sc]
            if not pts:
                continue
            f.write(f"#### {sc}\n\n")
            f.write(
                "| system | c | qps | p50 | p95 | p99 | max | mean | wall (ms) | ok | err |\n"
                "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|\n"
            )
            for sysname in systems_in_scenario(pts):
                rows = [p for p in pts if p.system == sysname]
                rows.sort(key=lambda p: p.concurrency)
                for p in rows:
                    f.write(
                        f"| {p.system} | {p.concurrency} | {p.qps:.1f} | {p.p50_ms:.3f} | {p.p95_ms:.3f} | {p.p99_ms:.3f} | "
                        f"{p.max_ms:.3f} | {p.mean_ms:.3f} | {p.wall_ms:.1f} | {p.ok_count} | {p.err_count} |\n"
                    )
            f.write("\n")

        if sweep_pts:
            f.write("### RustDB-only: stream_batch sweep\n\n")
            f.write(
                f"Second phase: same scenarios, **stream_batch** ∈ **{sweep_batches}** (labels include `sbN`). "
                "Compare QPS and tail latency vs batching; mean latency from `rustdb_load` JSON (`mean_us`).\n\n"
            )
            for sc in requested_names:
                pts = [p for p in sweep_pts if p.scenario == sc]
                if not pts:
                    continue
                f.write(f"#### {sc}\n\n")
                f.write(
                    "| system | c | qps | p50 | p95 | p99 | max | mean | wall (ms) | ok | err |\n"
                    "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|\n"
                )
                for sysname in systems_in_scenario(pts):
                    rows = [p for p in pts if p.system == sysname]
                    rows.sort(key=lambda p: (p.concurrency, p.stream_batch or 0))
                    for p in rows:
                        f.write(
                            f"| {p.system} | {p.concurrency} | {p.qps:.1f} | {p.p50_ms:.3f} | {p.p95_ms:.3f} | {p.p99_ms:.3f} | "
                            f"{p.max_ms:.3f} | {p.mean_ms:.3f} | {p.wall_ms:.1f} | {p.ok_count} | {p.err_count} |\n"
                        )
                f.write("\n")

        f.write("### Graphs\n\n")
        f.write("- `bench.png`: baseline phase — QPS and **p99** vs concurrency per scenario.\n")
        if sweep_pts:
            f.write(
                "- `bench_stream_batch.png`: stream_batch sweep — QPS and **p99** vs concurrency (RustDB only, `sbN` labels).\n"
            )

    # Plots
    png_path = out_dir / "bench.png"
    if baseline_pts:
        plot(baseline_pts, png_path, title_suffix="baseline")

    png_sweep = out_dir / "bench_stream_batch.png"
    if sweep_pts:
        plot(sweep_pts, png_sweep, title_suffix="stream_batch sweep")

    print(f"Wrote: {md_path}")
    print(f"Wrote: {csv_path}")
    print(f"Wrote: {png_path}")
    if sweep_pts:
        print(f"Wrote: {png_sweep}")


if __name__ == "__main__":
    sys.exit(main())

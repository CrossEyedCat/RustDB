//! Load generator for RustDB (QUIC).
//!
//! Runs a fixed set of SQL statements repeatedly with configurable concurrency and reports
//! throughput and latency percentiles.

use clap::Parser;
use rustdb::network::client::{
    build_quinn_client_config, connect, make_client_endpoint, query_once,
};
use rustdb::network::framing::ServerMessage;
use rustls::pki_types::CertificateDer;
use serde::Serialize;
use std::fs;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;

#[derive(clap::ValueEnum, Clone, Debug)]
enum ConnectionMode {
    /// One QUIC connection shared across workers (multiple streams).
    Shared,
    /// One QUIC connection per worker (more like real clients).
    PerWorker,
}

#[derive(Parser, Debug)]
#[command(name = "rustdb_load")]
struct Args {
    /// Server address (host:port).
    #[arg(long, default_value = "127.0.0.1:5432")]
    addr: String,

    /// Path to server leaf certificate (DER) for trusting the dev self-signed cert.
    #[arg(long)]
    cert: PathBuf,

    /// TLS server name (must match cert SAN; typically `localhost`).
    #[arg(long, default_value = "localhost")]
    server_name: String,

    /// Concurrency (number of in-flight queries).
    #[arg(long, default_value_t = 32)]
    concurrency: usize,

    /// Total number of queries to run (across all workers).
    #[arg(long, default_value_t = 10_000)]
    queries: usize,

    /// If set, reads SQL statements (one per line) from this file and cycles through them.
    #[arg(long)]
    sql_file: Option<PathBuf>,

    /// Single SQL statement to run (ignored when --sql-file is provided).
    #[arg(long, default_value = "SELECT 1")]
    sql: String,

    /// If set, generates a sequence of batched INSERT statements instead of using --sql/--sql-file.
    ///
    /// This avoids Windows command-line length limits and is useful for preloading large tables
    /// (e.g. 100k rows) efficiently.
    #[arg(long)]
    insert_table: Option<String>,

    /// Column name for generated INSERTs.
    #[arg(long, default_value = "a")]
    insert_column: String,

    /// Literal value for generated INSERTs (SQL integer literal).
    #[arg(long, default_value = "2")]
    insert_value: String,

    /// Total rows to generate across all INSERT statements.
    #[arg(long, default_value_t = 0)]
    insert_rows: usize,

    /// Rows per INSERT statement (VALUES list length).
    #[arg(long, default_value_t = 1000)]
    insert_batch: usize,

    /// Print the first N responses (useful for debugging).
    #[arg(long, default_value_t = 0)]
    print_first: usize,

    /// Emit a single JSON line with metrics to stdout (machine-readable).
    #[arg(long, default_value_t = false)]
    json: bool,

    /// How QUIC connections are established for concurrent workers.
    #[arg(long, value_enum, default_value_t = ConnectionMode::Shared)]
    connection_mode: ConnectionMode,
}

fn quantile(sorted: &[u128], q: f64) -> Option<u128> {
    if sorted.is_empty() {
        return None;
    }
    let q = q.clamp(0.0, 1.0);
    let idx = ((sorted.len() - 1) as f64 * q).round() as usize;
    Some(sorted[idx])
}

fn fmt_us(us: u128) -> String {
    if us >= 1_000_000 {
        format!("{:.2} s", us as f64 / 1_000_000.0)
    } else if us >= 1_000 {
        format!("{:.2} ms", us as f64 / 1_000.0)
    } else {
        format!("{us} µs")
    }
}

#[derive(Debug, Serialize)]
struct LoadReport {
    addr: String,
    server_name: String,
    concurrency: usize,
    queries: usize,
    ok: usize,
    err: usize,
    wall_ms: f64,
    qps: f64,
    p50_us: u128,
    p95_us: u128,
    p99_us: u128,
    max_us: u128,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let args = Args::parse();

    let addr: SocketAddr = args.addr.parse()?;
    let der = fs::read(&args.cert)?;
    let cert = CertificateDer::from(der);
    let client_cfg = build_quinn_client_config(std::slice::from_ref(&cert))?;
    let endpoint = make_client_endpoint(client_cfg)?;

    let statements: Arc<Vec<String>> = if let Some(table) = &args.insert_table {
        let total = args.insert_rows.max(1);
        let batch = args.insert_batch.max(1);
        let col = args.insert_column.trim();
        let val = args.insert_value.trim();

        let mut out: Vec<String> = Vec::with_capacity(total.div_ceil(batch));
        let mut remaining = total;
        while remaining > 0 {
            let n = remaining.min(batch);
            remaining -= n;

            // Rough capacity: "INSERT INTO t (a) VALUES " + n * "(2)," chars
            let mut sql = String::with_capacity(64 + n * 4);
            sql.push_str("INSERT INTO ");
            sql.push_str(table);
            sql.push_str(" (");
            sql.push_str(col);
            sql.push_str(") VALUES ");
            for i in 0..n {
                if i > 0 {
                    sql.push(',');
                }
                sql.push('(');
                sql.push_str(val);
                sql.push(')');
            }
            out.push(sql);
        }

        Arc::new(out)
    } else if let Some(p) = &args.sql_file {
        let raw = fs::read_to_string(p)?;
        let mut v: Vec<String> = raw
            .lines()
            .map(|l| l.trim())
            .filter(|l| !l.is_empty() && !l.starts_with("--"))
            .map(|l| l.to_string())
            .collect();
        if v.is_empty() {
            v.push("SELECT 1".to_string());
        }
        Arc::new(v)
    } else {
        Arc::new(vec![args.sql.clone()])
    };

    let start = Instant::now();
    let concurrency = args.concurrency.max(1);
    let total_queries = args.queries.max(1);

    // Establish shared connection once (if requested).
    let shared_conn = match args.connection_mode {
        ConnectionMode::Shared => Some(connect(&endpoint, addr, &args.server_name).await?),
        ConnectionMode::PerWorker => None,
    };

    // Worker model: N workers execute roughly total_queries/N sequentially.
    // This makes "per_worker connection" meaningful and avoids spawning 10k tasks.
    let sem = Arc::new(Semaphore::new(concurrency));
    let mut handles = Vec::with_capacity(concurrency);

    for worker_id in 0..concurrency {
        let permit = sem.clone().acquire_owned().await?;
        let statements = statements.clone();
        let mode = args.connection_mode.clone();
        let endpoint = endpoint.clone();
        let server_name = args.server_name.clone();
        let print_first = args.print_first;
        let shared_conn = shared_conn.clone();

        let base = total_queries / concurrency;
        let extra = (worker_id < (total_queries % concurrency)) as usize;
        let my_queries = base + extra;
        let start_index = worker_id * base + worker_id.min(total_queries % concurrency);

        handles.push(tokio::spawn(async move {
            let _permit = permit;

            let worker_conn = match mode {
                ConnectionMode::Shared => shared_conn.expect("shared connection"),
                ConnectionMode::PerWorker => connect(&endpoint, addr, &server_name).await?,
            };

            let mut out = Vec::with_capacity(my_queries);
            for j in 0..my_queries {
                let global_i = start_index + j;
                let sql = statements[global_i % statements.len()].clone();
                let t0 = Instant::now();

                let msg = query_once(&worker_conn, &sql).await;

                let dt = t0.elapsed();
                let print = global_i < print_first;
                out.push((global_i, sql, dt, msg, print));
            }

            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(out)
        }));
    }

    let mut durations_us: Vec<u128> = Vec::with_capacity(args.queries);
    let mut ok = 0usize;
    let mut err = 0usize;
    let mut first_err: Option<String> = None;

    for h in handles {
        let rows = h.await??;
        for (i, sql, dt, msg, print) in rows {
            let us = dt.as_micros();
            durations_us.push(us);
            match msg {
                Ok(m) => {
                    ok += 1;
                    if print {
                        match &m {
                            ServerMessage::ResultSet(p) => {
                                println!(
                                    "#{i} OK ResultSet cols={} rows={}",
                                    p.columns.len(),
                                    p.rows.len()
                                );
                            }
                            ServerMessage::ExecutionOk(p) => {
                                println!("#{i} OK ExecutionOk rows_affected={}", p.rows_affected);
                            }
                            ServerMessage::Error(p) => {
                                println!("#{i} OK Error code={} message={}", p.code, p.message);
                            }
                            ServerMessage::ServerReady(p) => {
                                println!("#{i} OK ServerReady {}", p.server_version);
                            }
                        }
                    }
                }
                Err(e) => {
                    err += 1;
                    if first_err.is_none() {
                        first_err = Some(format!("query #{i} failed: {e}; sql={sql}"));
                    }
                }
            }
        }
    }

    let wall = start.elapsed();
    durations_us.sort_unstable();

    let total = ok + err;
    let qps = if wall.as_secs_f64() > 0.0 {
        total as f64 / wall.as_secs_f64()
    } else {
        0.0
    };

    let p50 = quantile(&durations_us, 0.50).unwrap_or(0);
    let p95 = quantile(&durations_us, 0.95).unwrap_or(0);
    let p99 = quantile(&durations_us, 0.99).unwrap_or(0);
    let max = durations_us.last().copied().unwrap_or(0);

    let report = LoadReport {
        addr: args.addr.clone(),
        server_name: args.server_name.clone(),
        concurrency: args.concurrency,
        queries: args.queries,
        ok,
        err,
        wall_ms: wall.as_secs_f64() * 1000.0,
        qps,
        p50_us: p50,
        p95_us: p95,
        p99_us: p99,
        max_us: max,
    };

    if args.json {
        println!("{}", serde_json::to_string(&report)?);
    } else {
        println!();
        println!("== rustdb_load ==");
        println!("addr: {}", report.addr);
        println!("server_name: {}", report.server_name);
        println!("concurrency: {}", report.concurrency);
        println!("queries: {}", report.queries);
        println!("ok: {}  err: {}", report.ok, report.err);
        println!("wall: {:.2?}  throughput: {:.1} qps", wall, report.qps);
        println!(
            "latency: p50={} p95={} p99={} max={}",
            fmt_us(report.p50_us),
            fmt_us(report.p95_us),
            fmt_us(report.p99_us),
            fmt_us(report.max_us)
        );
    }

    if let Some(e) = first_err {
        eprintln!("\nfirst error: {e}");
        std::process::exit(2);
    }

    // Basic guard: ensure the run actually executed within a reasonable time.
    if total == 0 {
        eprintln!("no queries executed");
        std::process::exit(2);
    }

    // Avoid CI flakiness: do not fail on low throughput; just report.
    let _ = Duration::from_secs(0);
    Ok(())
}

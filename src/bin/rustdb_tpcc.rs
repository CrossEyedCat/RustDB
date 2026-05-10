//! Minimal TPC-C-ish throughput load generator for RustDB (QUIC).
//!
//! This is **not** a full TPC-C compliant implementation. It is a pragmatic CI benchmark:
//! - generates a mixed workload with OLTP-style read/write transactions
//! - reports throughput (txns/s) and a "tpmC" proxy based on New-Order transactions
//! - uses RustDB's QUIC client protocol directly (same framing as rustdb_load)

use clap::Parser;
use quinn::{Connection, RecvStream, SendStream};
use rustdb::network::client::{
    build_quinn_client_config_with_limits, connect, make_client_endpoint,
};
use rustdb::network::framing::{
    decode_server_frame_v1, encode_client_message_v1, ClientMessage, QueryPayload,
};
use rustdb::network::query_stream::read_application_frame_into;
use rustls::pki_types::CertificateDer;
use serde::Serialize;
use std::fs;
use std::io::Write;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;

const MAX_LATENCY_SAMPLES: usize = 200_000;

/// Must stay below the QUIC server's per-stream frame cap (`MAX_FRAMES_PER_STREAM` in
/// `src/network/query_stream.rs`, currently 1024).
const SERVER_MAX_FRAMES_PER_STREAM: usize = 1024;
const STREAM_FRAME_BUDGET: usize = SERVER_MAX_FRAMES_PER_STREAM - 64;

#[derive(Parser, Debug)]
#[command(name = "rustdb_tpcc")]
struct Args {
    /// Server address (host:port).
    #[arg(long, default_value = "127.0.0.1:5432")]
    addr: String,

    /// Path to server leaf certificate (DER).
    #[arg(long)]
    cert: PathBuf,

    /// TLS server name (must match cert SAN; typically `localhost`).
    #[arg(long, default_value = "localhost")]
    server_name: String,

    /// Concurrency (number of in-flight transactions).
    #[arg(long, default_value_t = 64)]
    concurrency: usize,

    /// Total transactions to execute (across all workers).
    #[arg(long, default_value_t = 5_000)]
    transactions: usize,

    /// Run the workload for this many seconds (overrides --transactions when set).
    #[arg(long)]
    duration_seconds: Option<u64>,

    /// Transaction mix as comma-separated weights, e.g. `new_order=0.45,payment=0.43,order_status=0.04,delivery=0.04,stock_level=0.04`.
    #[arg(
        long,
        default_value = "new_order=0.45,payment=0.43,order_status=0.04,delivery=0.04,stock_level=0.04"
    )]
    mix: String,

    /// Emit a single JSON line report.
    #[arg(long, default_value_t = false)]
    json: bool,

    /// QUIC max concurrent bidirectional streams.
    #[arg(long, default_value_t = 512)]
    quic_max_streams: usize,

    /// QUIC max idle timeout (seconds).
    #[arg(long, default_value_t = 30)]
    quic_idle_secs: u64,

    /// Timeout for establishing the initial QUIC connection (seconds).
    #[arg(long, default_value_t = 15)]
    connect_timeout_secs: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TxnKind {
    NewOrder,
    Payment,
    OrderStatus,
    Delivery,
    StockLevel,
}

#[derive(Clone, Debug)]
struct Mix {
    cumulative: Vec<(TxnKind, f64)>,
}

impl Mix {
    fn parse(s: &str) -> Result<Self, String> {
        let mut w = Vec::new();
        for part in s.split(',').map(|p| p.trim()).filter(|p| !p.is_empty()) {
            let (k, v) = part
                .split_once('=')
                .ok_or_else(|| format!("bad mix item: {part}"))?;
            let val: f64 = v.parse().map_err(|_| format!("bad weight: {part}"))?;
            let kind = match k.trim() {
                "new_order" => TxnKind::NewOrder,
                "payment" => TxnKind::Payment,
                "order_status" => TxnKind::OrderStatus,
                "delivery" => TxnKind::Delivery,
                "stock_level" => TxnKind::StockLevel,
                other => return Err(format!("unknown kind: {other}")),
            };
            if val < 0.0 {
                return Err(format!("negative weight: {part}"));
            }
            w.push((kind, val));
        }
        if w.is_empty() {
            return Err("empty mix".to_string());
        }
        let sum: f64 = w.iter().map(|(_, x)| *x).sum();
        if sum <= 0.0 {
            return Err("mix sum must be > 0".to_string());
        }
        let mut cum = 0.0;
        let mut cumulative = Vec::with_capacity(w.len());
        for (k, x) in w {
            cum += x / sum;
            cumulative.push((k, cum.min(1.0)));
        }
        Ok(Self { cumulative })
    }

    fn pick(&self, u: f64) -> TxnKind {
        for (k, c) in &self.cumulative {
            if u <= *c {
                return *k;
            }
        }
        self.cumulative
            .last()
            .map(|x| x.0)
            .unwrap_or(TxnKind::NewOrder)
    }
}

fn lcg_next(state: &mut u64) -> u64 {
    // LCG constants (Numerical Recipes)
    *state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
    *state
}

fn rand_f64_0_1(state: &mut u64) -> f64 {
    let x = lcg_next(state);
    // take top 53 bits
    let v = x >> 11;
    (v as f64) / ((1u64 << 53) as f64)
}

fn reservoir_sample_push(samples: &mut Vec<u128>, seen: &mut u64, rng: &mut u64, value: u128) {
    *seen = seen.wrapping_add(1);
    if samples.len() < MAX_LATENCY_SAMPLES {
        samples.push(value);
        return;
    }
    // Replace a random existing element with probability MAX/seen.
    let j = (lcg_next(rng) % (*seen).max(1)) as usize;
    if j < samples.len() {
        samples[j] = value;
    }
}

/// Runs one or more `Query` frames on an existing QUIC bidirectional stream (one server session).
/// Does not open/finish the stream; caller rotates streams to stay under the server's per-stream
/// frame limit and calls [`SendStream::finish`] when retiring a stream.
async fn run_sql_seq_on_bi(
    send: &mut SendStream,
    recv: &mut RecvStream,
    recv_buf: &mut Vec<u8>,
    sqls: &[String],
) -> Result<usize, Box<dyn std::error::Error + Send + Sync>> {
    let mut frames = 0usize;
    for sql in sqls {
        let frame =
            encode_client_message_v1(&ClientMessage::Query(QueryPayload { sql: sql.clone() }))?;
        send.write_all(&frame).await?;
        frames += 1;
        read_application_frame_into(recv, 64 * 1024 * 1024, recv_buf).await?;
        let msg = decode_server_frame_v1(recv_buf)?;
        // Treat server-side Error messages as failures.
        if let rustdb::network::framing::ServerMessage::Error(p) = msg {
            // Engine currently surfaces "record not found" as an error for some DELETE paths.
            // For this benchmark, treat that as an empty delete (0 rows affected).
            if p.code == 1001
                && p.message.to_ascii_lowercase().contains("record not found")
                && sql
                    .trim_start()
                    .to_ascii_lowercase()
                    .starts_with("delete from new_order")
            {
                continue;
            }

            // Best-effort cleanup: if we were inside an explicit transaction, try to roll it back
            // on the same stream before returning. This prevents leaking open transactions when the
            // client drops the stream early.
            let _ = async {
                let rb = encode_client_message_v1(&ClientMessage::Query(QueryPayload {
                    sql: "ROLLBACK".to_string(),
                }))?;
                send.write_all(&rb).await?;
                frames += 1;
                read_application_frame_into(recv, 64 * 1024 * 1024, recv_buf).await?;
                Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
            }
            .await;

            return Err(format!("server error: {}: {}", p.code, p.message).into());
        }
    }
    Ok(frames)
}

/// Reuses one QUIC bidirectional stream for many transactions (one OS-backed SQL session on the
/// server), rotating before the per-stream frame limit is hit.
async fn run_tpcc_transaction_with_stream_pool(
    conn: &Connection,
    send: &mut SendStream,
    recv: &mut RecvStream,
    recv_buf: &mut Vec<u8>,
    frames_used: &mut usize,
    sqls: &[String],
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let need = sqls.len().max(1);
    if *frames_used + need > STREAM_FRAME_BUDGET {
        send.finish()?;
        let (s, r) = conn.open_bi().await?;
        *send = s;
        *recv = r;
        *frames_used = 0;
    }
    match run_sql_seq_on_bi(send, recv, recv_buf, sqls).await {
        Ok(used) => {
            *frames_used += used;
            Ok(())
        }
        Err(e) => {
            // Unknown partial progress; retire this stream on the next transaction.
            *frames_used = STREAM_FRAME_BUDGET;
            Err(e)
        }
    }
}

fn txn_sql(kind: TxnKind, seed: u64, global_txn_id: u64) -> Vec<String> {
    // Small deterministic parameters.
    let mut st = seed ^ (global_txn_id.wrapping_mul(0x9E3779B97F4A7C15));
    let w_id = 1;
    let d_id = lcg_next(&mut st) % 5 + 1;
    let c_id = lcg_next(&mut st) % 5 + 1;
    let i_id = lcg_next(&mut st) % 5 + 1;
    let qty = lcg_next(&mut st) % 5 + 1;
    let amount = qty * 10;
    let o_id = global_txn_id;

    // Keep statements simple; RustDB engine may not support all SQL-92 features yet.
    match kind {
        TxnKind::NewOrder => vec![
            "BEGIN TRANSACTION".to_string(),
            // Keep the benchmark compatible with a minimal SQL parser: avoid arithmetic operators
            // in expressions (e.g. `col = col + 1`), which may not be supported.
            // Some engine paths additionally require UPDATE RHS values to be literals.
            format!(
                "UPDATE district SET d_next_o_id = 1 WHERE d_w_id = {w_id} AND d_id = {d_id}"
            ),
            format!(
                "INSERT INTO oorder (o_id, o_d_id, o_w_id, o_c_id, o_ol_cnt) VALUES ({o_id}, {d_id}, {w_id}, {c_id}, 1)"
            ),
            format!(
                "INSERT INTO new_order (no_o_id, no_d_id, no_w_id) VALUES ({o_id}, {d_id}, {w_id})"
            ),
            format!(
                "UPDATE stock SET s_qty = 100, s_ytd = 0, s_order_cnt = 0 WHERE s_w_id = {w_id} AND s_i_id = {i_id}"
            ),
            format!(
                "INSERT INTO order_line (ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_qty, ol_amount) VALUES ({o_id}, {d_id}, {w_id}, 1, {i_id}, {qty}, {amount})"
            ),
            "COMMIT".to_string(),
        ],
        TxnKind::Payment => vec![
            "BEGIN TRANSACTION".to_string(),
            format!(
                "UPDATE warehouse SET w_ytd = 0 WHERE w_id = {w_id}"
            ),
            format!(
                "UPDATE district SET d_ytd = 0 WHERE d_w_id = {w_id} AND d_id = {d_id}"
            ),
            format!(
                "UPDATE customer SET c_balance = 0 WHERE c_w_id = {w_id} AND c_d_id = {d_id} AND c_id = {c_id}"
            ),
            "COMMIT".to_string(),
        ],
        TxnKind::OrderStatus => vec![
            "BEGIN TRANSACTION".to_string(),
            format!(
                "SELECT * FROM oorder WHERE o_w_id = {w_id} AND o_d_id = {d_id} AND o_c_id = {c_id}"
            ),
            "COMMIT".to_string(),
        ],
        TxnKind::Delivery => vec![
            "BEGIN TRANSACTION".to_string(),
            // Simplified: delete one new_order row for a district.
            format!(
                "DELETE FROM new_order WHERE no_w_id = {w_id} AND no_d_id = {d_id}"
            ),
            "COMMIT".to_string(),
        ],
        TxnKind::StockLevel => vec![
            "BEGIN TRANSACTION".to_string(),
            format!(
                "SELECT * FROM stock WHERE s_w_id = {w_id} AND s_qty < 20"
            ),
            "COMMIT".to_string(),
        ],
    }
}

fn quantile_ms(sorted_us: &[u128], q: f64) -> f64 {
    if sorted_us.is_empty() {
        return 0.0;
    }
    let q = q.clamp(0.0, 1.0);
    let idx = ((sorted_us.len() - 1) as f64 * q).round() as usize;
    sorted_us[idx] as f64 / 1000.0
}

#[derive(Serialize)]
struct TpccReport {
    concurrency: usize,
    transactions: usize,
    elapsed_s: f64,
    txns_per_s: f64,
    new_orders: u64,
    #[serde(rename = "tpmC")]
    tpm_c: f64,
    p50_ms: f64,
    p95_ms: f64,
    p99_ms: f64,
    err: u64,
    mix: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let args = Args::parse();
    let mix = Mix::parse(&args.mix).map_err(|e| format!("invalid --mix: {e}"))?;

    let addr: SocketAddr = args.addr.parse()?;
    let der = fs::read(&args.cert)?;
    let cert = CertificateDer::from(der);
    let client_cfg = build_quinn_client_config_with_limits(
        std::slice::from_ref(&cert),
        args.quic_max_streams.max(args.concurrency),
        Duration::from_secs(args.quic_idle_secs),
    )?;
    let endpoint = make_client_endpoint(client_cfg)?;

    // Shared connection (single client) but multiple streams.
    let conn = tokio::time::timeout(
        Duration::from_secs(args.connect_timeout_secs.max(1)),
        connect(&endpoint, addr, &args.server_name),
    )
    .await
    .map_err(|_| format!("timeout connecting to {}", args.addr))??;

    let sem = Arc::new(Semaphore::new(args.concurrency.max(1)));
    let tx_total = args.transactions.max(1);
    let duration = args.duration_seconds;
    let deadline = duration.map(|s| Instant::now() + Duration::from_secs(s.max(1)));
    let global_txn_counter = Arc::new(AtomicU64::new(0));
    let new_orders = Arc::new(AtomicU64::new(0));
    let errors = Arc::new(AtomicU64::new(0));
    let printed_errors = Arc::new(AtomicU64::new(0));
    let print_limit: u64 = 50;

    let start = Instant::now();
    let mut handles = Vec::with_capacity(args.concurrency);

    for worker_id in 0..args.concurrency.max(1) {
        let permit = sem.clone().acquire_owned().await?;
        let conn = conn.clone();
        let mix = mix.clone();
        let global_txn_counter = global_txn_counter.clone();
        let new_orders = new_orders.clone();
        let errors = errors.clone();
        let printed_errors = printed_errors.clone();
        let mix_str = args.mix.clone();

        let base = tx_total / args.concurrency.max(1);
        let extra = (worker_id < (tx_total % args.concurrency.max(1))) as usize;
        let my_tx = base + extra;
        let start_index = worker_id * base + worker_id.min(tx_total % args.concurrency.max(1));

        handles.push(tokio::spawn(async move {
            let _permit = permit;
            let mut lat_us: Vec<u128> = Vec::with_capacity(my_tx);
            let seed = 0xC0FFEE_u64 ^ (worker_id as u64).wrapping_mul(0xA5A5A5A5A5A5A5A5);
            let mut seen: u64 = 0;
            let mut rng = seed ^ 0xD6E8FEB86659FD93;
            let mut completed: u64 = 0;

            let (mut send, mut recv) = conn.open_bi().await?;
            let mut recv_buf = Vec::new();
            let mut frames_used = 0usize;

            if let Some(dl) = deadline {
                while Instant::now() < dl {
                    let global_tx = global_txn_counter.fetch_add(1, Ordering::Relaxed);
                    let mut st = seed ^ global_tx.wrapping_mul(0xD1B54A32D192ED03);
                    let u = rand_f64_0_1(&mut st);
                    let kind = mix.pick(u);
                    if kind == TxnKind::NewOrder {
                        new_orders.fetch_add(1, Ordering::Relaxed);
                    }
                    let sqls = txn_sql(kind, seed, global_tx);
                    let t0 = Instant::now();
                    let res = run_tpcc_transaction_with_stream_pool(
                        &conn,
                        &mut send,
                        &mut recv,
                        &mut recv_buf,
                        &mut frames_used,
                        &sqls,
                    )
                    .await;
                    let dt = t0.elapsed();
                    reservoir_sample_push(&mut lat_us, &mut seen, &mut rng, dt.as_micros());
                    completed = completed.wrapping_add(1);
                    if res.is_err() {
                        errors.fetch_add(1, Ordering::Relaxed);
                        let n = printed_errors.fetch_add(1, Ordering::Relaxed);
                        if n < print_limit {
                            eprintln!(
                                "[tpcc] txn_error kind={kind:?} tx={global_tx} err={:?}",
                                res.err()
                            );
                        }
                    }
                }
                let _ = send.finish();
                Ok::<(Vec<u128>, String, u64), Box<dyn std::error::Error + Send + Sync>>((
                    lat_us, mix_str, completed,
                ))
            } else {
                for j in 0..my_tx {
                    let global_tx = (start_index + j) as u64;
                    let mut st = seed ^ global_tx.wrapping_mul(0xD1B54A32D192ED03);
                    let u = rand_f64_0_1(&mut st);
                    let kind = mix.pick(u);
                    if kind == TxnKind::NewOrder {
                        new_orders.fetch_add(1, Ordering::Relaxed);
                    }
                    let sqls = txn_sql(kind, seed, global_tx);
                    let t0 = Instant::now();
                    let res = run_tpcc_transaction_with_stream_pool(
                        &conn,
                        &mut send,
                        &mut recv,
                        &mut recv_buf,
                        &mut frames_used,
                        &sqls,
                    )
                    .await;
                    let dt = t0.elapsed();
                    reservoir_sample_push(&mut lat_us, &mut seen, &mut rng, dt.as_micros());
                    completed += 1;
                    if res.is_err() {
                        errors.fetch_add(1, Ordering::Relaxed);
                        let n = printed_errors.fetch_add(1, Ordering::Relaxed);
                        if n < print_limit {
                            eprintln!(
                                "[tpcc] txn_error kind={kind:?} tx={global_tx} err={:?}",
                                res.err()
                            );
                        }
                    }
                }
                let _ = send.finish();
                Ok::<(Vec<u128>, String, u64), Box<dyn std::error::Error + Send + Sync>>((
                    lat_us, mix_str, completed,
                ))
            }
        }));
    }

    let mut all_lat: Vec<u128> = Vec::with_capacity(tx_total);
    let mut mix_str = args.mix.clone();
    let mut total_done: u64 = 0;
    for h in handles {
        let (mut lat, mx, done) = h.await??;
        all_lat.append(&mut lat);
        mix_str = mx;
        total_done = total_done.wrapping_add(done);
    }

    let elapsed = start.elapsed().as_secs_f64().max(1e-9);
    all_lat.sort_unstable();
    let report_txns = if duration.is_some() {
        total_done.max(1) as usize
    } else {
        tx_total
    };
    let txns_per_s = (report_txns as f64) / elapsed;
    let no = new_orders.load(Ordering::Relaxed);
    let tpmc = (no as f64) / (elapsed / 60.0);

    let report = TpccReport {
        concurrency: args.concurrency.max(1),
        transactions: report_txns,
        elapsed_s: elapsed,
        txns_per_s,
        new_orders: no,
        tpm_c: tpmc,
        p50_ms: quantile_ms(&all_lat, 0.50),
        p95_ms: quantile_ms(&all_lat, 0.95),
        p99_ms: quantile_ms(&all_lat, 0.99),
        err: errors.load(Ordering::Relaxed),
        mix: mix_str,
    };

    if args.json {
        println!("{}", serde_json::to_string(&report)?);
        // Best-effort flush so CI still gets tpcc.json even if we exit non-zero.
        let _ = std::io::stdout().flush();
    } else {
        println!("== rustdb_tpcc ==");
        println!("concurrency: {}", report.concurrency);
        println!("transactions: {}", report.transactions);
        println!("elapsed_s: {:.3}", report.elapsed_s);
        println!("txns_per_s: {:.1}", report.txns_per_s);
        println!("new_orders: {}", report.new_orders);
        println!("tpmC: {:.1}", report.tpm_c);
        println!(
            "latency_ms: p50={:.2} p95={:.2} p99={:.2}",
            report.p50_ms, report.p95_ms, report.p99_ms
        );
        println!("err: {}", report.err);
        println!("mix: {}", report.mix);
    }

    if report.err > 0 {
        return Err(format!("workload completed with {} error(s)", report.err).into());
    }

    Ok(())
}

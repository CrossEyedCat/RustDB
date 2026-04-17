//! Minimal TPC-C-ish throughput load generator for RustDB (QUIC).
//!
//! This is **not** a full TPC-C compliant implementation. It is a pragmatic CI benchmark:
//! - generates a mixed workload with OLTP-style read/write transactions
//! - reports throughput (txns/s) and a "tpmC" proxy based on New-Order transactions
//! - uses RustDB's QUIC client protocol directly (same framing as rustdb_load)

use clap::Parser;
use quinn::Connection;
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
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;

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

async fn run_sql_seq_on_stream(
    conn: &Connection,
    sqls: &[String],
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let (mut send, mut recv) = conn.open_bi().await?;
    let mut recv_buf = Vec::new();
    for sql in sqls {
        let frame =
            encode_client_message_v1(&ClientMessage::Query(QueryPayload { sql: sql.clone() }))?;
        send.write_all(&frame).await?;
        read_application_frame_into(&mut recv, 64 * 1024 * 1024, &mut recv_buf).await?;
        let msg = decode_server_frame_v1(&recv_buf)?;
        // Treat server-side Error messages as failures.
        if let rustdb::network::framing::ServerMessage::Error(p) = msg {
            return Err(format!("server error: {}: {}", p.code, p.message).into());
        }
    }
    let _ = send.finish();
    Ok(())
}

fn txn_sql(kind: TxnKind, seed: u64, global_txn_id: u64) -> Vec<String> {
    // Small deterministic parameters.
    let mut st = seed ^ (global_txn_id.wrapping_mul(0x9E3779B97F4A7C15));
    let w_id = 1;
    let d_id = lcg_next(&mut st) % 5 + 1;
    let c_id = lcg_next(&mut st) % 5 + 1;
    let i_id = lcg_next(&mut st) % 5 + 1;
    let qty = lcg_next(&mut st) % 5 + 1;
    let o_id = global_txn_id;

    // Keep statements simple; RustDB engine may not support all SQL-92 features yet.
    match kind {
        TxnKind::NewOrder => vec![
            "BEGIN TRANSACTION".to_string(),
            // Advance district next order id (best-effort; no constraints).
            format!(
                "UPDATE district SET d_next_o_id = d_next_o_id + 1 WHERE d_w_id = {w_id} AND d_id = {d_id}"
            ),
            format!(
                "INSERT INTO oorder (o_id, o_d_id, o_w_id, o_c_id, o_ol_cnt) VALUES ({o_id}, {d_id}, {w_id}, {c_id}, 1)"
            ),
            format!(
                "INSERT INTO new_order (no_o_id, no_d_id, no_w_id) VALUES ({o_id}, {d_id}, {w_id})"
            ),
            format!(
                "UPDATE stock SET s_qty = s_qty - {qty}, s_ytd = s_ytd + {qty}, s_order_cnt = s_order_cnt + 1 WHERE s_w_id = {w_id} AND s_i_id = {i_id}"
            ),
            format!(
                "INSERT INTO order_line (ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_qty, ol_amount) VALUES ({o_id}, {d_id}, {w_id}, 1, {i_id}, {qty}, {qty}*10)"
            ),
            "COMMIT".to_string(),
        ],
        TxnKind::Payment => vec![
            "BEGIN TRANSACTION".to_string(),
            format!(
                "UPDATE warehouse SET w_ytd = w_ytd + 1 WHERE w_id = {w_id}"
            ),
            format!(
                "UPDATE district SET d_ytd = d_ytd + 1 WHERE d_w_id = {w_id} AND d_id = {d_id}"
            ),
            format!(
                "UPDATE customer SET c_balance = c_balance - 1 WHERE c_w_id = {w_id} AND c_d_id = {d_id} AND c_id = {c_id}"
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
    let conn = connect(&endpoint, addr, &args.server_name).await?;

    let sem = Arc::new(Semaphore::new(args.concurrency.max(1)));
    let tx_total = args.transactions.max(1);
    let new_orders = Arc::new(AtomicU64::new(0));
    let errors = Arc::new(AtomicU64::new(0));

    let start = Instant::now();
    let mut handles = Vec::with_capacity(args.concurrency);

    for worker_id in 0..args.concurrency.max(1) {
        let permit = sem.clone().acquire_owned().await?;
        let conn = conn.clone();
        let mix = mix.clone();
        let new_orders = new_orders.clone();
        let errors = errors.clone();
        let mix_str = args.mix.clone();

        let base = tx_total / args.concurrency.max(1);
        let extra = (worker_id < (tx_total % args.concurrency.max(1))) as usize;
        let my_tx = base + extra;
        let start_index = worker_id * base + worker_id.min(tx_total % args.concurrency.max(1));

        handles.push(tokio::spawn(async move {
            let _permit = permit;
            let mut lat_us: Vec<u128> = Vec::with_capacity(my_tx);
            let seed = 0xC0FFEE_u64 ^ (worker_id as u64).wrapping_mul(0xA5A5A5A5A5A5A5A5);

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
                let res = run_sql_seq_on_stream(&conn, &sqls).await;
                let dt = t0.elapsed();
                lat_us.push(dt.as_micros());
                if res.is_err() {
                    errors.fetch_add(1, Ordering::Relaxed);
                }
            }
            Ok::<(Vec<u128>, String), Box<dyn std::error::Error + Send + Sync>>((lat_us, mix_str))
        }));
    }

    let mut all_lat: Vec<u128> = Vec::with_capacity(tx_total);
    let mut mix_str = args.mix.clone();
    for h in handles {
        let (mut lat, mx) = h.await??;
        all_lat.append(&mut lat);
        mix_str = mx;
    }

    let elapsed = start.elapsed().as_secs_f64().max(1e-9);
    all_lat.sort_unstable();
    let txns_per_s = (tx_total as f64) / elapsed;
    let no = new_orders.load(Ordering::Relaxed);
    let tpmc = (no as f64) / (elapsed / 60.0);

    let report = TpccReport {
        concurrency: args.concurrency.max(1),
        transactions: tx_total,
        elapsed_s: elapsed,
        txns_per_s,
        new_orders: no,
        tpmC: tpmc,
        p50_ms: quantile_ms(&all_lat, 0.50),
        p95_ms: quantile_ms(&all_lat, 0.95),
        p99_ms: quantile_ms(&all_lat, 0.99),
        err: errors.load(Ordering::Relaxed),
        mix: mix_str,
    };

    if args.json {
        println!("{}", serde_json::to_string(&report)?);
    } else {
        println!("== rustdb_tpcc ==");
        println!("concurrency: {}", report.concurrency);
        println!("transactions: {}", report.transactions);
        println!("elapsed_s: {:.3}", report.elapsed_s);
        println!("txns_per_s: {:.1}", report.txns_per_s);
        println!("new_orders: {}", report.new_orders);
        println!("tpmC: {:.1}", report.tpmC);
        println!(
            "latency_ms: p50={:.2} p95={:.2} p99={:.2}",
            report.p50_ms, report.p95_ms, report.p99_ms
        );
        println!("err: {}", report.err);
        println!("mix: {}", report.mix);
    }

    Ok(())
}

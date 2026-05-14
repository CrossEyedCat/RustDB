//! Minimal TPC-C-ish throughput load generator for RustDB (QUIC).
//!
//! This is **not** a full TPC-C compliant implementation. It is a pragmatic CI benchmark:
//! - generates a mixed workload with OLTP-style read/write transactions
//! - reports throughput (txns/s) and a "tpmC" proxy based on New-Order transactions
//! - uses RustDB's QUIC client protocol directly (same framing as rustdb_load)

use async_trait::async_trait;
use clap::Parser;
use quinn::Connection;
use rustdb::network::client::{
    build_quinn_client_config_with_limits, connect, make_client_endpoint,
};
use rustdb::network::framing::{
    decode_server_frame_v1, encode_client_message_v1, ClientMessage, QueryPayload,
};
use rustdb::network::query_stream::read_application_frame_into;
use rustdb::tpcc_workload::{run_tpcc, Mix, TpccExec, TpccRunConfig};
use rustls::pki_types::CertificateDer;
use std::fs;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

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

    /// Append one CSV line per transaction attempt (worker,attempt_id,kind,ok,elapsed_us,error).
    #[arg(long)]
    txn_log: Option<PathBuf>,

    /// QUIC max concurrent bidirectional streams.
    #[arg(long, default_value_t = 512)]
    quic_max_streams: usize,

    /// QUIC max idle timeout (seconds).
    #[arg(long, default_value_t = 30)]
    quic_idle_secs: u64,
}

#[derive(Clone)]
struct QuicExec {
    conn: Connection,
}

#[async_trait]
impl TpccExec for QuicExec {
    async fn run_sql_batch(
        &self,
        sqls: &[String],
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let (mut send, mut recv) = self.conn.open_bi().await?;
        let mut recv_buf = Vec::new();
        // Pipeline: send all frames first, then read responses (same ordering as sequential mode;
        // reduces per-statement round-trip latency when the server processes frames in order).
        for sql in sqls {
            let frame =
                encode_client_message_v1(&ClientMessage::Query(QueryPayload { sql: sql.clone() }))?;
            send.write_all(&frame).await?;
        }
        for _ in sqls {
            read_application_frame_into(&mut recv, 64 * 1024 * 1024, &mut recv_buf).await?;
            let msg = decode_server_frame_v1(&recv_buf)?;
            if let rustdb::network::framing::ServerMessage::Error(p) = msg {
                return Err(format!("server error: {}: {}", p.code, p.message).into());
            }
        }
        let _ = send.finish();
        Ok(())
    }
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
    let conn = connect(&endpoint, addr, &args.server_name).await?;

    let concurrency = args.concurrency.max(1);
    let workers: Vec<Arc<QuicExec>> = (0..concurrency)
        .map(|_| Arc::new(QuicExec { conn: conn.clone() }))
        .collect();

    let duration = args.duration_seconds.map(|s| Duration::from_secs(s.max(1)));
    let report = run_tpcc(
        workers,
        TpccRunConfig {
            concurrency,
            transactions: args.transactions.max(1),
            duration,
            mix,
            mix_string: args.mix.clone(),
            txn_log: args.txn_log.clone(),
        },
    )
    .await?;

    if args.json {
        println!("{}", serde_json::to_string(&report)?);
    } else {
        println!("== rustdb_tpcc ==");
        println!("concurrency: {}", report.concurrency);
        println!("txn_attempts: {}", report.txn_attempts);
        println!("txn_successes: {}", report.txn_successes);
        println!("success_rate_pct: {:.2}", report.success_rate_pct);
        println!("elapsed_s: {:.3}", report.elapsed_s);
        println!("txns_per_s (successful only): {:.1}", report.txns_per_s);
        println!("attempts_per_s (all tries): {:.1}", report.attempts_per_s);
        println!("new_orders (successful only): {}", report.new_orders);
        println!("tpmC: {:.1}", report.tpm_c);
        println!(
            "latency_ms (successful only): p50={:.2} p95={:.2} p99={:.2}",
            report.p50_ms, report.p95_ms, report.p99_ms
        );
        println!("err (failed attempts): {}", report.err);
        println!("mix: {}", report.mix);
        if let Some(ref p) = report.txn_log_path {
            println!("txn_log: {p}");
        }
    }

    Ok(())
}

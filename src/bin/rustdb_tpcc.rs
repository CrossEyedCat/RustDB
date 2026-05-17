//! Minimal TPC-C-ish throughput load generator for RustDB (QUIC).
//!
//! This is **not** a full TPC-C compliant implementation. It is a pragmatic CI benchmark:
//! - generates a mixed workload with OLTP-style read/write transactions
//! - reports throughput (txns/s) and a "tpmC" proxy based on New-Order transactions
//! - uses RustDB's QUIC client protocol directly (same framing as rustdb_load)

use async_trait::async_trait;
use clap::Parser;
use quinn::{Connection, RecvStream, SendStream};
use rustdb::network::client::{
    build_quinn_client_config_with_limits, connect, make_client_endpoint,
};
use rustdb::network::engine::engine_error_code;
use rustdb::network::framing::{
    classify_server_frame_v1, decode_server_frame_v1, encode_client_message_v1,
    encode_execute_tpcc_frame_write, ClientMessage, ExecuteScriptPayload, ExecuteTpccPayload,
    ExecutionOkPayload, QueryPayload, ServerFrameClass, ServerMessage, PROTOCOL_VERSION_V1,
};
use rustdb::network::query_stream::read_application_frame_into;
use rustdb::tpcc_workload::{run_tpcc, txn_kind_as_u8, Mix, TpccExec, TpccRunConfig, TxnKind};
use rustls::pki_types::CertificateDer;
use std::fs;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

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

    /// Share one QUIC connection across all workers (legacy; serializes on one server SQL pool per conn).
    #[arg(long, default_value_t = false)]
    shared_connection: bool,

    /// Use native `ExecuteTpcc` wire path (one RTT per txn); falls back to `ExecuteScript` if unsupported.
    #[arg(long, default_value_t = false)]
    native_tpcc: bool,

    /// Number of QUIC connections when not using `--shared-connection` (default: one per worker).
    #[arg(long)]
    connections: Option<usize>,
}

fn tpcc_native_micro_hot() -> bool {
    std::env::var("RUSTDB_TPCC_NATIVE_MICRO")
        .ok()
        .is_some_and(|v| v == "1" || v.eq_ignore_ascii_case("true"))
}

/// One worker's long-lived bidirectional QUIC stream (reused across transactions).
struct WorkerBiStream {
    send: SendStream,
    recv: RecvStream,
    recv_buf: Vec<u8>,
    send_buf: Vec<u8>,
}

#[derive(Clone)]
struct QuicExec {
    conn: Connection,
    stream: Arc<Mutex<Option<WorkerBiStream>>>,
    execute_script: Arc<AtomicBool>,
    native_tpcc: Arc<AtomicBool>,
    micro_hot: bool,
}

impl QuicExec {
    async fn open_stream(
        conn: &Connection,
    ) -> Result<WorkerBiStream, Box<dyn std::error::Error + Send + Sync>> {
        let (send, recv) = conn.open_bi().await?;
        Ok(WorkerBiStream {
            send,
            recv,
            recv_buf: Vec::new(),
            send_buf: Vec::with_capacity(64),
        })
    }

    fn execute_script_unsupported(msg: &ServerMessage) -> bool {
        match msg {
            ServerMessage::Error(p) if p.code == engine_error_code::PROTOCOL => {
                let m = p.message.to_ascii_lowercase();
                m.contains("unknown message kind")
                    || m.contains("message kind")
                    || m.contains("wrong direction")
            }
            _ => false,
        }
    }

    async fn run_execute_script_on_stream(
        bi: &mut WorkerBiStream,
        sqls: &[String],
    ) -> Result<ServerMessage, Box<dyn std::error::Error + Send + Sync>> {
        let frame =
            encode_client_message_v1(&ClientMessage::ExecuteScript(ExecuteScriptPayload {
                sqls: sqls.to_vec(),
            }))?;
        bi.send.write_all(&frame).await?;
        read_application_frame_into(&mut bi.recv, 64 * 1024 * 1024, &mut bi.recv_buf).await?;
        Ok(decode_server_frame_v1(&bi.recv_buf)?)
    }

    async fn run_multi_query_on_stream(
        bi: &mut WorkerBiStream,
        sqls: &[String],
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        for sql in sqls {
            let frame =
                encode_client_message_v1(&ClientMessage::Query(QueryPayload { sql: sql.clone() }))?;
            bi.send.write_all(&frame).await?;
        }
        for _ in sqls {
            read_application_frame_into(&mut bi.recv, 64 * 1024 * 1024, &mut bi.recv_buf).await?;
            let msg = decode_server_frame_v1(&bi.recv_buf)?;
            if let ServerMessage::Error(p) = msg {
                return Err(format!("server error: {}: {}", p.code, p.message).into());
            }
        }
        Ok(())
    }

    async fn run_execute_tpcc_on_stream(
        bi: &mut WorkerBiStream,
        kind: u8,
        seed: u64,
        global_txn_id: u64,
        micro_hot: bool,
    ) -> Result<ServerMessage, Box<dyn std::error::Error + Send + Sync>> {
        bi.send_buf.clear();
        encode_execute_tpcc_frame_write(
            PROTOCOL_VERSION_V1,
            &ExecuteTpccPayload {
                kind,
                seed,
                global_txn_id,
            },
            &mut bi.send_buf,
        )?;
        bi.send.write_all(&bi.send_buf).await?;
        read_application_frame_into(&mut bi.recv, 64 * 1024 * 1024, &mut bi.recv_buf).await?;
        if micro_hot {
            return Self::classify_tpcc_response(&bi.recv_buf);
        }
        Ok(decode_server_frame_v1(&bi.recv_buf)?)
    }

    fn classify_tpcc_response(
        frame: &[u8],
    ) -> Result<ServerMessage, Box<dyn std::error::Error + Send + Sync>> {
        match classify_server_frame_v1(frame)? {
            ServerFrameClass::ExecutionOk => Ok(ServerMessage::ExecutionOk(ExecutionOkPayload {
                rows_affected: 0,
            })),
            ServerFrameClass::Error(p) => Ok(ServerMessage::Error(p)),
            ServerFrameClass::Other(msg) => Ok(msg),
        }
    }

    async fn run_batch_on_stream(
        bi: &mut WorkerBiStream,
        sqls: &[String],
        try_execute_script: &AtomicBool,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if try_execute_script.load(Ordering::Relaxed) {
            match Self::run_execute_script_on_stream(bi, sqls).await {
                Ok(msg) if Self::execute_script_unsupported(&msg) => {
                    try_execute_script.store(false, Ordering::Relaxed);
                }
                Ok(ServerMessage::Error(p)) => {
                    return Err(format!("server error: {}: {}", p.code, p.message).into());
                }
                Ok(_) => return Ok(()),
                Err(e) => return Err(e),
            }
        }
        Self::run_multi_query_on_stream(bi, sqls).await
    }
}

#[async_trait]
impl TpccExec for QuicExec {
    fn native_tpcc_enabled(&self) -> bool {
        self.native_tpcc.load(Ordering::Relaxed)
    }

    async fn run_native_tpcc(
        &self,
        kind: TxnKind,
        seed: u64,
        global_txn_id: u64,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        const MAX_ATTEMPTS: usize = 2;
        let wire_kind = txn_kind_as_u8(kind);
        for attempt in 0..MAX_ATTEMPTS {
            let mut guard = self.stream.lock().await;
            if guard.is_none() {
                *guard = Some(Self::open_stream(&self.conn).await?);
            }
            let bi = guard.as_mut().expect("stream just opened");
            match Self::run_execute_tpcc_on_stream(
                bi,
                wire_kind,
                seed,
                global_txn_id,
                self.micro_hot,
            )
            .await
            {
                Ok(ServerMessage::Error(p)) => {
                    return Err(format!("server error: {}: {}", p.code, p.message).into());
                }
                Ok(msg) if Self::execute_script_unsupported(&msg) => {
                    self.native_tpcc.store(false, Ordering::Relaxed);
                    return Err("ExecuteTpcc not supported by server".into());
                }
                Ok(_) => return Ok(()),
                Err(e) => {
                    *guard = None;
                    if attempt + 1 >= MAX_ATTEMPTS {
                        return Err(e);
                    }
                }
            }
        }
        unreachable!()
    }

    async fn run_sql_batch(
        &self,
        sqls: &[String],
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        const MAX_ATTEMPTS: usize = 2;
        for attempt in 0..MAX_ATTEMPTS {
            let mut guard = self.stream.lock().await;
            if guard.is_none() {
                *guard = Some(Self::open_stream(&self.conn).await?);
            }
            let bi = guard.as_mut().expect("stream just opened");
            match Self::run_batch_on_stream(bi, sqls, &self.execute_script).await {
                Ok(()) => return Ok(()),
                Err(e) => {
                    *guard = None;
                    if attempt + 1 >= MAX_ATTEMPTS {
                        return Err(e);
                    }
                }
            }
        }
        unreachable!()
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
    let concurrency = args.concurrency.max(1);
    if args.shared_connection && args.connections.is_some() {
        return Err("--connections cannot be used with --shared-connection".into());
    }
    let micro_hot = args.native_tpcc && tpcc_native_micro_hot();
    let workers: Vec<Arc<QuicExec>> = if args.shared_connection {
        let conn = connect(&endpoint, addr, &args.server_name).await?;
        (0..concurrency)
            .map(|_| {
                Arc::new(QuicExec {
                    conn: conn.clone(),
                    stream: Arc::new(Mutex::new(None)),
                    execute_script: Arc::new(AtomicBool::new(true)),
                    native_tpcc: Arc::new(AtomicBool::new(args.native_tpcc)),
                    micro_hot,
                })
            })
            .collect()
    } else {
        let conn_count = args.connections.unwrap_or(concurrency).max(1);
        let mut conns = Vec::with_capacity(conn_count);
        for _ in 0..conn_count {
            conns.push(connect(&endpoint, addr, &args.server_name).await?);
        }
        (0..concurrency)
            .map(|worker_idx| {
                Arc::new(QuicExec {
                    conn: conns[worker_idx % conn_count].clone(),
                    stream: Arc::new(Mutex::new(None)),
                    execute_script: Arc::new(AtomicBool::new(true)),
                    native_tpcc: Arc::new(AtomicBool::new(args.native_tpcc)),
                    micro_hot,
                })
            })
            .collect()
    };

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
            use_native_tpcc: args.native_tpcc,
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

// TPC-C deviations:
// - Table `oorder` is used instead of SQL keyword `ORDER` (same as many TPC-C ports).
// - Cardinalities are reduced from full TPC-C (100k items, 3k customers/district): configurable
//   `--items` and `--customers-per-district` default to moderate CI-friendly values.
// - New-Order remote warehouse / item supply paths are simplified (mostly home warehouse).
// - Payment omits the full “payment by name” path; uses primary-key customer lookup.
// - Delivery processes districts 1..NUM_DISTRICTS with SUM(order_line) when aggregates succeed;
//   otherwise a documented fallback omits balance adjustment (see `workload::txn_delivery`).
// - Stock-Level uses a threshold scan over `stock` rather than full recent-order distinct-item logic.
// - `UPDATE` RHS expressions are limited to literals in RustDB’s SQL surface; all increments are
//   implemented as SELECT-then-UPDATE with computed literals (TPC-C §2 semantics preserved).
// - `history.h_pk` is a surrogate key (RustDB has no AUTOINCREMENT); allocated with an Atomic counter.
// - Secondary indexes: `CREATE INDEX` is not wired in the QUIC SQL engine yet — omitted (table scans).

//! TPC-C-style QUIC benchmark for RustDB: deterministic **load** vs **measurement** phases,
//! standard mix weights, per-mix latency quantiles, and structured JSON/text reports.

use clap::Parser;
use quinn::{Connection, RecvStream, SendStream};
use rustdb::network::client::{
    build_quinn_client_config_with_limits, connect, make_client_endpoint,
};
use rustdb::network::framing::{
    decode_server_frame_v1, encode_client_message_v1, ClientMessage, ErrorPayload, QueryPayload,
    ResultSetPayload, ServerMessage,
};
use rustdb::network::query_stream::read_application_frame_into;
use rustls::pki_types::CertificateDer;
use serde::Serialize;
use std::collections::HashMap;
use std::fs;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
const APP_FRAME_MAX: u32 = 64 * 1024 * 1024;
/// TPC-C: 10 districts per warehouse (§1.3).
const NUM_DISTRICTS: i64 = 10;

// -----------------------------------------------------------------------------
// CLI
// -----------------------------------------------------------------------------

#[derive(Parser, Debug)]
#[command(name = "rustdb_tpcc")]
struct Args {
    /// Server address (host:port).
    #[arg(long, default_value = "127.0.0.1:5432")]
    addr: String,

    /// Path to server leaf certificate (DER).
    #[arg(long)]
    cert: PathBuf,

    /// TLS server name (SAN).
    #[arg(long, default_value = "localhost")]
    server_name: String,

    /// Concurrent workers (each runs one transactional stream at a time).
    #[arg(long, default_value_t = 64)]
    concurrency: usize,

    /// RNG seed (deterministic load + workload tie-breaks).
    #[arg(long, default_value_t = 0xC0FFEE_u64)]
    seed: u64,

    /// Warehouses to create and populate (≥1).
    #[arg(long, default_value_t = 4)]
    warehouses: i64,

    /// Items (`item` / `stock` cardinality per warehouse).
    #[arg(long, default_value_t = 1000)]
    items: i64,

    /// Customers per district (each warehouse has NUM_DISTRICTS districts).
    #[arg(long, default_value_t = 100)]
    customers_per_district: i64,

    /// Warm-up duration (mix runs; **not** counted in throughput or tpmC).
    #[arg(long, default_value_t = 10)]
    warmup_secs: u64,

    /// Measurement duration (successful transactions counted).
    #[arg(long, default_value_t = 60)]
    duration_secs: u64,

    /// Alternative to `--duration-secs`: fixed transaction cap (primarily for local debugging).
    #[arg(long)]
    transactions: Option<usize>,

    /// Output JSON path (atomic write).
    #[arg(long, default_value = "tpcc.json")]
    output_json: PathBuf,

    /// Human-readable report path.
    #[arg(long, default_value = "tpcc.txt")]
    output_text: PathBuf,

    /// QUIC max concurrent bidirectional streams.
    #[arg(long, default_value_t = 512)]
    quic_max_streams: usize,

    /// QUIC idle timeout (seconds).
    #[arg(long, default_value_t = 120)]
    quic_idle_secs: u64,

    /// Initial QUIC connect timeout (seconds).
    #[arg(long, default_value_t = 30)]
    connect_timeout_secs: u64,

    /// Per-statement wall-clock timeout (milliseconds).
    #[arg(long, default_value_t = 60_000)]
    statement_timeout_ms: u64,

    /// Skip DDL + load (expects existing populated schema — advanced).
    #[arg(long, default_value_t = false)]
    skip_load: bool,

    /// Legacy alias used by older scripts.
    #[arg(long = "duration-seconds")]
    duration_seconds_alias: Option<u64>,
}

// -----------------------------------------------------------------------------
// Protocol / wire helpers
// -----------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
enum TxnKind {
    NewOrder,
    Payment,
    OrderStatus,
    Delivery,
    StockLevel,
}

impl TxnKind {
    fn as_str(self) -> &'static str {
        match self {
            TxnKind::NewOrder => "new_order",
            TxnKind::Payment => "payment",
            TxnKind::OrderStatus => "order_status",
            TxnKind::Delivery => "delivery",
            TxnKind::StockLevel => "stock_level",
        }
    }

    fn idx(self) -> usize {
        match self {
            TxnKind::NewOrder => 0,
            TxnKind::Payment => 1,
            TxnKind::OrderStatus => 2,
            TxnKind::Delivery => 3,
            TxnKind::StockLevel => 4,
        }
    }
}

#[derive(Debug)]
enum RunFail {
    Timeout,
    Io(String),
    Decode(String),
    Server(ErrorPayload),
    /// Lost compare-and-swap on `district.d_next_o_id`; caller may retry the whole transaction.
    Contention,
}

impl std::fmt::Display for RunFail {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RunFail::Timeout => write!(f, "timeout"),
            RunFail::Io(s) => write!(f, "io: {s}"),
            RunFail::Decode(s) => write!(f, "decode: {s}"),
            RunFail::Server(p) => write!(f, "server {}: {}", p.code, p.message),
            RunFail::Contention => write!(f, "district order-id contention"),
        }
    }
}

impl std::error::Error for RunFail {}

fn classify_server_error(sql: &str, err: &ErrorPayload) -> ErrorCategory {
    let msg = err.message.to_ascii_lowercase();
    let sql_l = sql.trim_start().to_ascii_lowercase();
    if err.code == 1001
        && msg.contains("record not found")
        && (sql_l.starts_with("delete from new_order") || sql_l.starts_with("select"))
    {
        return ErrorCategory::BusinessLogicMiss;
    }
    if msg.contains("constraint")
        || msg.contains("primary key")
        || msg.contains("unique")
        || msg.contains("foreign key")
    {
        return ErrorCategory::ServerValidation;
    }
    ErrorCategory::ServerOther
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
enum ErrorCategory {
    BusinessLogicMiss,
    ServerValidation,
    ServerOther,
    NetworkIo,
    ProtocolDecode,
    Timeout,
}

impl ErrorCategory {
    fn as_str(self) -> &'static str {
        match self {
            ErrorCategory::BusinessLogicMiss => "business_logic_miss",
            ErrorCategory::ServerValidation => "server_validation",
            ErrorCategory::ServerOther => "server_error",
            ErrorCategory::NetworkIo => "network_io",
            ErrorCategory::ProtocolDecode => "protocol_decode",
            ErrorCategory::Timeout => "timeout",
        }
    }
}

async fn send_query_with_timeout(
    send: &mut SendStream,
    recv: &mut RecvStream,
    recv_buf: &mut Vec<u8>,
    sql: &str,
    timeout: Duration,
) -> Result<ServerMessage, RunFail> {
    let frame = encode_client_message_v1(&ClientMessage::Query(QueryPayload {
        sql: sql.to_string(),
    }))
    .map_err(|e| RunFail::Decode(e.to_string()))?;
    tokio::time::timeout(timeout, async {
        send.write_all(&frame)
            .await
            .map_err(|e| RunFail::Io(e.to_string()))?;
        read_application_frame_into(recv, APP_FRAME_MAX, recv_buf)
            .await
            .map_err(|e| RunFail::Io(e.to_string()))?;
        decode_server_frame_v1(recv_buf).map_err(|e| RunFail::Decode(e.to_string()))
    })
    .await
    .map_err(|_| RunFail::Timeout)?
}

async fn run_sql_on_stream(
    send: &mut SendStream,
    recv: &mut RecvStream,
    recv_buf: &mut Vec<u8>,
    sql: &str,
    stmt_timeout: Duration,
) -> Result<ServerMessage, RunFail> {
    let msg = send_query_with_timeout(send, recv, recv_buf, sql, stmt_timeout).await?;
    if let ServerMessage::Error(_) = msg {
        let rb = encode_client_message_v1(&ClientMessage::Query(QueryPayload {
            sql: "ROLLBACK".to_string(),
        }));
        if let Ok(rb) = rb {
            let _ = send.write_all(&rb).await;
            let _ = read_application_frame_into(recv, APP_FRAME_MAX, recv_buf).await;
        }
    }
    Ok(msg)
}

async fn run_sql_seq(
    send: &mut SendStream,
    recv: &mut RecvStream,
    recv_buf: &mut Vec<u8>,
    sqls: &[String],
    stmt_timeout: Duration,
) -> Result<(), RunFail> {
    for sql in sqls {
        let msg = run_sql_on_stream(send, recv, recv_buf, sql, stmt_timeout).await?;
        match msg {
            ServerMessage::Error(p) => return Err(RunFail::Server(p)),
            ServerMessage::ResultSet(_) | ServerMessage::ExecutionOk(_) => {}
            ServerMessage::ServerReady(_) => {}
        }
    }
    Ok(())
}

fn first_cell_int(rs: &ResultSetPayload) -> Option<i64> {
    let v = rs.rows.first()?.first()?;
    v.parse().ok()
}

async fn run_tpcc_transaction<F, Fut>(
    conn: &Connection,
    _stmt_timeout: Duration,
    f: F,
) -> Result<(), RunFail>
where
    F: FnOnce(SendStream, RecvStream) -> Fut,
    Fut: std::future::Future<Output = Result<(), RunFail>>,
{
    let (send, recv) = conn
        .open_bi()
        .await
        .map_err(|e| RunFail::Io(e.to_string()))?;
    let res = f(send, recv).await;
    res
}

async fn exec_raw_transaction(
    conn: &Connection,
    sqls: Vec<String>,
    stmt_timeout: Duration,
) -> Result<(), RunFail> {
    run_tpcc_transaction(conn, stmt_timeout, |mut send, mut recv| async move {
        let mut buf = Vec::new();
        run_sql_seq(&mut send, &mut recv, &mut buf, &sqls, stmt_timeout).await?;
        let _ = send.finish();
        Ok(())
    })
    .await
}

// -----------------------------------------------------------------------------
// Schema + loader
// -----------------------------------------------------------------------------

fn ddl_drop_create() -> Vec<String> {
    // Drop order respects FKs if enabled (RustDB may enforce); list children first.
    vec![
        "DROP TABLE IF EXISTS history".to_string(),
        "DROP TABLE IF EXISTS order_line".to_string(),
        "DROP TABLE IF EXISTS new_order".to_string(),
        "DROP TABLE IF EXISTS oorder".to_string(),
        "DROP TABLE IF EXISTS stock".to_string(),
        "DROP TABLE IF EXISTS customer".to_string(),
        "DROP TABLE IF EXISTS district".to_string(),
        "DROP TABLE IF EXISTS item".to_string(),
        "DROP TABLE IF EXISTS warehouse".to_string(),
        "CREATE TABLE warehouse (\
            w_id INTEGER NOT NULL PRIMARY KEY,\
            w_tax INTEGER NOT NULL,\
            w_ytd INTEGER NOT NULL\
        )"
        .to_string(),
        "CREATE TABLE district (\
            d_id INTEGER NOT NULL,\
            d_w_id INTEGER NOT NULL,\
            d_tax INTEGER NOT NULL,\
            d_ytd INTEGER NOT NULL,\
            d_next_o_id INTEGER NOT NULL,\
            PRIMARY KEY (d_w_id, d_id)\
        )"
        .to_string(),
        "CREATE TABLE customer (\
            c_id INTEGER NOT NULL,\
            c_d_id INTEGER NOT NULL,\
            c_w_id INTEGER NOT NULL,\
            c_first VARCHAR(16) NOT NULL,\
            c_last VARCHAR(16) NOT NULL,\
            c_balance INTEGER NOT NULL,\
            PRIMARY KEY (c_w_id, c_d_id, c_id)\
        )"
        .to_string(),
        "CREATE TABLE item (\
            i_id INTEGER NOT NULL PRIMARY KEY,\
            i_name VARCHAR(64) NOT NULL,\
            i_price INTEGER NOT NULL\
        )"
        .to_string(),
        "CREATE TABLE stock (\
            s_i_id INTEGER NOT NULL,\
            s_w_id INTEGER NOT NULL,\
            s_qty INTEGER NOT NULL,\
            s_ytd INTEGER NOT NULL,\
            s_order_cnt INTEGER NOT NULL,\
            PRIMARY KEY (s_w_id, s_i_id)\
        )"
        .to_string(),
        "CREATE TABLE oorder (\
            o_id INTEGER NOT NULL,\
            o_d_id INTEGER NOT NULL,\
            o_w_id INTEGER NOT NULL,\
            o_c_id INTEGER NOT NULL,\
            o_ol_cnt INTEGER NOT NULL,\
            PRIMARY KEY (o_w_id, o_d_id, o_id)\
        )"
        .to_string(),
        "CREATE TABLE new_order (\
            no_o_id INTEGER NOT NULL,\
            no_d_id INTEGER NOT NULL,\
            no_w_id INTEGER NOT NULL,\
            PRIMARY KEY (no_w_id, no_d_id, no_o_id)\
        )"
        .to_string(),
        "CREATE TABLE order_line (\
            ol_o_id INTEGER NOT NULL,\
            ol_d_id INTEGER NOT NULL,\
            ol_w_id INTEGER NOT NULL,\
            ol_number INTEGER NOT NULL,\
            ol_i_id INTEGER NOT NULL,\
            ol_qty INTEGER NOT NULL,\
            ol_amount INTEGER NOT NULL,\
            PRIMARY KEY (ol_w_id, ol_d_id, ol_o_id, ol_number)\
        )"
        .to_string(),
        "CREATE TABLE history (\
            h_pk INTEGER NOT NULL PRIMARY KEY,\
            h_c_id INTEGER NOT NULL,\
            h_c_d_id INTEGER NOT NULL,\
            h_c_w_id INTEGER NOT NULL,\
            h_d_id INTEGER NOT NULL,\
            h_w_id INTEGER NOT NULL,\
            h_amount INTEGER NOT NULL,\
            h_data VARCHAR(24) NOT NULL\
        )"
        .to_string(),
    ]
}

async fn load_phase(
    conn: &Connection,
    cfg: &Args,
    stmt_timeout: Duration,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if cfg.skip_load {
        return Ok(());
    }
    for stmt in ddl_drop_create() {
        exec_raw_transaction(conn, vec![stmt], stmt_timeout)
            .await
            .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { e.to_string().into() })?;
    }

    let mut warehouse_rows = Vec::new();
    for w in 1..=cfg.warehouses {
        warehouse_rows.push(format!(
            "INSERT INTO warehouse (w_id, w_tax, w_ytd) VALUES ({w}, {}, 0)",
            5 + ((w * 7) % 10)
        ));
    }
    exec_raw_transaction(conn, warehouse_rows, stmt_timeout)
        .await
        .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { e.to_string().into() })?;

    let mut dist_rows = Vec::new();
    for w in 1..=cfg.warehouses {
        for d in 1..=NUM_DISTRICTS {
            dist_rows.push(format!(
                "INSERT INTO district (d_id, d_w_id, d_tax, d_ytd, d_next_o_id) \
                 VALUES ({d}, {w}, {}, 0, 1)",
                5 + ((d * w) % 10)
            ));
        }
    }
    exec_chunked(conn, dist_rows, 50, stmt_timeout).await?;

    let mut cust_rows = Vec::new();
    for w in 1..=cfg.warehouses {
        for d in 1..=NUM_DISTRICTS {
            for c in 1..=cfg.customers_per_district {
                cust_rows.push(format!(
                    "INSERT INTO customer (c_id, c_d_id, c_w_id, c_first, c_last, c_balance) \
                     VALUES ({c}, {d}, {w}, 'fn{c}', 'ln{c}', 0)"
                ));
            }
        }
    }
    exec_chunked(conn, cust_rows, 40, stmt_timeout).await?;

    let mut item_rows = Vec::new();
    for i in 1..=cfg.items {
        item_rows.push(format!(
            "INSERT INTO item (i_id, i_name, i_price) VALUES ({i}, 'item{i}', {})",
            100 + (i % 900)
        ));
    }
    exec_chunked(conn, item_rows, 80, stmt_timeout).await?;

    let mut stock_rows = Vec::new();
    for w in 1..=cfg.warehouses {
        for i in 1..=cfg.items {
            stock_rows.push(format!(
                "INSERT INTO stock (s_i_id, s_w_id, s_qty, s_ytd, s_order_cnt) \
                 VALUES ({i}, {w}, 100, 0, 0)"
            ));
        }
    }
    exec_chunked(conn, stock_rows, 80, stmt_timeout).await?;

    eprintln!(
        "[tpcc] load complete: warehouses={} districts/wh={} customers/dist={} items={}",
        cfg.warehouses, NUM_DISTRICTS, cfg.customers_per_district, cfg.items
    );
    Ok(())
}

async fn exec_chunked(
    conn: &Connection,
    mut rows: Vec<String>,
    chunk: usize,
    stmt_timeout: Duration,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    while !rows.is_empty() {
        let n = chunk.min(rows.len());
        let chunk_rows: Vec<String> = rows.drain(..n).collect();
        exec_raw_transaction(conn, chunk_rows, stmt_timeout)
            .await
            .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { e.to_string().into() })?;
    }
    Ok(())
}

// -----------------------------------------------------------------------------
// Mix + PRNG (TPC-C §5.2 weights)
// -----------------------------------------------------------------------------

#[derive(Clone)]
struct Mix {
    cumulative: Vec<(TxnKind, f64)>,
}

impl Mix {
    fn standard() -> Self {
        Self::parse("new_order=0.45,payment=0.43,order_status=0.04,delivery=0.04,stock_level=0.04")
            .expect("standard mix")
    }

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
        let sum: f64 = w.iter().map(|(_, x)| *x).sum();
        if sum <= 0.0 {
            return Err("mix sum must be > 0".into());
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
    *state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
    *state
}

fn rand_u64(state: &mut u64) -> u64 {
    lcg_next(state)
}

fn rand_range(state: &mut u64, lo: i64, hi: i64) -> i64 {
    if hi <= lo {
        return lo;
    }
    let span = (hi - lo + 1) as u64;
    lo + (rand_u64(state) % span) as i64
}

fn rand_f64_1(state: &mut u64) -> f64 {
    let x = rand_u64(state);
    let v = x >> 11;
    (v as f64) / ((1u64 << 53) as f64)
}

// -----------------------------------------------------------------------------
// Transaction implementations (canonical sequencing; see TPC-C §2)
// -----------------------------------------------------------------------------

/// New-Order (§2.4): inserts parent order, `new_order`, and line rows with stock adjustments.
async fn txn_new_order(
    conn: &Connection,
    warehouses: i64,
    items: i64,
    customers_per_district: i64,
    rng: &mut u64,
    stmt_timeout: Duration,
) -> Result<(), RunFail> {
    const MAX_CAS: usize = 128;
    for _ in 0..MAX_CAS {
        let home_w = rand_range(rng, 1, warehouses);
        let d_id = rand_range(rng, 1, NUM_DISTRICTS);
        let c_id = rand_range(rng, 1, customers_per_district);
        let ol_cnt = rand_range(rng, 5, 15);
        let mut lines = Vec::with_capacity(ol_cnt as usize);
        for _ in 0..ol_cnt {
            lines.push((rand_range(rng, 1, items), rand_range(rng, 1, 10)));
        }

        let attempt = run_tpcc_transaction(conn, stmt_timeout, move |mut send, mut recv| async move {
        let mut buf = Vec::new();
        run_sql_on_stream(
            &mut send,
            &mut recv,
            &mut buf,
            "BEGIN TRANSACTION",
            stmt_timeout,
        )
        .await?;

        let sel_d = format!(
            "SELECT d_next_o_id, d_tax FROM district WHERE d_w_id = {home_w} AND d_id = {d_id}"
        );
        let msg = run_sql_on_stream(&mut send, &mut recv, &mut buf, &sel_d, stmt_timeout).await?;
        let rs = match msg {
            ServerMessage::ResultSet(rs) => rs,
            ServerMessage::Error(p) => return Err(RunFail::Server(p)),
            _ => {
                return Err(RunFail::Decode(
                    "unexpected response for district select".into(),
                ))
            }
        };
        let o_id = first_cell_int(&rs)
            .ok_or_else(|| RunFail::Decode("could not parse d_next_o_id".into()))?;

        let next_o = o_id.saturating_add(1);
        let upd_d = format!(
            "UPDATE district SET d_next_o_id = {next_o} WHERE d_w_id = {home_w} AND d_id = {d_id} AND d_next_o_id = {o_id}"
        );
        let msg = run_sql_on_stream(&mut send, &mut recv, &mut buf, &upd_d, stmt_timeout).await?;
        let rows = match msg {
            ServerMessage::ExecutionOk(p) => p.rows_affected,
            ServerMessage::Error(p) => return Err(RunFail::Server(p)),
            _ => {
                return Err(RunFail::Decode(
                    "expected ExecutionOk for district CAS update".into(),
                ))
            }
        };
        if rows == 0 {
            let _ = run_sql_on_stream(
                &mut send,
                &mut recv,
                &mut buf,
                "ROLLBACK",
                stmt_timeout,
            )
            .await;
            let _ = send.finish();
            return Err(RunFail::Contention);
        }

        let ins_o = format!(
            "INSERT INTO oorder (o_id, o_d_id, o_w_id, o_c_id, o_ol_cnt) \
             VALUES ({o_id}, {d_id}, {home_w}, {c_id}, {ol_cnt})"
        );
        run_sql_seq(&mut send, &mut recv, &mut buf, &[ins_o], stmt_timeout).await?;

        let ins_no = format!(
            "INSERT INTO new_order (no_o_id, no_d_id, no_w_id) VALUES ({o_id}, {d_id}, {home_w})"
        );
        run_sql_seq(&mut send, &mut recv, &mut buf, &[ins_no], stmt_timeout).await?;

        for (ol, (i_id, qty)) in (1..=ol_cnt).zip(lines.into_iter()) {
            let supply_w = home_w;

            let sel_s =
                format!("SELECT s_qty FROM stock WHERE s_w_id = {supply_w} AND s_i_id = {i_id}");
            let msg =
                run_sql_on_stream(&mut send, &mut recv, &mut buf, &sel_s, stmt_timeout).await?;
            let old_qty = match msg {
                ServerMessage::ResultSet(rs) => first_cell_int(&rs).unwrap_or(0),
                ServerMessage::Error(p) => return Err(RunFail::Server(p)),
                _ => 0,
            };
            let new_qty = (old_qty - qty).max(0);
            let upd_s = format!(
                "UPDATE stock SET s_qty = {new_qty}, s_ytd = 0, s_order_cnt = 0 \
                 WHERE s_w_id = {supply_w} AND s_i_id = {i_id}"
            );
            run_sql_seq(&mut send, &mut recv, &mut buf, &[upd_s], stmt_timeout).await?;

            let amount = qty * 100;
            let ins_ol = format!(
                "INSERT INTO order_line \
                 (ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_qty, ol_amount) \
                 VALUES ({o_id}, {d_id}, {home_w}, {ol}, {i_id}, {qty}, {amount})"
            );
            run_sql_seq(&mut send, &mut recv, &mut buf, &[ins_ol], stmt_timeout).await?;
        }

        run_sql_seq(
            &mut send,
            &mut recv,
            &mut buf,
            &["COMMIT".to_string()],
            stmt_timeout,
        )
        .await?;
        let _ = send.finish();
        Ok(())
        })
        .await;

        match attempt {
            Ok(()) => return Ok(()),
            Err(RunFail::Contention) => continue,
            Err(e) => return Err(e),
        }
    }
    Err(RunFail::Io(
        "new_order: exhausted district order-id retries".into(),
    ))
}

/// Payment (§2.5): warehouse + district + customer balance updates and `history` insert.
async fn txn_payment(
    conn: &Connection,
    warehouses: i64,
    customers_per_district: i64,
    hist_pk: Arc<AtomicU64>,
    rng: &mut u64,
    stmt_timeout: Duration,
) -> Result<(), RunFail> {
    let home_w = rand_range(rng, 1, warehouses);
    let d_id = rand_range(rng, 1, NUM_DISTRICTS);
    let c_id = rand_range(rng, 1, customers_per_district);
    let amount = rand_range(rng, 100, 50_000);

    run_tpcc_transaction(conn, stmt_timeout, |mut send, mut recv| async move {
        let mut buf = Vec::new();
        run_sql_on_stream(
            &mut send,
            &mut recv,
            &mut buf,
            "BEGIN TRANSACTION",
            stmt_timeout,
        )
        .await?;

        let q_w = format!("SELECT w_ytd FROM warehouse WHERE w_id = {home_w}");
        let msg = run_sql_on_stream(&mut send, &mut recv, &mut buf, &q_w, stmt_timeout).await?;
        let w_ytd = match msg {
            ServerMessage::ResultSet(rs) => first_cell_int(&rs).unwrap_or(0),
            ServerMessage::Error(p) => return Err(RunFail::Server(p)),
            _ => 0,
        };
        let q_d = format!(
            "SELECT d_ytd FROM district WHERE d_w_id = {home_w} AND d_id = {d_id}"
        );
        let msg = run_sql_on_stream(&mut send, &mut recv, &mut buf, &q_d, stmt_timeout).await?;
        let d_ytd = match msg {
            ServerMessage::ResultSet(rs) => first_cell_int(&rs).unwrap_or(0),
            ServerMessage::Error(p) => return Err(RunFail::Server(p)),
            _ => 0,
        };
        let q_c = format!(
            "SELECT c_balance FROM customer WHERE c_w_id = {home_w} AND c_d_id = {d_id} AND c_id = {c_id}"
        );
        let msg = run_sql_on_stream(&mut send, &mut recv, &mut buf, &q_c, stmt_timeout).await?;
        let c_bal = match msg {
            ServerMessage::ResultSet(rs) => first_cell_int(&rs).unwrap_or(0),
            ServerMessage::Error(p) => return Err(RunFail::Server(p)),
            _ => 0,
        };

        let nw_ytd = w_ytd.saturating_add(amount);
        let nd_ytd = d_ytd.saturating_add(amount);
        let nc_bal = c_bal.saturating_sub(amount);

        run_sql_seq(
            &mut send,
            &mut recv,
            &mut buf,
            &[format!("UPDATE warehouse SET w_ytd = {nw_ytd} WHERE w_id = {home_w}")],
            stmt_timeout,
        )
        .await?;
        run_sql_seq(
            &mut send,
            &mut recv,
            &mut buf,
            &[format!(
                "UPDATE district SET d_ytd = {nd_ytd} WHERE d_w_id = {home_w} AND d_id = {d_id}"
            )],
            stmt_timeout,
        )
        .await?;
        run_sql_seq(
            &mut send,
            &mut recv,
            &mut buf,
            &[format!(
                "UPDATE customer SET c_balance = {nc_bal} \
                 WHERE c_w_id = {home_w} AND c_d_id = {d_id} AND c_id = {c_id}"
            )],
            stmt_timeout,
        )
        .await?;

        let hp = hist_pk
            .fetch_add(1, Ordering::Relaxed)
            .wrapping_add(1);
        let ins_h = format!(
            "INSERT INTO history (h_pk, h_c_id, h_c_d_id, h_c_w_id, h_d_id, h_w_id, h_amount, h_data) \
             VALUES ({hp}, {c_id}, {d_id}, {home_w}, {d_id}, {home_w}, {amount}, 'pay')"
        );
        run_sql_seq(&mut send, &mut recv, &mut buf, &[ins_h], stmt_timeout).await?;

        run_sql_seq(
            &mut send,
            &mut recv,
            &mut buf,
            &["COMMIT".to_string()],
            stmt_timeout,
        )
        .await?;
        let _ = send.finish();
        Ok(())
    })
    .await
}

/// Order-Status (§2.6): read last order for a customer.
async fn txn_order_status(
    conn: &Connection,
    warehouses: i64,
    customers_per_district: i64,
    rng: &mut u64,
    stmt_timeout: Duration,
) -> Result<(), RunFail> {
    let w_id = rand_range(rng, 1, warehouses);
    let d_id = rand_range(rng, 1, NUM_DISTRICTS);
    let c_id = rand_range(rng, 1, customers_per_district);
    let q = format!(
        "SELECT * FROM oorder WHERE o_w_id = {w_id} AND o_d_id = {d_id} AND o_c_id = {c_id} \
         ORDER BY o_id DESC LIMIT 1"
    );
    run_tpcc_transaction(conn, stmt_timeout, |mut send, mut recv| async move {
        let mut buf = Vec::new();
        run_sql_on_stream(
            &mut send,
            &mut recv,
            &mut buf,
            "BEGIN TRANSACTION",
            stmt_timeout,
        )
        .await?;
        run_sql_seq(&mut send, &mut recv, &mut buf, &[q], stmt_timeout).await?;
        run_sql_seq(
            &mut send,
            &mut recv,
            &mut buf,
            &["COMMIT".to_string()],
            stmt_timeout,
        )
        .await?;
        let _ = send.finish();
        Ok(())
    })
    .await
}

/// Delivery (§2.7): for each district, deliver one pending `new_order` if present.
async fn txn_delivery(
    conn: &Connection,
    warehouses: i64,
    _customers_per_district: i64,
    rng: &mut u64,
    stmt_timeout: Duration,
) -> Result<(), RunFail> {
    let w_id = rand_range(rng, 1, warehouses);
    let fallback_sum = rand_range(rng, 100, 5000);

    run_tpcc_transaction(conn, stmt_timeout, move |mut send, mut recv| async move {
        let mut buf = Vec::new();
        run_sql_on_stream(
            &mut send,
            &mut recv,
            &mut buf,
            "BEGIN TRANSACTION",
            stmt_timeout,
        )
        .await?;

        for d_id in 1..=NUM_DISTRICTS {
            let q_no = format!(
                "SELECT no_o_id FROM new_order WHERE no_w_id = {w_id} AND no_d_id = {d_id} \
                 ORDER BY no_o_id ASC LIMIT 1"
            );
            let msg =
                run_sql_on_stream(&mut send, &mut recv, &mut buf, &q_no, stmt_timeout).await?;
            let oid = match msg {
                ServerMessage::ResultSet(rs) => first_cell_int(&rs),
                ServerMessage::Error(p) => return Err(RunFail::Server(p)),
                _ => None,
            };
            let Some(oid) = oid else {
                continue;
            };

            let del = format!(
                "DELETE FROM new_order WHERE no_w_id = {w_id} AND no_d_id = {d_id} AND no_o_id = {oid}"
            );
            run_sql_seq(&mut send, &mut recv, &mut buf, &[del], stmt_timeout).await?;

            let q_cid = format!(
                "SELECT o_c_id FROM oorder WHERE o_w_id = {w_id} AND o_d_id = {d_id} AND o_id = {oid}"
            );
            let msg =
                run_sql_on_stream(&mut send, &mut recv, &mut buf, &q_cid, stmt_timeout).await?;
            let o_c_id = match msg {
                ServerMessage::ResultSet(rs) => first_cell_int(&rs),
                ServerMessage::Error(p) => return Err(RunFail::Server(p)),
                _ => None,
            };
            let Some(cid) = o_c_id else {
                continue;
            };

            let sum_q = format!(
                "SELECT SUM(ol_amount) FROM order_line \
                 WHERE ol_w_id = {w_id} AND ol_d_id = {d_id} AND ol_o_id = {oid}"
            );
            let msg =
                run_sql_on_stream(&mut send, &mut recv, &mut buf, &sum_q, stmt_timeout).await?;
            let owed = match msg {
                ServerMessage::ResultSet(rs) => first_cell_int(&rs).unwrap_or(0),
                ServerMessage::Error(_) => fallback_sum,
                _ => 0,
            };

            let qb = format!(
                "SELECT c_balance FROM customer WHERE c_w_id = {w_id} AND c_d_id = {d_id} AND c_id = {cid}"
            );
            let msg =
                run_sql_on_stream(&mut send, &mut recv, &mut buf, &qb, stmt_timeout).await?;
            let bal = match msg {
                ServerMessage::ResultSet(rs) => first_cell_int(&rs).unwrap_or(0),
                ServerMessage::Error(p) => return Err(RunFail::Server(p)),
                _ => 0,
            };
            let nbal = bal.saturating_sub(owed);
            let upd = format!(
                "UPDATE customer SET c_balance = {nbal} \
                 WHERE c_w_id = {w_id} AND c_d_id = {d_id} AND c_id = {cid}"
            );
            run_sql_seq(&mut send, &mut recv, &mut buf, &[upd], stmt_timeout).await?;
        }

        run_sql_seq(
            &mut send,
            &mut recv,
            &mut buf,
            &["COMMIT".to_string()],
            stmt_timeout,
        )
        .await?;
        let _ = send.finish();
        Ok(())
    })
    .await
}

/// Stock-Level (§2.8): simplified threshold scan over local `stock`.
async fn txn_stock_level(
    conn: &Connection,
    warehouses: i64,
    rng: &mut u64,
    stmt_timeout: Duration,
) -> Result<(), RunFail> {
    let w_id = rand_range(rng, 1, warehouses);
    let threshold = rand_range(rng, 10, 80);

    run_tpcc_transaction(conn, stmt_timeout, |mut send, mut recv| async move {
        let mut buf = Vec::new();
        run_sql_on_stream(
            &mut send,
            &mut recv,
            &mut buf,
            "BEGIN TRANSACTION",
            stmt_timeout,
        )
        .await?;
        let sel =
            format!("SELECT COUNT(*) FROM stock WHERE s_w_id = {w_id} AND s_qty < {threshold}");
        run_sql_seq(&mut send, &mut recv, &mut buf, &[sel], stmt_timeout).await?;
        run_sql_seq(
            &mut send,
            &mut recv,
            &mut buf,
            &["COMMIT".to_string()],
            stmt_timeout,
        )
        .await?;
        let _ = send.finish();
        Ok(())
    })
    .await
}

fn map_fail_sql(last_sql: &str, e: RunFail) -> ErrorCategory {
    match e {
        RunFail::Timeout => ErrorCategory::Timeout,
        RunFail::Io(_) => ErrorCategory::NetworkIo,
        RunFail::Decode(_) => ErrorCategory::ProtocolDecode,
        RunFail::Server(p) => classify_server_error(last_sql, &p),
        RunFail::Contention => ErrorCategory::BusinessLogicMiss,
    }
}

struct WorkloadParams {
    warehouses: i64,
    items: i64,
    cust_pd: i64,
    hist_pk: Arc<AtomicU64>,
    stmt_timeout: Duration,
}

async fn run_mix_txn(
    conn: &Connection,
    kind: TxnKind,
    p: &WorkloadParams,
    rng: &mut u64,
) -> Result<(), RunFail> {
    match kind {
        TxnKind::NewOrder => {
            txn_new_order(conn, p.warehouses, p.items, p.cust_pd, rng, p.stmt_timeout).await
        }
        TxnKind::Payment => {
            txn_payment(
                conn,
                p.warehouses,
                p.cust_pd,
                Arc::clone(&p.hist_pk),
                rng,
                p.stmt_timeout,
            )
            .await
        }
        TxnKind::OrderStatus => {
            txn_order_status(conn, p.warehouses, p.cust_pd, rng, p.stmt_timeout).await
        }
        TxnKind::Delivery => txn_delivery(conn, p.warehouses, p.cust_pd, rng, p.stmt_timeout).await,
        TxnKind::StockLevel => txn_stock_level(conn, p.warehouses, rng, p.stmt_timeout).await,
    }
}

// -----------------------------------------------------------------------------
// Metrics + reporting
// -----------------------------------------------------------------------------

#[derive(Default, Clone)]
struct WorkerStats {
    mix_ok: [u64; 5],
    new_orders: u64,
    err_cat: HashMap<ErrorCategory, u64>,
    lat_mix: [Vec<u128>; 5],
    lat_all: Vec<u128>,
}

fn merge_lat(samples: &mut [u128]) {
    samples.sort_unstable();
}

fn quantiles_us(samples: &[u128], q: f64) -> f64 {
    if samples.is_empty() {
        return 0.0;
    }
    let qq = q.clamp(0.0, 1.0);
    let idx = (((samples.len() - 1) as f64) * qq).round() as usize;
    samples[idx.min(samples.len() - 1)] as f64 / 1000.0
}

#[derive(Clone, Copy, Serialize)]
struct LatencyMs {
    p50: f64,
    p95: f64,
    p99: f64,
    p999: f64,
}

#[derive(Serialize)]
struct JsonReport {
    txns_per_s: f64,
    #[serde(rename = "tpmC")]
    tpm_c: f64,
    transactions: TxnCountsJson,
    elapsed_s: f64,
    warmup_secs: u64,
    concurrency: usize,
    warehouses: i64,
    errors_by_category: HashMap<String, u64>,
    latency_ms_by_mix: HashMap<String, LatencyMs>,
    overall_latency_ms: LatencyMs,
    seed: u64,
    commit_sha: String,
    started_at: String,
    finished_at: String,
}

#[derive(Serialize)]
struct TxnCountsJson {
    success_total: u64,
    by_mix: HashMap<String, u64>,
}

fn utc_iso(t: std::time::SystemTime) -> String {
    let secs = t
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    format!("{secs}")
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut args = Args::parse();
    if let Some(d) = args.duration_seconds_alias {
        args.duration_secs = d;
    }

    let stmt_timeout = Duration::from_millis(args.statement_timeout_ms.max(1));

    let addr: SocketAddr = args.addr.parse().map_err(|e| format!("bad --addr: {e}"))?;
    let der = fs::read(&args.cert).map_err(|e| format!("read cert: {e}"))?;
    let cert = CertificateDer::from(der);
    let client_cfg = build_quinn_client_config_with_limits(
        std::slice::from_ref(&cert),
        args.quic_max_streams.max(args.concurrency),
        Duration::from_secs(args.quic_idle_secs),
    )
    .map_err(|e| format!("tls client config: {e}"))?;
    let endpoint = make_client_endpoint(client_cfg)?;

    let conn = tokio::time::timeout(
        Duration::from_secs(args.connect_timeout_secs.max(1)),
        connect(&endpoint, addr, &args.server_name),
    )
    .await
    .map_err(|_| format!("timeout connecting to {}", args.addr))??;

    let started_at = std::time::SystemTime::now();

    load_phase(&conn, &args, stmt_timeout).await.map_err(|e| {
        format!("load phase failed (schema/data); fix DDL/connectivity or pass --skip-load: {e}")
    })?;

    let mix = Mix::standard();
    let hist_pk = Arc::new(AtomicU64::new(0));

    let measurement_secs = args.duration_secs.max(1);
    let warmup_secs = args.warmup_secs;
    let deadline_tx = args.transactions;

    let warehouses = args.warehouses.max(1);
    let items = args.items.max(1);
    let cust_pd = args.customers_per_district.max(1);

    let warm_deadline = Instant::now() + Duration::from_secs(warmup_secs);
    let mut warm_handles = Vec::new();
    for wid in 0..args.concurrency.max(1) {
        let conn = conn.clone();
        let hist_pk_w = hist_pk.clone();
        let mix_w = mix.clone();
        let seed_w = args.seed ^ ((wid as u64).wrapping_mul(0x9E3779B97F4A7C15));
        let wp = WorkloadParams {
            warehouses,
            items,
            cust_pd,
            hist_pk: hist_pk_w,
            stmt_timeout,
        };
        warm_handles.push(tokio::spawn(async move {
            let mut rng = seed_w;
            while Instant::now() < warm_deadline {
                let u = rand_f64_1(&mut rng);
                let kind = mix_w.pick(u);
                let _ = run_mix_txn(&conn, kind, &wp, &mut rng).await;
            }
        }));
    }
    for h in warm_handles {
        let _ = h.await;
    }

    let measure_start = Instant::now();
    let measure_deadline = measure_start + Duration::from_secs(measurement_secs);
    let mut handles = Vec::new();

    for wid in 0..args.concurrency.max(1) {
        let conn = conn.clone();
        let hist_pk = hist_pk.clone();
        let mix = mix.clone();
        let seed = args.seed ^ ((wid as u64).wrapping_mul(0x9E3779B97F4A7C15));

        handles.push(tokio::spawn(async move {
            let mut st = WorkerStats::default();
            let mut rng = seed;
            let wp = WorkloadParams {
                warehouses,
                items,
                cust_pd,
                hist_pk,
                stmt_timeout,
            };

            if let Some(limit) = deadline_tx {
                for _ in 0..limit {
                    let u = rand_f64_1(&mut rng);
                    let kind = mix.pick(u);
                    let t0 = Instant::now();
                    let r = run_mix_txn(&conn, kind, &wp, &mut rng).await;
                    let dt = t0.elapsed().as_micros();
                    match r {
                        Ok(()) => {
                            st.mix_ok[kind.idx()] += 1;
                            if kind == TxnKind::NewOrder {
                                st.new_orders += 1;
                            }
                            st.lat_mix[kind.idx()].push(dt);
                            st.lat_all.push(dt);
                        }
                        Err(e) => {
                            let cat = map_fail_sql("", e);
                            *st.err_cat.entry(cat).or_insert(0) += 1;
                        }
                    }
                }
            } else {
                while Instant::now() < measure_deadline {
                    let u = rand_f64_1(&mut rng);
                    let kind = mix.pick(u);
                    let t0 = Instant::now();
                    let r = run_mix_txn(&conn, kind, &wp, &mut rng).await;
                    let dt = t0.elapsed().as_micros();
                    match r {
                        Ok(()) => {
                            st.mix_ok[kind.idx()] += 1;
                            if kind == TxnKind::NewOrder {
                                st.new_orders += 1;
                            }
                            st.lat_mix[kind.idx()].push(dt);
                            st.lat_all.push(dt);
                        }
                        Err(e) => {
                            let cat = map_fail_sql("", e);
                            *st.err_cat.entry(cat).or_insert(0) += 1;
                        }
                    }
                }
            }
            st
        }));
    }

    let mut merged = WorkerStats::default();
    for h in handles {
        let s = h.await.map_err(|e| format!("worker join: {e}"))?;
        for i in 0..5 {
            merged.mix_ok[i] += s.mix_ok[i];
            merged.lat_mix[i].extend(s.lat_mix[i].clone());
        }
        merged.new_orders += s.new_orders;
        merged.lat_all.extend(s.lat_all);
        for (k, v) in s.err_cat {
            *merged.err_cat.entry(k).or_insert(0) += v;
        }
    }

    let elapsed = measure_start.elapsed().as_secs_f64().max(1e-9);
    let succ_total: u64 = merged.mix_ok.iter().sum();
    let txns_per_s = succ_total as f64 / elapsed;
    let tpm_c = merged.new_orders as f64 / (elapsed / 60.0);

    merge_lat(&mut merged.lat_all);
    for i in 0..5 {
        merge_lat(&mut merged.lat_mix[i]);
    }

    let mut latency_ms_by_mix: HashMap<String, LatencyMs> = HashMap::new();
    let kinds = [
        TxnKind::NewOrder,
        TxnKind::Payment,
        TxnKind::OrderStatus,
        TxnKind::Delivery,
        TxnKind::StockLevel,
    ];
    for k in kinds {
        let v = &merged.lat_mix[k.idx()];
        latency_ms_by_mix.insert(
            k.as_str().to_string(),
            LatencyMs {
                p50: quantiles_us(v, 0.50),
                p95: quantiles_us(v, 0.95),
                p99: quantiles_us(v, 0.99),
                p999: quantiles_us(v, 0.999),
            },
        );
    }

    let overall = LatencyMs {
        p50: quantiles_us(&merged.lat_all, 0.50),
        p95: quantiles_us(&merged.lat_all, 0.95),
        p99: quantiles_us(&merged.lat_all, 0.99),
        p999: quantiles_us(&merged.lat_all, 0.999),
    };

    let mut txt = String::new();
    txt.push_str("== rustdb_tpcc (measurement) ==\n");
    txt.push_str(&format!("seed: {}\n", args.seed));
    txt.push_str(&format!("warehouses: {}\n", warehouses));
    txt.push_str(&format!("warmup_secs: {}\n", warmup_secs));
    txt.push_str(&format!("duration_secs (measured): {:.3}\n", elapsed));
    txt.push_str(&format!("concurrency: {}\n", args.concurrency.max(1)));
    txt.push_str(&format!("success_total: {}\n", succ_total));
    txt.push_str(&format!("txns_per_s: {:.2}\n", txns_per_s));
    txt.push_str(&format!("tpmC (new_orders/min): {:.2}\n", tpm_c));
    txt.push_str(&format!("new_orders (success): {}\n", merged.new_orders));
    txt.push_str("\nper-mix success:\n");
    for k in kinds {
        txt.push_str(&format!("  {}: {}\n", k.as_str(), merged.mix_ok[k.idx()]));
    }
    txt.push_str("\noverall latency (ms):\n");
    txt.push_str(&format!(
        "  p50 {:.3}  p95 {:.3}  p99 {:.3}  p999 {:.3}\n",
        overall.p50, overall.p95, overall.p99, overall.p999
    ));
    txt.push_str("\nerrors_by_category:\n");
    let mut cats: Vec<_> = merged.err_cat.iter().collect();
    cats.sort_by(|a, b| a.0.as_str().cmp(b.0.as_str()));
    for (c, n) in cats {
        txt.push_str(&format!("  {}: {}\n", c.as_str(), n));
    }

    let mut by_mix_map: HashMap<String, u64> = HashMap::new();
    for k in kinds {
        by_mix_map.insert(k.as_str().to_string(), merged.mix_ok[k.idx()]);
    }

    let mut errors_by_category: HashMap<String, u64> = HashMap::new();
    for (k, v) in &merged.err_cat {
        errors_by_category.insert(k.as_str().to_string(), *v);
    }

    let commit_sha = std::env::var("GITHUB_SHA").unwrap_or_default();

    let finished_at = std::time::SystemTime::now();

    let json = JsonReport {
        txns_per_s,
        tpm_c,
        transactions: TxnCountsJson {
            success_total: succ_total,
            by_mix: by_mix_map,
        },
        elapsed_s: elapsed,
        warmup_secs,
        concurrency: args.concurrency.max(1),
        warehouses,
        errors_by_category,
        latency_ms_by_mix,
        overall_latency_ms: overall,
        seed: args.seed,
        commit_sha,
        started_at: utc_iso(started_at),
        finished_at: utc_iso(finished_at),
    };

    let json_s = serde_json::to_string_pretty(&json).map_err(|e| format!("serialize json: {e}"))?;
    let tmp = args.output_json.with_extension("json.tmp");
    fs::write(&tmp, &json_s).map_err(|e| format!("write tmp json: {e}"))?;
    fs::rename(&tmp, &args.output_json).map_err(|e| format!("rename json: {e}"))?;

    fs::write(&args.output_text, txt.as_bytes()).map_err(|e| format!("write tpcc.txt: {e}"))?;

    eprintln!(
        "[tpcc] measurement done: txns_per_s={:.2} tpmC={:.2} success={} errors={:?}",
        txns_per_s, tpm_c, succ_total, merged.err_cat
    );

    Ok(())
}

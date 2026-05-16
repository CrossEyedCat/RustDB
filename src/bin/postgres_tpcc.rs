//! TPC-C-ish throughput load generator for PostgreSQL (TCP + `tokio-postgres`).
//!
//! Uses the same statement mix and parameters as `rustdb_tpcc` (`rustdb::tpcc_workload`).
//! Schema: apply `scripts/tpcc_seed.sql` (same minimal tables as RustDB CI).

use async_trait::async_trait;
use clap::Parser;
use rustdb::tpcc_workload::{run_tpcc, Mix, TpccExec, TpccRunConfig};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio_postgres::{Client, NoTls};

#[derive(Parser, Debug)]
#[command(name = "postgres_tpcc")]
struct Args {
    #[arg(long, default_value = "127.0.0.1")]
    host: String,

    #[arg(long, default_value_t = 5432_u16)]
    port: u16,

    #[arg(long, default_value = "postgres")]
    user: String,

    #[arg(long, default_value = "postgres")]
    password: String,

    #[arg(long, default_value = "tpcc_bench")]
    database: String,

    #[arg(long, default_value_t = 64)]
    concurrency: usize,

    #[arg(long, default_value_t = 5_000)]
    transactions: usize,

    #[arg(long)]
    duration_seconds: Option<u64>,

    #[arg(
        long,
        default_value = "new_order=0.45,payment=0.43,order_status=0.04,delivery=0.04,stock_level=0.04"
    )]
    mix: String,

    #[arg(long, default_value_t = false)]
    json: bool,

    #[arg(long)]
    txn_log: Option<PathBuf>,
}

struct PgExec {
    client: Client,
}

#[async_trait]
impl TpccExec for PgExec {
    async fn run_sql_batch(
        &self,
        sqls: &[String],
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        for sql in sqls {
            self.client.simple_query(sql).await?;
        }
        Ok(())
    }
}

async fn connect_worker(
    host: &str,
    port: u16,
    user: &str,
    password: &str,
    database: &str,
) -> Result<Client, Box<dyn std::error::Error + Send + Sync>> {
    let conn_str =
        format!("host={host} port={port} user={user} password={password} dbname={database}");
    let (client, connection) = tokio_postgres::connect(&conn_str, NoTls).await?;
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("postgres_tpcc: connection task error: {e}");
        }
    });
    Ok(client)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let args = Args::parse();
    let mix = Mix::parse(&args.mix).map_err(|e| format!("invalid --mix: {e}"))?;

    let concurrency = args.concurrency.max(1);
    let mut workers: Vec<Arc<PgExec>> = Vec::with_capacity(concurrency);
    for _ in 0..concurrency {
        let c = connect_worker(
            &args.host,
            args.port,
            &args.user,
            &args.password,
            &args.database,
        )
        .await?;
        workers.push(Arc::new(PgExec { client: c }));
    }

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
            use_native_tpcc: false,
        },
    )
    .await?;

    if args.json {
        println!("{}", serde_json::to_string(&report)?);
    } else {
        println!("== postgres_tpcc ==");
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

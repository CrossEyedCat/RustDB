//! TPC-C-ish throughput load generator for PostgreSQL (TCP + `tokio-postgres`).
//!
//! Uses the same statement mix and parameters as `rustdb_tpcc` (`rustdb::tpcc_workload`).
//! Schema: apply `scripts/tpcc_seed.sql` (same minimal tables as RustDB CI).

use async_trait::async_trait;
use clap::Parser;
use rustdb::tpcc_workload::{run_tpcc, txn_params, Mix, TpccExec, TpccRunConfig, TxnKind};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio_postgres::{Client, NoTls, Statement};

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

    /// Use server-side prepared statements (fairer PG baseline vs ad-hoc simple_query).
    #[arg(long, default_value_t = false)]
    prepared: bool,

    #[arg(long, default_value_t = false)]
    json: bool,

    #[arg(long)]
    txn_log: Option<PathBuf>,
}

struct PgStmts {
    no_district: Statement,
    no_oorder: Statement,
    no_new_order: Statement,
    no_stock: Statement,
    no_order_line: Statement,
    pay_warehouse: Statement,
    pay_district: Statement,
    pay_customer: Statement,
    os_oorder: Statement,
    del_new_order: Statement,
    sl_stock: Statement,
}

struct PgExec {
    client: Client,
    prepared: Option<PgStmts>,
}

impl PgExec {
    async fn connect_prepared(
        host: &str,
        port: u16,
        user: &str,
        password: &str,
        database: &str,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let client = connect_worker(host, port, user, password, database).await?;
        let stmts = PgStmts {
            no_district: client
                .prepare(
                    "UPDATE district SET d_next_o_id = d_next_o_id + 1 WHERE d_w_id = $1 AND d_id = $2",
                )
                .await?,
            no_oorder: client
                .prepare(
                    "INSERT INTO oorder (o_id, o_d_id, o_w_id, o_c_id, o_ol_cnt) VALUES ($1, $2, $3, $4, 1)",
                )
                .await?,
            no_new_order: client
                .prepare(
                    "INSERT INTO new_order (no_o_id, no_d_id, no_w_id) VALUES ($1, $2, $3)",
                )
                .await?,
            no_stock: client
                .prepare(
                    "UPDATE stock SET s_qty = s_qty - $1, s_ytd = s_ytd + $1, s_order_cnt = s_order_cnt + 1 WHERE s_w_id = $2 AND s_i_id = $3",
                )
                .await?,
            no_order_line: client
                .prepare(
                    "INSERT INTO order_line (ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_qty, ol_amount) VALUES ($1, $2, $3, 1, $4, $5, $6)",
                )
                .await?,
            pay_warehouse: client
                .prepare("UPDATE warehouse SET w_ytd = w_ytd + 1 WHERE w_id = $1")
                .await?,
            pay_district: client
                .prepare(
                    "UPDATE district SET d_ytd = d_ytd + 1 WHERE d_w_id = $1 AND d_id = $2",
                )
                .await?,
            pay_customer: client
                .prepare(
                    "UPDATE customer SET c_balance = c_balance - 1 WHERE c_w_id = $1 AND c_d_id = $2 AND c_id = $3",
                )
                .await?,
            os_oorder: client
                .prepare(
                    "SELECT * FROM oorder WHERE o_w_id = $1 AND o_d_id = $2 AND o_c_id = $3",
                )
                .await?,
            del_new_order: client
                .prepare("DELETE FROM new_order WHERE no_w_id = $1 AND no_d_id = $2")
                .await?,
            sl_stock: client
                .prepare("SELECT * FROM stock WHERE s_w_id = $1 AND s_qty < 20")
                .await?,
        };
        Ok(Self {
            client,
            prepared: Some(stmts),
        })
    }

    async fn run_prepared_kind(
        &self,
        kind: TxnKind,
        seed: u64,
        global_txn_id: u64,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let stmts = self
            .prepared
            .as_ref()
            .ok_or("prepared statements not initialized")?;
        let p = txn_params(seed, global_txn_id);
        let w_id = p.w_id;
        let d_id = p.d_id;
        let c_id = p.c_id;
        let i_id = p.i_id;
        let qty = p.qty;
        let o_id = i32::try_from(p.o_id)
            .map_err(|_| format!("o_id {} out of range for PostgreSQL INTEGER", p.o_id))?;
        let amount = qty * 10;

        self.client.batch_execute("BEGIN").await?;
        let run = async {
            match kind {
                TxnKind::NewOrder => {
                    self.client
                        .execute(&stmts.no_district, &[&w_id, &d_id])
                        .await?;
                    self.client
                        .execute(&stmts.no_oorder, &[&o_id, &d_id, &w_id, &c_id])
                        .await?;
                    self.client
                        .execute(&stmts.no_new_order, &[&o_id, &d_id, &w_id])
                        .await?;
                    self.client
                        .execute(&stmts.no_stock, &[&qty, &w_id, &i_id])
                        .await?;
                    self.client
                        .execute(
                            &stmts.no_order_line,
                            &[&o_id, &d_id, &w_id, &i_id, &qty, &amount],
                        )
                        .await?;
                }
                TxnKind::Payment => {
                    self.client.execute(&stmts.pay_warehouse, &[&w_id]).await?;
                    self.client
                        .execute(&stmts.pay_district, &[&w_id, &d_id])
                        .await?;
                    self.client
                        .execute(&stmts.pay_customer, &[&w_id, &d_id, &c_id])
                        .await?;
                }
                TxnKind::OrderStatus => {
                    self.client
                        .query(&stmts.os_oorder, &[&w_id, &d_id, &c_id])
                        .await?;
                }
                TxnKind::Delivery => {
                    self.client
                        .execute(&stmts.del_new_order, &[&w_id, &d_id])
                        .await?;
                }
                TxnKind::StockLevel => {
                    self.client.query(&stmts.sl_stock, &[&w_id]).await?;
                }
            }
            Ok::<(), tokio_postgres::Error>(())
        };
        if let Err(e) = run.await {
            let _ = self.client.batch_execute("ROLLBACK").await;
            return Err(e.into());
        }
        self.client.batch_execute("COMMIT").await?;
        Ok(())
    }
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

    async fn run_kind(
        &self,
        kind: TxnKind,
        seed: u64,
        global_txn_id: u64,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if self.prepared.is_some() {
            self.run_prepared_kind(kind, seed, global_txn_id).await
        } else {
            self.run_sql_batch(&rustdb::tpcc_workload::txn_sql(kind, seed, global_txn_id))
                .await
        }
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

fn prepared_from_env(args: &Args) -> bool {
    if args.prepared {
        return true;
    }
    matches!(
        std::env::var("POSTGRES_TPCC_PREPARED").ok().as_deref(),
        Some("1") | Some("true") | Some("TRUE") | Some("yes") | Some("YES")
    )
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let args = Args::parse();
    let mix = Mix::parse(&args.mix).map_err(|e| format!("invalid --mix: {e}"))?;
    let use_prepared = prepared_from_env(&args);

    let concurrency = args.concurrency.max(1);
    let mut workers: Vec<Arc<PgExec>> = Vec::with_capacity(concurrency);
    for _ in 0..concurrency {
        let exec = if use_prepared {
            PgExec::connect_prepared(
                &args.host,
                args.port,
                &args.user,
                &args.password,
                &args.database,
            )
            .await?
        } else {
            let client = connect_worker(
                &args.host,
                args.port,
                &args.user,
                &args.password,
                &args.database,
            )
            .await?;
            PgExec {
                client,
                prepared: None,
            }
        };
        workers.push(Arc::new(exec));
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
        println!("prepared: {use_prepared}");
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

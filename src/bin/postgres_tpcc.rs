//! TPC-C-ish throughput load generator for PostgreSQL (TCP + `tokio-postgres`).
//!
//! Uses the same statement mix and parameters as `rustdb_tpcc` (`rustdb::tpcc_workload`).
//! Schema: apply `scripts/tpcc_seed.sql` (same minimal tables as RustDB CI).
//!
//! # Round trips per transaction
//!
//! `rustdb_tpcc` sends a whole transaction in **one** network round trip (`ExecuteTpcc`, and
//! `ExecuteScript` on the SQL path). Sending `BEGIN` / each statement / `COMMIT` one at a time
//! would therefore charge PostgreSQL 3–7 round trips for the same work — and because PostgreSQL
//! holds row locks until `COMMIT`, every extra in-transaction round trip also lengthens the
//! serialized section on the hot `warehouse` / `district` rows. That is a property of the driver,
//! not of the engine, so batching is **on by default** here: each transaction is one round trip.
//!
//! `--no-batch` (or `POSTGRES_TPCC_BATCH=0`) restores the legacy statement-per-round-trip
//! behaviour, which is useful for measuring the round-trip effect on purpose.

use async_trait::async_trait;
use clap::Parser;
use rustdb::tpcc_workload::{run_tpcc, txn_params, Mix, TpccExec, TpccRunConfig, TxnKind};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio_postgres::types::ToSql;
use tokio_postgres::{Client, NoTls, Statement};

/// Bound query parameters for one prepared statement.
type Params<'a, const N: usize> = [&'a (dyn ToSql + Sync); N];

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

    /// Send every statement of a transaction in its own round trip (legacy behaviour).
    /// Default is one round trip per transaction, matching `rustdb_tpcc`.
    #[arg(long, default_value_t = false)]
    no_batch: bool,

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
    /// One network round trip per transaction (see module docs). `false` restores the
    /// legacy statement-per-round-trip behaviour.
    batched: bool,
}

/// Per-transaction parameters, already converted to the PostgreSQL column types.
struct PgTxnArgs {
    w_id: i32,
    d_id: i32,
    c_id: i32,
    i_id: i32,
    qty: i32,
    o_id: i32,
    amount: i32,
}

/// Whole `BEGIN` .. `COMMIT` transaction as one simple-query script — the same shape
/// `rustdb_tpcc` sends over `ExecuteScript`. PostgreSQL still parses, plans, executes and streams
/// the result rows for every statement; only the round trips are collapsed.
fn batched_script(sqls: &[String]) -> String {
    sqls.join(";\n")
}

fn pg_txn_args(
    seed: u64,
    global_txn_id: u64,
) -> Result<PgTxnArgs, Box<dyn std::error::Error + Send + Sync>> {
    let p = txn_params(seed, global_txn_id);
    let o_id = i32::try_from(p.o_id)
        .map_err(|_| format!("o_id {} out of range for PostgreSQL INTEGER", p.o_id))?;
    Ok(PgTxnArgs {
        w_id: p.w_id,
        d_id: p.d_id,
        c_id: p.c_id,
        i_id: p.i_id,
        qty: p.qty,
        o_id,
        amount: p.qty * 10,
    })
}

impl PgExec {
    async fn connect_prepared(
        host: &str,
        port: u16,
        user: &str,
        password: &str,
        database: &str,
        batched: bool,
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
            batched,
        })
    }

    fn stmts(&self) -> Result<&PgStmts, Box<dyn std::error::Error + Send + Sync>> {
        self.prepared
            .as_ref()
            .ok_or_else(|| "prepared statements not initialized".into())
    }

    async fn run_prepared_kind(
        &self,
        kind: TxnKind,
        seed: u64,
        global_txn_id: u64,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if self.batched {
            self.run_prepared_pipelined(kind, seed, global_txn_id).await
        } else {
            self.run_prepared_sequential(kind, seed, global_txn_id)
                .await
        }
    }

    /// One round trip per transaction: `BEGIN`, the statement body and `COMMIT` are enqueued in a
    /// single flush. `tokio::try_join!` polls its arguments in order and `tokio-postgres` enqueues
    /// a request the first time its future is polled, so the wire order matches the argument order.
    async fn run_prepared_pipelined(
        &self,
        kind: TxnKind,
        seed: u64,
        global_txn_id: u64,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let s = self.stmts()?;
        let a = pg_txn_args(seed, global_txn_id)?;
        let c = &self.client;

        // `try_join!` moves the futures it is given, so the parameter slices must outlive the
        // macro invocation — hence the explicit bindings.
        let res = match kind {
            TxnKind::NewOrder => {
                let district: Params<2> = [&a.w_id, &a.d_id];
                let oorder: Params<4> = [&a.o_id, &a.d_id, &a.w_id, &a.c_id];
                let new_order: Params<3> = [&a.o_id, &a.d_id, &a.w_id];
                let stock: Params<3> = [&a.qty, &a.w_id, &a.i_id];
                let order_line: Params<6> = [&a.o_id, &a.d_id, &a.w_id, &a.i_id, &a.qty, &a.amount];
                tokio::try_join!(
                    c.batch_execute("BEGIN"),
                    c.execute(&s.no_district, &district),
                    c.execute(&s.no_oorder, &oorder),
                    c.execute(&s.no_new_order, &new_order),
                    c.execute(&s.no_stock, &stock),
                    c.execute(&s.no_order_line, &order_line),
                    c.batch_execute("COMMIT"),
                )
                .map(|_| ())
            }
            TxnKind::Payment => {
                let warehouse: Params<1> = [&a.w_id];
                let district: Params<2> = [&a.w_id, &a.d_id];
                let customer: Params<3> = [&a.w_id, &a.d_id, &a.c_id];
                tokio::try_join!(
                    c.batch_execute("BEGIN"),
                    c.execute(&s.pay_warehouse, &warehouse),
                    c.execute(&s.pay_district, &district),
                    c.execute(&s.pay_customer, &customer),
                    c.batch_execute("COMMIT"),
                )
                .map(|_| ())
            }
            TxnKind::OrderStatus => {
                let oorder: Params<3> = [&a.w_id, &a.d_id, &a.c_id];
                tokio::try_join!(
                    c.batch_execute("BEGIN"),
                    c.query(&s.os_oorder, &oorder),
                    c.batch_execute("COMMIT"),
                )
                .map(|_| ())
            }
            TxnKind::Delivery => {
                let new_order: Params<2> = [&a.w_id, &a.d_id];
                tokio::try_join!(
                    c.batch_execute("BEGIN"),
                    c.execute(&s.del_new_order, &new_order),
                    c.batch_execute("COMMIT"),
                )
                .map(|_| ())
            }
            TxnKind::StockLevel => {
                let stock: Params<1> = [&a.w_id];
                tokio::try_join!(
                    c.batch_execute("BEGIN"),
                    c.query(&s.sl_stock, &stock),
                    c.batch_execute("COMMIT"),
                )
                .map(|_| ())
            }
        };

        if let Err(e) = res {
            // `try_join!` drops the remaining futures on the first error, so the pipelined
            // `COMMIT` may never be observed. PostgreSQL turns `COMMIT` on an aborted transaction
            // into a rollback anyway; this makes sure the session is not left inside one either
            // way (a no-op `ROLLBACK` outside a transaction is only a notice).
            let _ = self.client.batch_execute("ROLLBACK").await;
            return Err(e.into());
        }
        Ok(())
    }

    /// Legacy path (`--no-batch`): one round trip per statement.
    async fn run_prepared_sequential(
        &self,
        kind: TxnKind,
        seed: u64,
        global_txn_id: u64,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let stmts = self.stmts()?;
        let a = pg_txn_args(seed, global_txn_id)?;
        let PgTxnArgs {
            w_id,
            d_id,
            c_id,
            i_id,
            qty,
            o_id,
            amount,
        } = a;

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
        if self.batched {
            self.client.batch_execute(&batched_script(sqls)).await?;
        } else {
            for sql in sqls {
                self.client.simple_query(sql).await?;
            }
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

fn env_flag(name: &str) -> Option<bool> {
    match std::env::var(name)
        .ok()?
        .trim()
        .to_ascii_lowercase()
        .as_str()
    {
        "1" | "true" | "yes" | "on" => Some(true),
        "0" | "false" | "no" | "off" => Some(false),
        _ => None,
    }
}

fn prepared_from_env(args: &Args) -> bool {
    if args.prepared {
        return true;
    }
    env_flag("POSTGRES_TPCC_PREPARED").unwrap_or(false)
}

/// One round trip per transaction unless explicitly disabled via `--no-batch` or
/// `POSTGRES_TPCC_BATCH=0`.
fn batched_from_env(args: &Args) -> bool {
    if args.no_batch {
        return false;
    }
    env_flag("POSTGRES_TPCC_BATCH").unwrap_or(true)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let args = Args::parse();
    let mix = Mix::parse(&args.mix).map_err(|e| format!("invalid --mix: {e}"))?;
    let use_prepared = prepared_from_env(&args);
    let batched = batched_from_env(&args);

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
                batched,
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
                batched,
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
        println!("batched (1 round trip per txn): {batched}");
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

#[cfg(test)]
mod tests {
    use super::*;
    use rustdb::tpcc_workload::txn_sql;

    const KINDS: [TxnKind; 5] = [
        TxnKind::NewOrder,
        TxnKind::Payment,
        TxnKind::OrderStatus,
        TxnKind::Delivery,
        TxnKind::StockLevel,
    ];

    fn args_from(argv: &[&str]) -> Args {
        Args::parse_from(std::iter::once("postgres_tpcc").chain(argv.iter().copied()))
    }

    /// One test function: `POSTGRES_TPCC_*` is process-global state.
    #[test]
    fn batching_defaults_on_and_flag_beats_env() {
        std::env::remove_var("POSTGRES_TPCC_BATCH");
        assert!(batched_from_env(&args_from(&[])));
        assert!(!batched_from_env(&args_from(&["--no-batch"])));

        for on in ["1", "true", "YES", "on"] {
            std::env::set_var("POSTGRES_TPCC_BATCH", on);
            assert!(batched_from_env(&args_from(&[])), "{on}");
        }
        for off in ["0", "false", "NO", "off"] {
            std::env::set_var("POSTGRES_TPCC_BATCH", off);
            assert!(!batched_from_env(&args_from(&[])), "{off}");
        }

        // Unparseable value falls back to the default; `--no-batch` still wins.
        std::env::set_var("POSTGRES_TPCC_BATCH", "maybe");
        assert!(batched_from_env(&args_from(&[])));
        assert!(!batched_from_env(&args_from(&["--no-batch"])));
        std::env::remove_var("POSTGRES_TPCC_BATCH");
    }

    #[test]
    fn batched_script_is_a_single_begin_commit_block() {
        for kind in KINDS {
            let sqls = txn_sql(kind, 0xC0FF_EE00, 7);
            let script = batched_script(&sqls);
            assert!(
                script.starts_with("BEGIN TRANSACTION"),
                "{kind:?}: {script}"
            );
            assert!(script.ends_with("COMMIT"), "{kind:?}: {script}");
            // Exactly one separator per statement boundary: the whole transaction is one message,
            // so PostgreSQL pays the same number of round trips as `rustdb_tpcc` (one).
            assert_eq!(script.matches(";\n").count(), sqls.len() - 1, "{kind:?}");
            for sql in &sqls {
                assert!(script.contains(sql.as_str()), "{kind:?}: missing {sql}");
            }
        }
    }
}

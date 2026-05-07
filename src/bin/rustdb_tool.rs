use clap::{Parser, Subcommand};
use rustdb::common::DurabilityMode;
use rustdb::logging::log_record::{LogRecord, LogRecordType, TransactionId};
use rustdb::network::engine::{EngineHandle, EngineOutput, SessionContext};
use rustdb::network::sql_engine::SqlEngine;
use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;

/// Lightweight embedded tooling for a local data directory.
///
/// This binary is intentionally separate from `rustdb`'s main CLI to keep changes isolated and
/// avoid churn while the embedded API/config evolves.
#[derive(Parser, Debug)]
#[command(name = "rustdb_tool")]
#[command(about = "RustDB embedded tooling helpers")]
#[command(version)]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand, Debug)]
enum Cmd {
    /// Open the engine and run a trivial query (SELECT 1).
    Health {
        /// Data directory to open (contains `.rustdb/`).
        #[arg(long, value_name = "DIR")]
        data_dir: PathBuf,
    },
    /// Print a summary of WAL records under `data_dir/.rustdb/wal`.
    ///
    /// Output includes total record count, per-type breakdown, LSN range, and a
    /// list of transactions that look unfinished (no `COMMIT` or `ABORT` marker).
    WalStatus {
        /// Data directory to inspect.
        #[arg(long, value_name = "DIR")]
        data_dir: PathBuf,
    },
    /// Run a manual checkpoint via `SqlEngine::checkpoint()`.
    Checkpoint {
        /// Data directory to open and checkpoint.
        #[arg(long, value_name = "DIR")]
        data_dir: PathBuf,
    },
    /// Show overall durability/WAL/checkpoint diagnostics for a data directory.
    ///
    /// Opens the engine, then prints durability mode, WAL status (record count, last LSN,
    /// active transactions) and checkpoint statistics. Useful for verifying that a
    /// process is in the expected mode (safe vs fast) before/after a benchmark or crash test.
    Status {
        /// Data directory to open and inspect.
        #[arg(long, value_name = "DIR")]
        data_dir: PathBuf,
    },
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    match cli.cmd {
        Cmd::Health { data_dir } => cmd_health(data_dir),
        Cmd::WalStatus { data_dir } => cmd_wal_status(data_dir),
        Cmd::Checkpoint { data_dir } => cmd_checkpoint(data_dir),
        Cmd::Status { data_dir } => cmd_status(data_dir),
    }
}

fn cmd_health(data_dir: PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    let engine = SqlEngine::open(data_dir)?;
    let mut ctx = SessionContext::default();
    match engine.execute_sql("SELECT 1", &mut ctx)? {
        EngineOutput::ResultSet { rows, .. } => {
            println!("ok: rows={}", rows.len());
            Ok(())
        }
        other => Err(format!("unexpected output: {other:?}").into()),
    }
}

fn cmd_checkpoint(data_dir: PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    let engine = SqlEngine::open(data_dir)?;
    engine.checkpoint()?;
    println!("ok: checkpoint written");
    Ok(())
}

fn cmd_wal_status(data_dir: PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    let wal_dir = data_dir.join(".rustdb").join("wal");
    if !wal_dir.is_dir() {
        println!("wal: directory missing ({})", wal_dir.display());
        return Ok(());
    }
    let recs = LogRecord::read_log_records_from_directory(&wal_dir)?;
    print_wal_summary(&recs);
    Ok(())
}

fn cmd_status(data_dir: PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    println!("data_dir: {}", data_dir.display());
    print_durability_env();

    let engine = SqlEngine::open(data_dir.clone())?;
    println!(
        "engine: durability={} wal_enabled={}",
        durability_str(engine.durability()),
        engine.wal_enabled()
    );

    if engine.wal_enabled() {
        let wal_dir = data_dir.join(".rustdb").join("wal");
        if wal_dir.is_dir() {
            let recs = LogRecord::read_log_records_from_directory(&wal_dir)?;
            print_wal_summary(&recs);
        } else {
            println!("wal: directory missing ({})", wal_dir.display());
        }

        match engine.checkpoint_statistics() {
            Some(stats) => {
                println!(
                    "checkpoint: total={} auto={} forced={} failed={}",
                    stats.total_checkpoints,
                    stats.auto_checkpoints,
                    stats.forced_checkpoints,
                    stats.failed_checkpoints,
                );
                println!(
                    "checkpoint: last_lsn={} last_size_bytes={} last_time_unix={}",
                    stats.last_checkpoint_lsn,
                    stats.last_checkpoint_size,
                    stats.last_checkpoint_time
                );
                println!(
                    "checkpoint: avg_time_ms={} total_time_ms={} flushed_pages={}",
                    stats.average_checkpoint_time_ms,
                    stats.total_checkpoint_time_ms,
                    stats.total_flushed_pages,
                );
            }
            None => println!("checkpoint: manager not wired (disabled or WAL off)"),
        }
    } else {
        println!("wal: disabled");
        println!("checkpoint: unavailable (WAL disabled)");
    }
    Ok(())
}

fn durability_str(mode: DurabilityMode) -> &'static str {
    match mode {
        DurabilityMode::Safe => "safe",
        DurabilityMode::Fast => "fast",
    }
}

fn print_durability_env() {
    let fsync = std::env::var_os("RUSTDB_FSYNC_COMMIT").is_some();
    let no_wal = std::env::var_os("RUSTDB_DISABLE_WAL").is_some();
    let no_chk = std::env::var_os("RUSTDB_DISABLE_CHECKPOINT").is_some();
    let auto_chk = std::env::var_os("RUSTDB_AUTO_CHECKPOINT").is_some();
    println!(
        "env: RUSTDB_FSYNC_COMMIT={} RUSTDB_DISABLE_WAL={} RUSTDB_DISABLE_CHECKPOINT={} RUSTDB_AUTO_CHECKPOINT={}",
        bool_str(fsync),
        bool_str(no_wal),
        bool_str(no_chk),
        bool_str(auto_chk)
    );
}

fn bool_str(b: bool) -> &'static str {
    if b {
        "1"
    } else {
        "0"
    }
}

/// Pretty-prints WAL diagnostics: total count, per-type breakdown, LSN range,
/// and any transactions that lack a commit/abort marker.
fn print_wal_summary(recs: &[LogRecord]) {
    if recs.is_empty() {
        println!("wal: empty");
        return;
    }
    let first = &recs[0];
    let last = &recs[recs.len() - 1];
    println!("wal: records={}", recs.len());
    println!(
        "wal: lsn_range={}..{} first_type={:?} last_type={:?}",
        first.lsn, last.lsn, first.record_type, last.record_type
    );

    let mut by_type: BTreeMap<String, usize> = BTreeMap::new();
    for r in recs {
        *by_type.entry(format!("{:?}", r.record_type)).or_insert(0) += 1;
    }
    let parts: Vec<String> = by_type.iter().map(|(k, v)| format!("{k}={v}")).collect();
    println!("wal: by_type {}", parts.join(" "));

    let active = active_transactions(recs);
    if active.is_empty() {
        println!("wal: active_transactions=0");
    } else {
        let ids: Vec<String> = active.iter().map(|tid| tid.to_string()).collect();
        println!(
            "wal: active_transactions={} ids=[{}]",
            active.len(),
            ids.join(",")
        );
    }
}

/// Returns transactions seen in the WAL that are missing a `Commit` or `Abort` record.
fn active_transactions(recs: &[LogRecord]) -> Vec<TransactionId> {
    #[derive(Default)]
    struct TxState {
        seen: bool,
        committed: bool,
        aborted: bool,
    }
    let mut state: HashMap<TransactionId, TxState> = HashMap::new();
    for r in recs {
        let Some(tid) = r.transaction_id else {
            continue;
        };
        let entry = state.entry(tid).or_default();
        entry.seen = true;
        match r.record_type {
            LogRecordType::TransactionCommit => entry.committed = true,
            LogRecordType::TransactionAbort => entry.aborted = true,
            _ => {}
        }
    }
    let mut active: Vec<TransactionId> = state
        .into_iter()
        .filter(|(_, s)| s.seen && !s.committed && !s.aborted)
        .map(|(tid, _)| tid)
        .collect();
    active.sort();
    active
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustdb::common::DurabilityMode;
    use rustdb::network::sql_engine::{SqlEngine, SqlEngineConfig};
    use std::path::Path;
    use tempfile::TempDir;

    fn open_engine(dir: &Path) -> SqlEngine {
        SqlEngine::open_with_config(
            dir.to_path_buf(),
            SqlEngineConfig {
                durability: DurabilityMode::Safe,
                wal_enabled: true,
                ..SqlEngineConfig::default()
            },
        )
        .unwrap()
    }

    #[test]
    fn active_transactions_detects_open_tx() {
        let dir = TempDir::new().unwrap();
        {
            let engine = open_engine(dir.path());
            let mut ctx = SessionContext::default();
            engine.execute_sql("BEGIN TRANSACTION", &mut ctx).unwrap();
            engine
                .execute_sql("INSERT INTO t (a, b) VALUES (1, 'x')", &mut ctx)
                .unwrap();
            // Drop ctx + engine without commit/rollback: the WAL keeps BEGIN+INSERT
            // without a matching COMMIT/ABORT marker so the tx looks active.
        }

        let wal = dir.path().join(".rustdb").join("wal");
        let recs = LogRecord::read_log_records_from_directory(&wal).unwrap();
        let active = active_transactions(&recs);
        assert_eq!(
            active.len(),
            1,
            "expected exactly one active tx, got {active:?}"
        );
    }

    #[test]
    fn active_transactions_clears_on_commit() {
        let dir = TempDir::new().unwrap();
        let engine = open_engine(dir.path());
        let mut ctx = SessionContext::default();
        engine.execute_sql("BEGIN TRANSACTION", &mut ctx).unwrap();
        engine
            .execute_sql("INSERT INTO t (a, b) VALUES (1, 'x')", &mut ctx)
            .unwrap();
        engine.execute_sql("COMMIT", &mut ctx).unwrap();

        let wal = dir.path().join(".rustdb").join("wal");
        let recs = LogRecord::read_log_records_from_directory(&wal).unwrap();
        let active = active_transactions(&recs);
        assert!(
            active.is_empty(),
            "no active txs expected after COMMIT, got {active:?}"
        );
    }
}

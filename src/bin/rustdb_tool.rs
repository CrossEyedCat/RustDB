use clap::{Parser, Subcommand};
use rustdb::logging::log_record::LogRecord;
use rustdb::network::engine::{EngineHandle, EngineOutput, SessionContext};
use rustdb::network::sql_engine::SqlEngine;
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
    /// Print a minimal summary of WAL records under `data_dir/.rustdb/wal`.
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
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    match cli.cmd {
        Cmd::Health { data_dir } => cmd_health(data_dir),
        Cmd::WalStatus { data_dir } => cmd_wal_status(data_dir),
        Cmd::Checkpoint { data_dir } => cmd_checkpoint(data_dir),
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
    let recs = LogRecord::read_log_records_from_directory(&wal_dir)?;
    if recs.is_empty() {
        println!("wal: empty");
        return Ok(());
    }
    let first = &recs[0];
    let last = &recs[recs.len() - 1];
    println!("wal: records={}", recs.len());
    println!(
        "wal: first_lsn={} first_type={:?}",
        first.lsn, first.record_type
    );
    println!(
        "wal: last_lsn={} last_type={:?}",
        last.lsn, last.record_type
    );
    Ok(())
}

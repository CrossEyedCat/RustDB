//! Minimal durability hook: append a commit record on explicit `COMMIT`.
//!
//! When `RUSTDB_FSYNC_COMMIT=1` is set, the log line is followed by `fsync`.

use crate::common::Result;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

/// Append one line to `data_dir/.rustdb/commits.log` for a committed transaction.
pub fn append_commit_log_line(data_dir: &Path) -> Result<()> {
    let dir = data_dir.join(".rustdb");
    std::fs::create_dir_all(&dir)?;
    let path = dir.join("commits.log");
    let ts_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0);
    let line = format!("commit {ts_ms}\n");
    let mut file = OpenOptions::new().create(true).append(true).open(path)?;
    file.write_all(line.as_bytes())?;
    if matches!(std::env::var("RUSTDB_FSYNC_COMMIT").as_deref(), Ok("1")) {
        file.sync_all()?;
    }
    Ok(())
}

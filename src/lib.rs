//! rustdb - Relational database implementation in Rust
//!
//! This module provides core functionality for working with a relational database,
//! including data management, SQL parsing, query execution, and transactions.

#![allow(clippy::absurd_extreme_comparisons)]
#![allow(clippy::assertions_on_constants)]
#![allow(clippy::useless_vec)]
#![allow(clippy::field_reassign_with_default)]
#![allow(clippy::bool_assert_comparison)]
#![allow(clippy::approx_constant)]
#![allow(clippy::new_without_default)]
#![allow(clippy::unwrap_or_default)]
#![allow(clippy::should_implement_trait)]
#![allow(clippy::only_used_in_recursion)]
#![allow(clippy::collapsible_match)]
#![allow(clippy::bool_comparison)]
#![allow(clippy::derivable_impls)]
#![allow(clippy::type_complexity)]
#![allow(clippy::never_loop)]
#![allow(clippy::while_immutable_condition)]
#![allow(clippy::redundant_closure)]
#![allow(clippy::redundant_pattern_matching)]
#![allow(clippy::collapsible_if)]
#![allow(clippy::single_char_add_str)]
#![allow(clippy::useless_format)]
#![allow(clippy::too_many_arguments)]
#![allow(clippy::module_inception)]
#![allow(clippy::unnecessary_cast)]
#![allow(clippy::borrowed_box)]
#![allow(clippy::manual_map)]
#![allow(clippy::suspicious_open_options)]
#![allow(clippy::needless_range_loop)]
#![allow(clippy::len_zero)]
#![allow(clippy::manual_range_contains)]
#![allow(clippy::unnecessary_unwrap)]
#![allow(clippy::implicit_saturating_sub)]
#![allow(clippy::match_like_matches_macro)]
#![allow(clippy::needless_borrows_for_generic_args)]
#![allow(unused_imports)]
#![allow(unused_variables)]
#![allow(dead_code)]
#![allow(unreachable_code)]
#![allow(unused_mut)]
#![allow(unused_doc_comments)]
#![allow(unused_comparisons)]
#![allow(unused_must_use)]

pub mod analyzer;
pub mod catalog;
pub mod cli;
pub mod common;
pub mod core;
pub mod debug;
pub mod executor;
pub mod logging;
pub mod network;
pub mod parser;
pub mod planner;
pub mod storage;
pub mod tracing_setup;

pub use network::SqlEngine;

pub use common::error::{Error, Result};
pub use common::types::*;

use std::path::PathBuf;

/// Library version
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Main database handle (data directory and lifecycle).
pub struct Database {
    data_path: Option<PathBuf>,
}

impl Database {
    /// Creates a new in-memory database handle (no persistent directory until [`Self::open`]).
    pub fn new() -> Result<Self> {
        Ok(Self { data_path: None })
    }

    /// Opens or creates a database directory at `path`.
    pub fn open(path: &str) -> Result<Self> {
        let p = PathBuf::from(path);
        std::fs::create_dir_all(&p)
            .map_err(|e| Error::database(format!("create_dir {}: {}", path, e)))?;
        Ok(Self { data_path: Some(p) })
    }

    /// Active data directory, if any.
    pub fn path(&self) -> Option<&std::path::Path> {
        self.data_path.as_deref()
    }

    /// Releases resources and clears the handle.
    pub fn close(&mut self) -> Result<()> {
        self.data_path = None;
        Ok(())
    }
}

impl Drop for Database {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

#[cfg(test)]
mod crate_tests {
    use super::{Database, Error, Result, VERSION};

    #[test]
    #[allow(clippy::const_is_empty)] // CARGO_PKG_VERSION is non-empty; check kept explicit
    fn test_version_constant() {
        assert!(!VERSION.is_empty());
    }

    #[test]
    fn test_database_new_open_close() -> Result<()> {
        let mut db = Database::new()?;
        assert!(db.path().is_none());
        let tmp = std::env::temp_dir().join("rustdb_lib_test_open");
        let mut on_disk =
            Database::open(tmp.to_str().ok_or_else(|| Error::database("temp path"))?)?;
        assert_eq!(on_disk.path(), Some(tmp.as_path()));
        let _ = Database::open(tmp.to_str().ok_or_else(|| Error::database("temp path"))?)?;
        on_disk.close()?;
        assert!(on_disk.path().is_none());
        db.close()?;
        Ok(())
    }
}

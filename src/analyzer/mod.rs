//! Semantic analyzer for rustdb
//!
//! This module is responsible for semantic checking of SQL queries,
//! including checking object existence, type compatibility,
//! access rights and other semantic rules.

pub mod access_checker;
pub mod metadata_cache;
pub mod object_checker;
pub mod semantic_analyzer;
pub mod type_checker;

#[cfg(test)]
pub mod tests;

// Re-export main types
pub use access_checker::{AccessCheckResult, AccessChecker, Permission};
pub use metadata_cache::{CacheEntry, MetadataCache};
pub use object_checker::{ObjectCheckResult, ObjectChecker};
pub use semantic_analyzer::{
    AnalysisContext, AnalysisResult, SemanticAnalyzer, SemanticAnalyzerSettings,
};
pub use type_checker::{TypeCheckResult, TypeChecker, TypeCompatibility};

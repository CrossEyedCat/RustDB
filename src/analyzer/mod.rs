//! Семантический анализатор для rustdb
//!
//! Этот модуль отвечает за семантическую проверку SQL запросов,
//! включая проверку существования объектов, совместимости типов,
//! прав доступа и другие семантические правила.

pub mod access_checker;
pub mod metadata_cache;
pub mod object_checker;
pub mod semantic_analyzer;
pub mod type_checker;

#[cfg(test)]
pub mod tests;

// Переэкспортируем основные типы
pub use access_checker::{AccessCheckResult, AccessChecker, Permission};
pub use metadata_cache::{CacheEntry, MetadataCache};
pub use object_checker::{ObjectCheckResult, ObjectChecker};
pub use semantic_analyzer::{
    AnalysisContext, AnalysisResult, SemanticAnalyzer, SemanticAnalyzerSettings,
};
pub use type_checker::{TypeCheckResult, TypeChecker, TypeCompatibility};

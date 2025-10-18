//! Семантический анализатор для rustdb
//! 
//! Этот модуль отвечает за семантическую проверку SQL запросов,
//! включая проверку существования объектов, совместимости типов,
//! прав доступа и другие семантические правила.

pub mod semantic_analyzer;
pub mod object_checker;
pub mod type_checker;
pub mod access_checker;
pub mod metadata_cache;

#[cfg(test)]
pub mod tests;

// Переэкспортируем основные типы
pub use semantic_analyzer::{SemanticAnalyzer, SemanticAnalyzerSettings, AnalysisContext, AnalysisResult};
pub use object_checker::{ObjectChecker, ObjectCheckResult};
pub use type_checker::{TypeChecker, TypeCheckResult, TypeCompatibility};
pub use access_checker::{AccessChecker, AccessCheckResult, Permission};
pub use metadata_cache::{MetadataCache, CacheEntry};


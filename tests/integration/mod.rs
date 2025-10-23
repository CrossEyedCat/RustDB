//! Интеграционные тесты для RustBD
//!
//! Этот модуль содержит тесты, которые проверяют взаимодействие
//! между различными компонентами системы.

pub mod benchmark_tests;
pub mod common;
pub mod full_cycle_tests;
pub mod stress_tests;
pub mod transaction_tests;

// Re-export common utilities
pub use common::*;

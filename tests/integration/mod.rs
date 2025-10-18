//! Интеграционные тесты для RustBD
//! 
//! Этот модуль содержит тесты, которые проверяют взаимодействие
//! между различными компонентами системы.

pub mod full_cycle_tests;
pub mod transaction_tests;
pub mod benchmark_tests;
pub mod stress_tests;
pub mod common;

// Re-export common utilities
pub use common::*;


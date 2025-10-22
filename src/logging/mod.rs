//! Система логирования rustdb
//!
//! Этот модуль содержит полную систему логирования для базы данных:
//! - Write-Ahead Logging (WAL) для обеспечения ACID свойств
//! - Структурированные лог-записи с различными типами операций
//! - Буферизованная запись логов с использованием оптимизированного I/O
//! - Система восстановления данных из логов
//! - Контрольные точки и сжатие логов
//! - Мониторинг и метрики производительности

pub mod checkpoint;
pub mod compaction;
pub mod log_record;
pub mod log_writer;
pub mod metrics;
pub mod recovery;
pub mod wal;

#[cfg(test)]
pub mod tests;

pub use checkpoint::*;
pub use compaction::*;
pub use log_record::*;
pub use log_writer::*;
pub use metrics::*;
pub use recovery::*;
pub use wal::*;

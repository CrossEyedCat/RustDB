//! Система логирования RustBD
//!
//! Этот модуль содержит полную систему логирования для базы данных:
//! - Write-Ahead Logging (WAL) для обеспечения ACID свойств
//! - Структурированные лог-записи с различными типами операций
//! - Буферизованная запись логов с использованием оптимизированного I/O
//! - Система восстановления данных из логов
//! - Контрольные точки и сжатие логов
//! - Мониторинг и метрики производительности

pub mod log_record;
pub mod log_writer;
pub mod wal;
pub mod recovery;
pub mod checkpoint;
pub mod compaction;
pub mod metrics;



pub use log_record::*;
pub use log_writer::*;
pub use wal::*;
pub use recovery::*;
pub use checkpoint::*;
pub use compaction::*;
pub use metrics::*;

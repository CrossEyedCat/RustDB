//! Каталог метаданных rustdb

pub mod access;
pub mod schema;
pub mod statistics;

#[cfg(test)]
mod tests;

pub use statistics::{
    StatisticsManager, StatisticsSettings, TableStatistics, ColumnStatistics,
    ValueDistribution, HistogramBucket, ColumnValue
};

// TODO: Реализовать каталог метаданных

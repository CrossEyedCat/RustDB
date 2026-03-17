//! Metadata catalog for rustdb

pub mod access;
pub mod schema;
pub mod statistics;

#[cfg(test)]
mod tests;

pub use statistics::{
    ColumnStatistics, ColumnValue, HistogramBucket, StatisticsManager, StatisticsSettings,
    TableStatistics, ValueDistribution,
};

// TODO: Implement metadata catalog

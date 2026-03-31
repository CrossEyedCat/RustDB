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

pub use access::AccessControl;
pub use schema::SchemaManager;

/// Bundles catalog-facing subsystems (schema + access control hooks).
#[derive(Debug, Default)]
pub struct MetadataCatalog {
    pub schema: SchemaManager,
    pub access: AccessControl,
}

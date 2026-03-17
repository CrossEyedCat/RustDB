//! Statistics manager for rustdb

use crate::common::{Error, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::SystemTime;

/// Table statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableStatistics {
    /// Table name
    pub table_name: String,
    /// Total number of rows
    pub total_rows: usize,
    /// Table size in bytes
    pub total_size_bytes: usize,
    /// Last statistics update time
    pub last_updated: SystemTime,
    /// Column statistics
    pub column_statistics: HashMap<String, ColumnStatistics>,
}

/// Column statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnStatistics {
    /// Column name
    pub column_name: String,
    /// Number of distinct values
    pub distinct_values: usize,
    /// Number of NULL values
    pub null_count: usize,
    /// Minimum value
    pub min_value: Option<ColumnValue>,
    /// Maximum value
    pub max_value: Option<ColumnValue>,
    /// Average value length (for strings)
    pub avg_length: Option<f64>,
    /// Value distribution histogram
    pub value_distribution: ValueDistribution,
}

/// Value distribution in a column
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ValueDistribution {
    /// Uniform distribution
    Uniform {
        /// Step between values
        step: f64,
    },
    /// Normal distribution
    Normal {
        /// Mean value
        mean: f64,
        /// Standard deviation
        std_dev: f64,
    },
    /// Histogram with buckets
    Histogram {
        /// Histogram buckets
        buckets: Vec<HistogramBucket>,
    },
    /// Unknown distribution
    Unknown,
}

/// Histogram bucket
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HistogramBucket {
    /// Lower bound of value
    pub lower_bound: f64,
    /// Upper bound of value
    pub upper_bound: f64,
    /// Number of values in bucket
    pub count: usize,
    /// Number of distinct values in bucket
    pub distinct_count: usize,
}

/// Column value for statistics
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ColumnValue {
    /// Integer
    Integer(i64),
    /// Floating point number
    Float(f64),
    /// String
    String(String),
    /// Boolean value
    Boolean(bool),
    /// NULL value
    Null,
}

/// Statistics manager
pub struct StatisticsManager {
    /// Table statistics cache
    table_statistics: HashMap<String, TableStatistics>,
    /// Statistics collection settings
    settings: StatisticsSettings,
    /// Last update time
    last_update: SystemTime,
}

/// Statistics collection settings
#[derive(Debug, Clone)]
pub struct StatisticsSettings {
    /// Automatically update statistics
    pub auto_update: bool,
    /// Statistics update interval in seconds
    pub update_interval_seconds: u64,
    /// Maximum number of buckets in histogram
    pub max_histogram_buckets: usize,
    /// Enable detailed logging
    pub enable_debug_logging: bool,
}

impl Default for StatisticsSettings {
    fn default() -> Self {
        Self {
            auto_update: true,
            update_interval_seconds: 3600, // 1 hour
            max_histogram_buckets: 100,
            enable_debug_logging: false,
        }
    }
}

impl StatisticsManager {
    /// Create new statistics manager
    pub fn new() -> Result<Self> {
        Ok(Self {
            table_statistics: HashMap::new(),
            settings: StatisticsSettings::default(),
            last_update: SystemTime::now(),
        })
    }

    /// Create statistics manager with settings
    pub fn with_settings(settings: StatisticsSettings) -> Result<Self> {
        Ok(Self {
            table_statistics: HashMap::new(), // Initialize with empty cache
            settings,
            last_update: SystemTime::now(),
        })
    }

    /// Collect statistics for a table
    pub fn collect_table_statistics(&mut self, table_name: &str) -> Result<TableStatistics> {
        // In real implementation, statistics would be collected from table files
        // For demonstration, create dummy statistics

        let mut column_stats = HashMap::new();

        // Create statistics for typical columns
        column_stats.insert(
            "id".to_string(),
            self.create_column_statistics("id", 10000, 0, 1, 10000),
        );
        column_stats.insert(
            "name".to_string(),
            self.create_column_statistics("name", 9500, 500, 0, 0),
        );
        column_stats.insert(
            "age".to_string(),
            self.create_column_statistics("age", 100, 0, 18, 65),
        );
        column_stats.insert(
            "salary".to_string(),
            self.create_column_statistics("salary", 1000, 0, 30000, 150000),
        );

        let table_stats = TableStatistics {
            table_name: table_name.to_string(),
            total_rows: 10000,
            total_size_bytes: 1024 * 1024, // 1MB
            last_updated: SystemTime::now(),
            column_statistics: column_stats,
        };

        self.table_statistics
            .insert(table_name.to_string(), table_stats.clone());
        Ok(table_stats)
    }

    /// Create column statistics
    pub fn create_column_statistics(
        &self,
        column_name: &str,
        distinct: usize,
        nulls: usize,
        min_val: i64,
        max_val: i64,
    ) -> ColumnStatistics {
        let value_distribution = if column_name == "id" {
            ValueDistribution::Uniform { step: 1.0 }
        } else if column_name == "age" {
            ValueDistribution::Normal {
                mean: 35.0,
                std_dev: 10.0,
            }
        } else {
            ValueDistribution::Unknown
        };

        ColumnStatistics {
            column_name: column_name.to_string(),
            distinct_values: distinct,
            null_count: nulls,
            min_value: Some(ColumnValue::Integer(min_val)),
            max_value: Some(ColumnValue::Integer(max_val)),
            avg_length: None,
            value_distribution,
        }
    }

    /// Get table statistics
    pub fn get_table_statistics(&self, table_name: &str) -> Option<&TableStatistics> {
        self.table_statistics.get(table_name)
    }

    /// Estimate selectivity of condition for column
    pub fn estimate_selectivity(
        &self,
        table_name: &str,
        column_name: &str,
        condition: &str,
    ) -> Result<f64> {
        if let Some(table_stats) = self.get_table_statistics(table_name) {
            if let Some(column_stats) = table_stats.column_statistics.get(column_name) {
                return self.calculate_selectivity(column_stats, condition);
            }
        }

        // If statistics are unavailable, return conservative estimate
        Ok(0.1)
    }

    /// Calculate condition selectivity
    fn calculate_selectivity(
        &self,
        column_stats: &ColumnStatistics,
        condition: &str,
    ) -> Result<f64> {
        // Simplified implementation - in real system would parse conditions here
        match condition {
            "=" => Ok(1.0 / column_stats.distinct_values as f64),
            "!=" => Ok(1.0 - (1.0 / column_stats.distinct_values as f64)),
            "<" | "<=" | ">" | ">=" => Ok(0.5), // Approximate estimate for ranges
            "LIKE" => Ok(0.1),                  // Approximate estimate for LIKE
            "IN" => Ok(0.2),                    // Approximate estimate for IN
            _ => Ok(0.1),                       // Conservative default estimate
        }
    }

    /// Estimate number of result rows for condition
    pub fn estimate_result_rows(
        &self,
        table_name: &str,
        column_name: &str,
        condition: &str,
    ) -> Result<usize> {
        if let Some(table_stats) = self.get_table_statistics(table_name) {
            let selectivity = self.estimate_selectivity(table_name, column_name, condition)?;
            let estimated_rows = (table_stats.total_rows as f64 * selectivity).round() as usize;
            Ok(estimated_rows)
        } else {
            // If statistics are unavailable, return conservative estimate
            Ok(table_name.len()) // Simple heuristic
        }
    }

    /// Update table statistics
    pub fn update_table_statistics(&mut self, table_name: &str) -> Result<()> {
        self.collect_table_statistics(table_name)?;
        self.last_update = SystemTime::now();
        Ok(())
    }

    /// Get all statistics
    pub fn get_all_statistics(&self) -> &HashMap<String, TableStatistics> {
        &self.table_statistics
    }

    /// Clear statistics cache
    pub fn clear_cache(&mut self) {
        self.table_statistics.clear();
    }

    /// Get settings
    pub fn settings(&self) -> &StatisticsSettings {
        &self.settings
    }

    /// Update settings
    pub fn update_settings(&mut self, settings: StatisticsSettings) {
        self.settings = settings;
    }

    /// Check if statistics should be updated
    pub fn should_update_statistics(&self) -> bool {
        if !self.settings.auto_update {
            return false;
        }

        if let Ok(elapsed) = self.last_update.elapsed() {
            elapsed.as_secs() >= self.settings.update_interval_seconds
        } else {
            true
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_statistics_manager_creation() {
        let manager = StatisticsManager::new();
        assert!(manager.is_ok());
    }

    #[test]
    fn test_statistics_manager_with_settings() {
        let settings = StatisticsSettings {
            auto_update: false,
            update_interval_seconds: 7200,
            max_histogram_buckets: 200,
            enable_debug_logging: true,
        };

        let manager = StatisticsManager::with_settings(settings);
        assert!(manager.is_ok());

        let manager = manager.unwrap();
        assert_eq!(manager.settings().auto_update, false);
        assert_eq!(manager.settings().update_interval_seconds, 7200);
        assert_eq!(manager.settings().max_histogram_buckets, 200);
        assert_eq!(manager.settings().enable_debug_logging, true);
    }

    #[test]
    fn test_table_statistics_collection() {
        let mut manager = StatisticsManager::new().unwrap();
        let stats = manager.collect_table_statistics("users");
        assert!(stats.is_ok());

        let stats = stats.unwrap();
        assert_eq!(stats.table_name, "users");
        assert_eq!(stats.total_rows, 10000);
        assert_eq!(stats.total_size_bytes, 1024 * 1024);
        assert!(stats.column_statistics.contains_key("id"));
        assert!(stats.column_statistics.contains_key("name"));
        assert!(stats.column_statistics.contains_key("age"));
        assert!(stats.column_statistics.contains_key("salary"));
    }

    #[test]
    fn test_column_statistics_creation() {
        let manager = StatisticsManager::new().unwrap();

        // Test creating statistics for id column
        let id_stats = manager.create_column_statistics("id", 10000, 0, 1, 10000);
        assert_eq!(id_stats.column_name, "id");
        assert_eq!(id_stats.distinct_values, 10000);
        assert_eq!(id_stats.null_count, 0);
        assert_eq!(id_stats.min_value, Some(ColumnValue::Integer(1)));
        assert_eq!(id_stats.max_value, Some(ColumnValue::Integer(10000)));

        match id_stats.value_distribution {
            ValueDistribution::Uniform { step } => assert_eq!(step, 1.0),
            _ => panic!("Expected uniform distribution for id"),
        }

        // Test creating statistics for age column
        let age_stats = manager.create_column_statistics("age", 100, 0, 18, 65);
        assert_eq!(age_stats.column_name, "age");
        assert_eq!(age_stats.distinct_values, 100);
        assert_eq!(age_stats.min_value, Some(ColumnValue::Integer(18)));
        assert_eq!(age_stats.max_value, Some(ColumnValue::Integer(65)));

        match age_stats.value_distribution {
            ValueDistribution::Normal { mean, std_dev } => {
                assert_eq!(mean, 35.0);
                assert_eq!(std_dev, 10.0);
            }
            _ => panic!("Expected normal distribution for age"),
        }
    }

    #[test]
    fn test_selectivity_estimation() {
        let mut manager = StatisticsManager::new().unwrap();
        manager.collect_table_statistics("users").unwrap();

        // Test selectivity estimation for various operators
        let selectivity_eq = manager.estimate_selectivity("users", "id", "=").unwrap();
        assert!(selectivity_eq > 0.0);
        assert!(selectivity_eq <= 1.0);

        let selectivity_neq = manager.estimate_selectivity("users", "id", "!=").unwrap();
        assert!(selectivity_eq > 0.0);
        assert!(selectivity_neq <= 1.0);

        let selectivity_range = manager.estimate_selectivity("users", "age", ">").unwrap();
        assert_eq!(selectivity_range, 0.5);

        let selectivity_like = manager
            .estimate_selectivity("users", "name", "LIKE")
            .unwrap();
        assert_eq!(selectivity_like, 0.1);

        let selectivity_in = manager.estimate_selectivity("users", "id", "IN").unwrap();
        assert_eq!(selectivity_in, 0.2);

        let selectivity_unknown = manager
            .estimate_selectivity("users", "id", "UNKNOWN")
            .unwrap();
        assert_eq!(selectivity_unknown, 0.1);
    }

    #[test]
    fn test_result_rows_estimation() {
        let mut manager = StatisticsManager::new().unwrap();
        manager.collect_table_statistics("users").unwrap();

        // Test result row count estimation
        let rows_eq = manager.estimate_result_rows("users", "id", "=").unwrap();
        assert!(rows_eq > 0);
        assert!(rows_eq <= 10000);

        let rows_range = manager.estimate_result_rows("users", "age", ">").unwrap();
        assert!(rows_range > 0);
        assert!(rows_range <= 10000);

        // Test case with non-existent table
        let rows_unknown = manager
            .estimate_result_rows("unknown_table", "id", "=")
            .unwrap();
        assert_eq!(rows_unknown, "unknown_table".len());
    }

    #[test]
    fn test_statistics_update() {
        let mut manager = StatisticsManager::new().unwrap();

        // Collect statistics
        let stats1 = manager.collect_table_statistics("users").unwrap();
        let last_updated1 = stats1.last_updated;

        // Update statistics
        manager.update_table_statistics("users").unwrap();
        let stats2 = manager.get_table_statistics("users").unwrap();
        let last_updated2 = stats2.last_updated;

        // Update time should change
        assert!(last_updated2 > last_updated1);
    }

    #[test]
    fn test_statistics_cache() {
        let mut manager = StatisticsManager::new().unwrap();

        // Collect statistics for multiple tables
        manager.collect_table_statistics("users").unwrap();
        manager.collect_table_statistics("orders").unwrap();

        // Check that statistics are cached
        assert!(manager.get_table_statistics("users").is_some());
        assert!(manager.get_table_statistics("orders").is_some());

        // Clear cache
        manager.clear_cache();

        // Check that cache is cleared
        assert!(manager.get_table_statistics("users").is_none());
        assert!(manager.get_table_statistics("orders").is_none());
    }

    #[test]
    fn test_statistics_settings_update() {
        let mut manager = StatisticsManager::new().unwrap();
        let original_settings = manager.settings().clone();

        // Create new settings
        let new_settings = StatisticsSettings {
            auto_update: false,
            update_interval_seconds: 1800,
            max_histogram_buckets: 150,
            enable_debug_logging: true,
        };

        // Update settings
        manager.update_settings(new_settings.clone());

        // Check that settings updated
        assert_eq!(manager.settings().auto_update, new_settings.auto_update);
        assert_eq!(
            manager.settings().update_interval_seconds,
            new_settings.update_interval_seconds
        );
        assert_eq!(
            manager.settings().max_histogram_buckets,
            new_settings.max_histogram_buckets
        );
        assert_eq!(
            manager.settings().enable_debug_logging,
            new_settings.enable_debug_logging
        );

        // Check that settings differ from original
        assert_ne!(
            manager.settings().auto_update,
            original_settings.auto_update
        );
        assert_ne!(
            manager.settings().update_interval_seconds,
            original_settings.update_interval_seconds
        );
    }

    #[test]
    fn test_should_update_statistics() {
        let mut manager = StatisticsManager::new().unwrap();

        // By default auto_update = true
        assert!(manager.settings().auto_update);

        // Immediately after creation should not require update
        assert!(!manager.should_update_statistics());

        // Disable auto-update
        let settings = StatisticsSettings {
            auto_update: false,
            ..Default::default()
        };
        manager.update_settings(settings);

        // With auto-update disabled should not require update
        assert!(!manager.should_update_statistics());
    }

    #[test]
    fn test_value_distribution_variants() {
        // Test all value distribution variants

        let uniform = ValueDistribution::Uniform { step: 2.5 };
        match uniform {
            ValueDistribution::Uniform { step } => assert_eq!(step, 2.5),
            _ => panic!("Expected uniform distribution"),
        }

        let normal = ValueDistribution::Normal {
            mean: 42.0,
            std_dev: 5.0,
        };
        match normal {
            ValueDistribution::Normal { mean, std_dev } => {
                assert_eq!(mean, 42.0);
                assert_eq!(std_dev, 5.0);
            }
            _ => panic!("Expected normal distribution"),
        }

        let histogram = ValueDistribution::Histogram {
            buckets: vec![HistogramBucket {
                lower_bound: 0.0,
                upper_bound: 10.0,
                count: 100,
                distinct_count: 50,
            }],
        };
        match histogram {
            ValueDistribution::Histogram { buckets } => {
                assert_eq!(buckets.len(), 1);
                assert_eq!(buckets[0].lower_bound, 0.0);
                assert_eq!(buckets[0].upper_bound, 10.0);
                assert_eq!(buckets[0].count, 100);
                assert_eq!(buckets[0].distinct_count, 50);
            }
            _ => panic!("Expected histogram"),
        }

        let unknown = ValueDistribution::Unknown;
        match unknown {
            ValueDistribution::Unknown => (), // OK
            _ => panic!("Expected unknown distribution"),
        }
    }

    #[test]
    fn test_column_value_variants() {
        // Test all column value variants

        let int_val = ColumnValue::Integer(42);
        match int_val {
            ColumnValue::Integer(val) => assert_eq!(val, 42),
            _ => panic!("Expected integer"),
        }

        let float_val = ColumnValue::Float(3.14);
        match float_val {
            ColumnValue::Float(val) => assert_eq!(val, 3.14),
            _ => panic!("Expected floating point number"),
        }

        let string_val = ColumnValue::String("test".to_string());
        match string_val {
            ColumnValue::String(val) => assert_eq!(val, "test"),
            _ => panic!("Expected string"),
        }

        let bool_val = ColumnValue::Boolean(true);
        match bool_val {
            ColumnValue::Boolean(val) => assert_eq!(val, true),
            _ => panic!("Expected boolean value"),
        }

        let null_val = ColumnValue::Null;
        match null_val {
            ColumnValue::Null => (), // OK
            _ => panic!("Expected NULL value"),
        }
    }
}

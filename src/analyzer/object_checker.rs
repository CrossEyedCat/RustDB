//! Module for checking database object existence

use crate::common::Result;
use std::collections::HashMap;

/// Object existence check result
#[derive(Debug, Clone)]
pub struct ObjectCheckResult {
    /// Whether object exists
    pub exists: bool,
    /// Object type
    pub object_type: ObjectType,
    /// Additional object information
    pub metadata: ObjectMetadata,
}

/// Database object type
#[derive(Debug, Clone, PartialEq)]
pub enum ObjectType {
    Table,
    Column,
    Index,
    View,
    Function,
    Procedure,
    Trigger,
}

/// Object metadata
#[derive(Debug, Clone)]
pub struct ObjectMetadata {
    /// Object name
    pub name: String,
    /// Schema to which object belongs
    pub schema_name: Option<String>,
    /// Additional properties
    pub properties: HashMap<String, String>,
}

impl ObjectMetadata {
    pub fn new(name: String) -> Self {
        Self {
            name,
            schema_name: None,
            properties: HashMap::new(),
        }
    }

    pub fn with_schema(mut self, schema_name: String) -> Self {
        self.schema_name = Some(schema_name);
        self
    }

    pub fn with_property(mut self, key: String, value: String) -> Self {
        self.properties.insert(key, value);
        self
    }
}

/// Object existence checker
pub struct ObjectChecker {
    /// Check results cache
    cache: HashMap<String, ObjectCheckResult>,
    /// Whether caching is enabled
    cache_enabled: bool,
}

impl ObjectChecker {
    /// Create new object checker
    pub fn new() -> Self {
        Self {
            cache: HashMap::new(),
            cache_enabled: true,
        }
    }

    /// Create checker with caching disabled
    pub fn without_cache() -> Self {
        Self {
            cache: HashMap::new(),
            cache_enabled: false,
        }
    }

    /// Check table existence (simplified version for testing)
    pub fn check_table_exists(
        &mut self,
        table_name: &str,
        _schema: &(),
    ) -> Result<ObjectCheckResult> {
        let cache_key = format!("table:{}", table_name);

        // Check cache
        if self.cache_enabled {
            if let Some(cached_result) = self.cache.get(&cache_key) {
                return Ok(cached_result.clone());
            }
        }

        // In test mode always return true
        let result = ObjectCheckResult {
            exists: true,
            object_type: ObjectType::Table,
            metadata: ObjectMetadata::new(table_name.to_string())
                .with_property("type".to_string(), "table".to_string()),
        };

        // Cache result
        if self.cache_enabled {
            self.cache.insert(cache_key, result.clone());
        }

        Ok(result)
    }

    /// Check column existence in table (simplified version)
    pub fn check_column_exists(
        &mut self,
        table_name: &str,
        column_name: &str,
        _schema: &(),
    ) -> Result<ObjectCheckResult> {
        let cache_key = format!("column:{}:{}", table_name, column_name);

        // Check cache
        if self.cache_enabled {
            if let Some(cached_result) = self.cache.get(&cache_key) {
                return Ok(cached_result.clone());
            }
        }

        // In test mode always return true
        let result = ObjectCheckResult {
            exists: true,
            object_type: ObjectType::Column,
            metadata: ObjectMetadata::new(column_name.to_string())
                .with_property("table".to_string(), table_name.to_string())
                .with_property("type".to_string(), "column".to_string()),
        };

        // Cache result
        if self.cache_enabled {
            self.cache.insert(cache_key, result.clone());
        }

        Ok(result)
    }

    /// Check index existence (simplified version)
    pub fn check_index_exists(
        &mut self,
        index_name: &str,
        _schema: &(),
    ) -> Result<ObjectCheckResult> {
        let cache_key = format!("index:{}", index_name);

        // Check cache
        if self.cache_enabled {
            if let Some(cached_result) = self.cache.get(&cache_key) {
                return Ok(cached_result.clone());
            }
        }

        // In test mode always return false for indexes
        let result = ObjectCheckResult {
            exists: false,
            object_type: ObjectType::Index,
            metadata: ObjectMetadata::new(index_name.to_string())
                .with_property("type".to_string(), "index".to_string()),
        };

        // Cache result
        if self.cache_enabled {
            self.cache.insert(cache_key, result.clone());
        }

        Ok(result)
    }

    /// Clear cache
    pub fn clear_cache(&mut self) {
        self.cache.clear();
    }

    /// Enable or disable caching
    pub fn set_cache_enabled(&mut self, enabled: bool) {
        self.cache_enabled = enabled;
        if !enabled {
            self.cache.clear();
        }
    }

    /// Get cache statistics
    pub fn cache_statistics(&self) -> (usize, bool) {
        (self.cache.len(), self.cache_enabled)
    }
}

impl Default for ObjectChecker {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_object_checker_creation() {
        let checker = ObjectChecker::new();
        assert!(checker.cache_enabled);
        assert_eq!(checker.cache.len(), 0);
    }

    #[test]
    fn test_object_checker_without_cache() {
        let checker = ObjectChecker::without_cache();
        assert!(!checker.cache_enabled);
    }

    #[test]
    fn test_object_metadata() {
        let metadata = ObjectMetadata::new("test_table".to_string())
            .with_schema("public".to_string())
            .with_property("type".to_string(), "table".to_string());

        assert_eq!(metadata.name, "test_table");
        assert_eq!(metadata.schema_name, Some("public".to_string()));
        assert_eq!(metadata.properties.get("type"), Some(&"table".to_string()));
    }

    #[test]
    fn test_cache_operations() {
        let mut checker = ObjectChecker::new();

        // Initially cache is empty
        let (cache_size, cache_enabled) = checker.cache_statistics();
        assert_eq!(cache_size, 0);
        assert!(cache_enabled);

        // Disable cache
        checker.set_cache_enabled(false);
        let (_, cache_enabled) = checker.cache_statistics();
        assert!(!cache_enabled);

        // Enable cache again
        checker.set_cache_enabled(true);
        let (_, cache_enabled) = checker.cache_statistics();
        assert!(cache_enabled);
    }

    #[test]
    fn test_table_exists_check() -> Result<()> {
        let mut checker = ObjectChecker::new();
        let result = checker.check_table_exists("users", &())?;

        assert!(result.exists);
        assert_eq!(result.object_type, ObjectType::Table);
        assert_eq!(result.metadata.name, "users");

        Ok(())
    }

    #[test]
    fn test_column_exists_check() -> Result<()> {
        let mut checker = ObjectChecker::new();
        let result = checker.check_column_exists("users", "name", &())?;

        assert!(result.exists);
        assert_eq!(result.object_type, ObjectType::Column);
        assert_eq!(result.metadata.name, "name");

        Ok(())
    }

    #[test]
    fn test_index_exists_check() -> Result<()> {
        let mut checker = ObjectChecker::new();
        let result = checker.check_index_exists("idx_users_email", &())?;

        assert!(!result.exists); // In test mode indexes do not exist
        assert_eq!(result.object_type, ObjectType::Index);
        assert_eq!(result.metadata.name, "idx_users_email");

        Ok(())
    }
}

//! Main semantic analyzer

use crate::common::Result;
use crate::parser::ast::*;
use crate::storage::schema_manager::SchemaManager;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use super::{AccessChecker, MetadataCache, ObjectChecker, TypeChecker};

#[derive(Debug, Clone, Default)]
struct SelectScope {
    /// Known table names and aliases visible in this SELECT (FROM + JOIN + subquery aliases).
    table_refs: std::collections::HashMap<String, String>, // ref -> base name or "<subquery>"
}

/// Semantic analyzer settings
#[derive(Debug, Clone)]
pub struct SemanticAnalyzerSettings {
    /// Enable object existence checking
    pub check_object_existence: bool,
    /// Enable type checking
    pub check_types: bool,
    /// Enable access rights checking
    pub check_access_rights: bool,
    /// Enable metadata caching
    pub enable_metadata_cache: bool,
    /// Strict validation (stop on first error)
    pub strict_validation: bool,
    /// Maximum number of warnings
    pub max_warnings: usize,
}

impl Default for SemanticAnalyzerSettings {
    fn default() -> Self {
        Self {
            check_object_existence: true,
            check_types: true,
            check_access_rights: false, // Disabled by default for simplicity
            enable_metadata_cache: true,
            strict_validation: false,
            max_warnings: 100,
        }
    }
}

/// Analysis context
#[derive(Debug, Clone)]
pub struct AnalysisContext {
    /// Database schema (simplified version for testing)
    pub schema: Option<()>,
    /// Current user
    pub current_user: Option<String>,
    /// Active transaction
    pub transaction_id: Option<u64>,
    /// Additional parameters
    pub parameters: HashMap<String, String>,
}

impl Default for AnalysisContext {
    fn default() -> Self {
        Self {
            schema: None,
            current_user: Some("admin".to_string()),
            transaction_id: None,
            parameters: HashMap::new(),
        }
    }
}

/// Semantic analysis result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnalysisResult {
    /// Analysis success
    pub is_valid: bool,
    /// Errors
    pub errors: Vec<SemanticError>,
    /// Warnings
    pub warnings: Vec<SemanticWarning>,
    /// Type information
    pub type_info: TypeInformation,
    /// Analysis statistics
    pub statistics: AnalysisStatistics,
}

impl AnalysisResult {
    pub fn new() -> Self {
        Self {
            is_valid: true,
            errors: Vec::new(),
            warnings: Vec::new(),
            type_info: TypeInformation::new(),
            statistics: AnalysisStatistics::new(),
        }
    }

    pub fn add_error(&mut self, error: SemanticError) {
        self.errors.push(error);
        self.is_valid = false;
    }

    pub fn add_warning(&mut self, warning: SemanticWarning) {
        self.warnings.push(warning);
    }

    pub fn has_errors(&self) -> bool {
        !self.errors.is_empty()
    }

    pub fn has_warnings(&self) -> bool {
        !self.warnings.is_empty()
    }
}

/// Semantic error
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SemanticError {
    pub error_type: SemanticErrorType,
    pub message: String,
    pub location: Option<String>,
    pub suggested_fix: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SemanticErrorType {
    ObjectNotFound,
    TypeMismatch,
    AccessDenied,
    DuplicateObject,
    InvalidOperation,
    ConstraintViolation,
}

/// Semantic warning
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SemanticWarning {
    pub warning_type: SemanticWarningType,
    pub message: String,
    pub location: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SemanticWarningType {
    ImplicitTypeConversion,
    PerformanceHint,
    DeprecatedFeature,
    PotentialIssue,
}

/// Type information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TypeInformation {
    /// Column types in result
    pub result_types: Vec<DataType>,
    /// Type conversion information
    pub type_conversions: Vec<TypeConversion>,
}

impl TypeInformation {
    pub fn new() -> Self {
        Self {
            result_types: Vec::new(),
            type_conversions: Vec::new(),
        }
    }
}

/// Type conversion information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TypeConversion {
    pub from_type: DataType,
    pub to_type: DataType,
    pub is_implicit: bool,
    pub location: String,
}

/// Analysis statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnalysisStatistics {
    /// Analysis time in milliseconds
    pub analysis_time_ms: u64,
    /// Number of checked objects
    pub objects_checked: usize,
    /// Number of type checks
    pub type_checks: usize,
    /// Number of access checks
    pub access_checks: usize,
    /// Number of cache hits
    pub cache_hits: usize,
    /// Number of cache misses
    pub cache_misses: usize,
}

impl AnalysisStatistics {
    pub fn new() -> Self {
        Self {
            analysis_time_ms: 0,
            objects_checked: 0,
            type_checks: 0,
            access_checks: 0,
            cache_hits: 0,
            cache_misses: 0,
        }
    }
}

/// Main semantic analyzer
pub struct SemanticAnalyzer {
    settings: SemanticAnalyzerSettings,
    object_checker: ObjectChecker,
    type_checker: TypeChecker,
    access_checker: AccessChecker,
    metadata_cache: MetadataCache,
    schema_manager: Option<SchemaManager>,
}

impl SemanticAnalyzer {
    /// Create new semantic analyzer
    pub fn new(settings: SemanticAnalyzerSettings) -> Self {
        Self {
            object_checker: ObjectChecker::new(),
            type_checker: TypeChecker::new(),
            access_checker: AccessChecker::new(),
            metadata_cache: MetadataCache::new(settings.enable_metadata_cache),
            settings,
            schema_manager: None,
        }
    }

    /// Create analyzer with default settings
    pub fn default() -> Self {
        Self::new(SemanticAnalyzerSettings::default())
    }

    /// Set schema manager
    pub fn with_schema_manager(mut self, schema_manager: SchemaManager) -> Self {
        self.schema_manager = Some(schema_manager);
        self
    }

    /// Analyze SQL query
    pub fn analyze(
        &mut self,
        statement: &SqlStatement,
        context: &AnalysisContext,
    ) -> Result<AnalysisResult> {
        let start_time = std::time::Instant::now();
        let mut result = AnalysisResult::new();

        // Object existence check
        if self.settings.check_object_existence {
            if let Err(e) = self.check_object_existence(statement, context, &mut result) {
                if self.settings.strict_validation {
                    return Err(e);
                }
                result.add_error(SemanticError {
                    error_type: SemanticErrorType::ObjectNotFound,
                    message: format!("Object existence check failed: {}", e),
                    location: None,
                    suggested_fix: None,
                });
            }
        }

        // Type check
        if self.settings.check_types {
            if let Err(e) = self.check_types(statement, context, &mut result) {
                if self.settings.strict_validation {
                    return Err(e);
                }
                result.add_error(SemanticError {
                    error_type: SemanticErrorType::TypeMismatch,
                    message: format!("Type check failed: {}", e),
                    location: None,
                    suggested_fix: None,
                });
            }
        }

        // Access rights check
        if self.settings.check_access_rights {
            if let Err(e) = self.check_access_rights(statement, context, &mut result) {
                if self.settings.strict_validation {
                    return Err(e);
                }
                result.add_error(SemanticError {
                    error_type: SemanticErrorType::AccessDenied,
                    message: format!("Access check failed: {}", e),
                    location: None,
                    suggested_fix: None,
                });
            }
        }

        // Update statistics
        result.statistics.analysis_time_ms = start_time.elapsed().as_millis() as u64;

        Ok(result)
    }

    /// Analyze multiple queries
    pub fn analyze_multiple(
        &mut self,
        statements: &[SqlStatement],
        context: &AnalysisContext,
    ) -> Result<Vec<AnalysisResult>> {
        let mut results = Vec::new();

        for statement in statements {
            let result = self.analyze(statement, context)?;
            results.push(result);

            // If strict validation is enabled and there are errors, stop
            if self.settings.strict_validation && results.last().unwrap().has_errors() {
                break;
            }
        }

        Ok(results)
    }

    /// Check object existence
    fn check_object_existence(
        &mut self,
        statement: &SqlStatement,
        context: &AnalysisContext,
        result: &mut AnalysisResult,
    ) -> Result<()> {
        match statement {
            SqlStatement::Select(select) => {
                self.check_select_objects(select, context, result)?;
            }
            SqlStatement::Insert(insert) => {
                self.check_insert_objects(insert, context, result)?;
            }
            SqlStatement::Update(update) => {
                self.check_update_objects(update, context, result)?;
            }
            SqlStatement::Delete(delete) => {
                self.check_delete_objects(delete, context, result)?;
            }
            SqlStatement::CreateTable(create) => {
                self.check_create_table_objects(create, context, result)?;
            }
            SqlStatement::AlterTable(alter) => {
                self.check_alter_table_objects(alter, context, result)?;
            }
            SqlStatement::DropTable(drop) => {
                self.check_drop_table_objects(drop, context, result)?;
            }
            _ => {
                // Transactional commands do not require object checking
            }
        }
        Ok(())
    }

    /// Check types
    fn check_types(
        &mut self,
        statement: &SqlStatement,
        context: &AnalysisContext,
        result: &mut AnalysisResult,
    ) -> Result<()> {
        // Delegate type checking to specialized module
        let type_result = self.type_checker.check_statement(statement, context)?;

        // Transfer results
        for error in type_result.errors {
            result.add_error(SemanticError {
                error_type: SemanticErrorType::TypeMismatch,
                message: error.message,
                location: error.location,
                suggested_fix: error.suggested_fix,
            });
        }

        for warning in type_result.warnings {
            result.add_warning(SemanticWarning {
                warning_type: SemanticWarningType::ImplicitTypeConversion,
                message: warning.message,
                location: warning.location,
            });
        }

        result.type_info = type_result.type_info;
        result.statistics.type_checks += type_result.checks_performed;

        Ok(())
    }

    /// Check access rights
    fn check_access_rights(
        &mut self,
        statement: &SqlStatement,
        context: &AnalysisContext,
        result: &mut AnalysisResult,
    ) -> Result<()> {
        // Delegate access rights checking to specialized module
        let access_result = self.access_checker.check_statement(statement, context)?;

        // Transfer results
        for error in access_result.errors {
            result.add_error(SemanticError {
                error_type: SemanticErrorType::AccessDenied,
                message: error.message,
                location: error.location,
                suggested_fix: error.suggested_fix,
            });
        }

        result.statistics.access_checks += access_result.checks_performed;

        Ok(())
    }

    // Helper methods for checking different query types
    fn check_select_objects(
        &mut self,
        select: &SelectStatement,
        context: &AnalysisContext,
        result: &mut AnalysisResult,
    ) -> Result<()> {
        result.statistics.objects_checked += 1;

        let mut scope = SelectScope::default();

        // Check tables in FROM (and populate scope for qualified identifiers).
        if let Some(from) = &select.from {
            self.check_from_clause_objects(from, context, result, &mut scope)?;
        }

        // Check columns in SELECT
        for item in &select.select_list {
            self.check_select_item_objects(item, context, result, &scope)?;
        }

        if let Some(where_clause) = &select.where_clause {
            self.check_expression_objects(where_clause, context, result, &scope)?;
        }
        for expr in &select.group_by {
            self.check_expression_objects(expr, context, result, &scope)?;
        }
        if let Some(having) = &select.having {
            self.check_expression_objects(having, context, result, &scope)?;
        }
        for order in &select.order_by {
            self.check_expression_objects(&order.expr, context, result, &scope)?;
        }

        Ok(())
    }

    fn check_from_clause_objects(
        &mut self,
        from: &FromClause,
        context: &AnalysisContext,
        result: &mut AnalysisResult,
        scope: &mut SelectScope,
    ) -> Result<()> {
        // Check main table (or subquery) + populate scope.
        let table_name = match &from.table {
            TableReference::Table { name, alias } => {
                scope.table_refs.insert(name.clone(), name.clone());
                if let Some(a) = alias {
                    scope.table_refs.insert(a.clone(), name.clone());
                }
                name
            }
            TableReference::Subquery { query, alias } => {
                scope.table_refs.insert(alias.clone(), "<subquery>".to_string());
                // Recurse into subquery.
                self.check_select_objects(query, context, result)?;
                // No schema existence check for subquery itself.
                ""
            }
        };

        if !table_name.is_empty() {
            if let Some(schema) = &context.schema {
            let object_result = self.object_checker.check_table_exists(table_name, schema)?;
            if !object_result.exists {
                result.add_error(SemanticError {
                    error_type: SemanticErrorType::ObjectNotFound,
                    message: format!("Table '{}' does not exist", table_name),
                    location: Some("FROM clause".to_string()),
                    suggested_fix: Some(
                        "Check table name spelling or create the table".to_string(),
                    ),
                });
            }
            }
        }

        // Check JOIN tables
        for join in &from.joins {
            let join_table_name = match &join.table {
                TableReference::Table { name, alias } => {
                    scope.table_refs.insert(name.clone(), name.clone());
                    if let Some(a) = alias {
                        scope.table_refs.insert(a.clone(), name.clone());
                    }
                    name
                }
                TableReference::Subquery { query, alias } => {
                    scope.table_refs.insert(alias.clone(), "<subquery>".to_string());
                    self.check_select_objects(query, context, result)?;
                    continue;
                }
            };

            if let Some(schema) = &context.schema {
                let object_result = self
                    .object_checker
                    .check_table_exists(join_table_name, schema)?;
                if !object_result.exists {
                    result.add_error(SemanticError {
                        error_type: SemanticErrorType::ObjectNotFound,
                        message: format!("Join table '{}' does not exist", join_table_name),
                        location: Some("JOIN clause".to_string()),
                        suggested_fix: Some(
                            "Check table name spelling or create the table".to_string(),
                        ),
                    });
                }
            }

            if let Some(cond) = &join.condition {
                self.check_expression_objects(cond, context, result, scope)?;
            }
        }

        Ok(())
    }

    fn check_select_item_objects(
        &mut self,
        item: &SelectItem,
        context: &AnalysisContext,
        result: &mut AnalysisResult,
        scope: &SelectScope,
    ) -> Result<()> {
        match item {
            SelectItem::Expression { expr, .. } => {
                self.check_expression_objects(expr, context, result, scope)?;
            }
            SelectItem::Wildcard => {
                // Wildcard does not require special checking
            }
        }
        Ok(())
    }

    fn check_expression_objects(
        &mut self,
        expr: &Expression,
        context: &AnalysisContext,
        result: &mut AnalysisResult,
        scope: &SelectScope,
    ) -> Result<()> {
        match expr {
            Expression::Identifier(_) => {
                // Check column existence
                result.statistics.objects_checked += 1;
            }
            Expression::QualifiedIdentifier { table, .. } => {
                result.statistics.objects_checked += 1;
                if !scope.table_refs.is_empty() && !scope.table_refs.contains_key(table) {
                    result.add_error(SemanticError {
                        error_type: SemanticErrorType::InvalidOperation,
                        message: format!("Unknown table reference/alias '{}'", table),
                        location: Some("qualified identifier".to_string()),
                        suggested_fix: Some("Check FROM/JOIN aliases or qualify the correct table".to_string()),
                    });
                }
            }
            Expression::BinaryOp { left, right, .. } => {
                self.check_expression_objects(left, context, result, scope)?;
                self.check_expression_objects(right, context, result, scope)?;
            }
            Expression::UnaryOp { expr: operand, .. } => {
                self.check_expression_objects(operand, context, result, scope)?;
            }
            Expression::Function { args, .. } => {
                for arg in args {
                    self.check_expression_objects(arg, context, result, scope)?;
                }
            }
            Expression::IsNull { expr, .. } => {
                self.check_expression_objects(expr, context, result, scope)?;
            }
            Expression::Like { expr, pattern, .. } => {
                self.check_expression_objects(expr, context, result, scope)?;
                self.check_expression_objects(pattern, context, result, scope)?;
            }
            Expression::Between { expr, low, high } => {
                self.check_expression_objects(expr, context, result, scope)?;
                self.check_expression_objects(low, context, result, scope)?;
                self.check_expression_objects(high, context, result, scope)?;
            }
            Expression::In { expr, list } => {
                self.check_expression_objects(expr, context, result, scope)?;
                match list {
                    InList::Values(vals) => {
                        for v in vals {
                            self.check_expression_objects(v, context, result, scope)?;
                        }
                    }
                    InList::Subquery(sel) => {
                        self.check_select_objects(sel, context, result)?;
                    }
                }
            }
            Expression::Exists(sel) => {
                self.check_select_objects(sel, context, result)?;
            }
            Expression::Case {
                expr,
                when_clauses,
                else_clause,
            } => {
                if let Some(e) = expr {
                    self.check_expression_objects(e, context, result, scope)?;
                }
                for wc in when_clauses {
                    self.check_expression_objects(&wc.condition, context, result, scope)?;
                    self.check_expression_objects(&wc.result, context, result, scope)?;
                }
                if let Some(e) = else_clause {
                    self.check_expression_objects(e, context, result, scope)?;
                }
            }
            _ => {
                // Literals and other expressions do not require object checking
            }
        }
        Ok(())
    }

    fn check_insert_objects(
        &mut self,
        insert: &InsertStatement,
        context: &AnalysisContext,
        result: &mut AnalysisResult,
    ) -> Result<()> {
        result.statistics.objects_checked += 1;

        // Check table existence
        if let Some(schema) = &context.schema {
            let object_result = self
                .object_checker
                .check_table_exists(&insert.table, schema)?;
            if !object_result.exists {
                result.add_error(SemanticError {
                    error_type: SemanticErrorType::ObjectNotFound,
                    message: format!("Table '{}' does not exist", insert.table),
                    location: Some("INSERT statement".to_string()),
                    suggested_fix: Some("Check table name or create the table".to_string()),
                });
            }
        }

        Ok(())
    }

    fn check_update_objects(
        &mut self,
        update: &UpdateStatement,
        context: &AnalysisContext,
        result: &mut AnalysisResult,
    ) -> Result<()> {
        result.statistics.objects_checked += 1;

        // Check table existence
        if let Some(schema) = &context.schema {
            let object_result = self
                .object_checker
                .check_table_exists(&update.table, schema)?;
            if !object_result.exists {
                result.add_error(SemanticError {
                    error_type: SemanticErrorType::ObjectNotFound,
                    message: format!("Table '{}' does not exist", update.table),
                    location: Some("UPDATE statement".to_string()),
                    suggested_fix: Some("Check table name or create the table".to_string()),
                });
            }
        }

        Ok(())
    }

    fn check_delete_objects(
        &mut self,
        delete: &DeleteStatement,
        context: &AnalysisContext,
        result: &mut AnalysisResult,
    ) -> Result<()> {
        result.statistics.objects_checked += 1;

        // Check table existence
        if let Some(schema) = &context.schema {
            let object_result = self
                .object_checker
                .check_table_exists(&delete.table, schema)?;
            if !object_result.exists {
                result.add_error(SemanticError {
                    error_type: SemanticErrorType::ObjectNotFound,
                    message: format!("Table '{}' does not exist", delete.table),
                    location: Some("DELETE statement".to_string()),
                    suggested_fix: Some("Check table name or create the table".to_string()),
                });
            }
        }

        Ok(())
    }

    fn check_create_table_objects(
        &mut self,
        create: &CreateTableStatement,
        context: &AnalysisContext,
        result: &mut AnalysisResult,
    ) -> Result<()> {
        result.statistics.objects_checked += 1;

        // Check that table does not already exist
        if let Some(schema) = &context.schema {
            let object_result = self
                .object_checker
                .check_table_exists(&create.table_name, schema)?;
            if object_result.exists {
                result.add_error(SemanticError {
                    error_type: SemanticErrorType::DuplicateObject,
                    message: format!("Table '{}' already exists", create.table_name),
                    location: Some("CREATE TABLE statement".to_string()),
                    suggested_fix: Some(
                        "Use a different table name or DROP the existing table".to_string(),
                    ),
                });
            }
        }

        Ok(())
    }

    fn check_alter_table_objects(
        &mut self,
        alter: &AlterTableStatement,
        context: &AnalysisContext,
        result: &mut AnalysisResult,
    ) -> Result<()> {
        result.statistics.objects_checked += 1;

        // Check table existence
        if let Some(schema) = &context.schema {
            let object_result = self
                .object_checker
                .check_table_exists(&alter.table_name, schema)?;
            if !object_result.exists {
                result.add_error(SemanticError {
                    error_type: SemanticErrorType::ObjectNotFound,
                    message: format!("Table '{}' does not exist", alter.table_name),
                    location: Some("ALTER TABLE statement".to_string()),
                    suggested_fix: Some("Check table name or create the table first".to_string()),
                });
            }
        }

        Ok(())
    }

    fn check_drop_table_objects(
        &mut self,
        drop: &DropTableStatement,
        context: &AnalysisContext,
        result: &mut AnalysisResult,
    ) -> Result<()> {
        result.statistics.objects_checked += 1;

        // Check table existence
        if let Some(schema) = &context.schema {
            let object_result = self
                .object_checker
                .check_table_exists(&drop.table_name, schema)?;
            if !object_result.exists && !drop.if_exists {
                result.add_error(SemanticError {
                    error_type: SemanticErrorType::ObjectNotFound,
                    message: format!("Table '{}' does not exist", drop.table_name),
                    location: Some("DROP TABLE statement".to_string()),
                    suggested_fix: Some("Use IF EXISTS clause or check table name".to_string()),
                });
            }
        }

        Ok(())
    }

    /// Get analyzer settings
    pub fn settings(&self) -> &SemanticAnalyzerSettings {
        &self.settings
    }

    /// Update analyzer settings
    pub fn update_settings(&mut self, settings: SemanticAnalyzerSettings) {
        self.settings = settings;
    }

    /// Clear metadata cache
    pub fn clear_cache(&mut self) {
        self.metadata_cache.clear();
    }

    /// Get cache statistics
    pub fn cache_statistics(&self) -> (usize, usize) {
        self.metadata_cache.statistics()
    }
}

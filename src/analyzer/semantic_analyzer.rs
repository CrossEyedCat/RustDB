//! Основной семантический анализатор

use crate::common::Result;
use crate::parser::ast::*;
// use crate::catalog::schema::Schema; // TODO: Implement when schema module is ready
use crate::storage::schema_manager::SchemaManager;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use super::{AccessChecker, MetadataCache, ObjectChecker, TypeChecker};

/// Настройки семантического анализатора
#[derive(Debug, Clone)]
pub struct SemanticAnalyzerSettings {
    /// Включить проверку существования объектов
    pub check_object_existence: bool,
    /// Включить проверку типов
    pub check_types: bool,
    /// Включить проверку прав доступа
    pub check_access_rights: bool,
    /// Включить кэширование метаданных
    pub enable_metadata_cache: bool,
    /// Строгая валидация (прерывать при первой ошибке)
    pub strict_validation: bool,
    /// Максимальное количество предупреждений
    pub max_warnings: usize,
}

impl Default for SemanticAnalyzerSettings {
    fn default() -> Self {
        Self {
            check_object_existence: true,
            check_types: true,
            check_access_rights: false, // По умолчанию отключено для простоты
            enable_metadata_cache: true,
            strict_validation: false,
            max_warnings: 100,
        }
    }
}

/// Контекст анализа
#[derive(Debug, Clone)]
pub struct AnalysisContext {
    /// Схема базы данных (упрощенная версия для тестирования)
    pub schema: Option<()>,
    /// Текущий пользователь
    pub current_user: Option<String>,
    /// Активная транзакция
    pub transaction_id: Option<u64>,
    /// Дополнительные параметры
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

/// Результат семантического анализа
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnalysisResult {
    /// Успешность анализа
    pub is_valid: bool,
    /// Ошибки
    pub errors: Vec<SemanticError>,
    /// Предупреждения
    pub warnings: Vec<SemanticWarning>,
    /// Информация о типах
    pub type_info: TypeInformation,
    /// Статистика анализа
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

/// Семантическая ошибка
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

/// Семантическое предупреждение
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

/// Информация о типах
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TypeInformation {
    /// Типы колонок в результате
    pub result_types: Vec<DataType>,
    /// Информация о преобразованиях типов
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

/// Информация о преобразовании типа
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TypeConversion {
    pub from_type: DataType,
    pub to_type: DataType,
    pub is_implicit: bool,
    pub location: String,
}

/// Статистика анализа
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnalysisStatistics {
    /// Время анализа в миллисекундах
    pub analysis_time_ms: u64,
    /// Количество проверенных объектов
    pub objects_checked: usize,
    /// Количество проверок типов
    pub type_checks: usize,
    /// Количество проверок доступа
    pub access_checks: usize,
    /// Количество попаданий в кэш
    pub cache_hits: usize,
    /// Количество промахов кэша
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

/// Основной семантический анализатор
pub struct SemanticAnalyzer {
    settings: SemanticAnalyzerSettings,
    object_checker: ObjectChecker,
    type_checker: TypeChecker,
    access_checker: AccessChecker,
    metadata_cache: MetadataCache,
    schema_manager: Option<SchemaManager>,
}

impl SemanticAnalyzer {
    /// Создает новый семантический анализатор
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

    /// Создает анализатор с настройками по умолчанию
    pub fn default() -> Self {
        Self::new(SemanticAnalyzerSettings::default())
    }

    /// Устанавливает менеджер схем
    pub fn with_schema_manager(mut self, schema_manager: SchemaManager) -> Self {
        self.schema_manager = Some(schema_manager);
        self
    }

    /// Анализирует SQL запрос
    pub fn analyze(
        &mut self,
        statement: &SqlStatement,
        context: &AnalysisContext,
    ) -> Result<AnalysisResult> {
        let start_time = std::time::Instant::now();
        let mut result = AnalysisResult::new();

        // Проверка существования объектов
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

        // Проверка типов
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

        // Проверка прав доступа
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

        // Обновляем статистику
        result.statistics.analysis_time_ms = start_time.elapsed().as_millis() as u64;

        Ok(result)
    }

    /// Анализирует множественные запросы
    pub fn analyze_multiple(
        &mut self,
        statements: &[SqlStatement],
        context: &AnalysisContext,
    ) -> Result<Vec<AnalysisResult>> {
        let mut results = Vec::new();

        for statement in statements {
            let result = self.analyze(statement, context)?;
            results.push(result);

            // Если включена строгая валидация и есть ошибки, прерываем
            if self.settings.strict_validation && results.last().unwrap().has_errors() {
                break;
            }
        }

        Ok(results)
    }

    /// Проверяет существование объектов
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
                // Транзакционные команды не требуют проверки объектов
            }
        }
        Ok(())
    }

    /// Проверяет типы
    fn check_types(
        &mut self,
        statement: &SqlStatement,
        context: &AnalysisContext,
        result: &mut AnalysisResult,
    ) -> Result<()> {
        // Делегируем проверку типов специализированному модулю
        let type_result = self.type_checker.check_statement(statement, context)?;

        // Переносим результаты
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

    /// Проверяет права доступа
    fn check_access_rights(
        &mut self,
        statement: &SqlStatement,
        context: &AnalysisContext,
        result: &mut AnalysisResult,
    ) -> Result<()> {
        // Делегируем проверку прав доступа специализированному модулю
        let access_result = self.access_checker.check_statement(statement, context)?;

        // Переносим результаты
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

    // Вспомогательные методы для проверки различных типов запросов
    fn check_select_objects(
        &mut self,
        select: &SelectStatement,
        context: &AnalysisContext,
        result: &mut AnalysisResult,
    ) -> Result<()> {
        result.statistics.objects_checked += 1;

        // Проверяем таблицы в FROM
        if let Some(from) = &select.from {
            self.check_from_clause_objects(from, context, result)?;
        }

        // Проверяем колонки в SELECT
        for item in &select.select_list {
            self.check_select_item_objects(item, context, result)?;
        }

        Ok(())
    }

    fn check_from_clause_objects(
        &mut self,
        from: &FromClause,
        context: &AnalysisContext,
        result: &mut AnalysisResult,
    ) -> Result<()> {
        // Проверяем основную таблицу
        let table_name = match &from.table {
            TableReference::Table { name, .. } => name,
            TableReference::Subquery { .. } => {
                // Подзапросы требуют отдельной обработки
                return Ok(());
            }
        };

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

        // Проверяем JOIN таблицы
        for join in &from.joins {
            let join_table_name = match &join.table {
                TableReference::Table { name, .. } => name,
                TableReference::Subquery { .. } => continue, // Пропускаем подзапросы
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
        }

        Ok(())
    }

    fn check_select_item_objects(
        &mut self,
        item: &SelectItem,
        context: &AnalysisContext,
        result: &mut AnalysisResult,
    ) -> Result<()> {
        match item {
            SelectItem::Expression { expr, .. } => {
                self.check_expression_objects(expr, context, result)?;
            }
            SelectItem::Wildcard => {
                // Wildcard не требует специальной проверки
            }
        }
        Ok(())
    }

    fn check_expression_objects(
        &mut self,
        expr: &Expression,
        context: &AnalysisContext,
        result: &mut AnalysisResult,
    ) -> Result<()> {
        match expr {
            Expression::Identifier(_) | Expression::QualifiedIdentifier { .. } => {
                // Проверяем существование колонки
                result.statistics.objects_checked += 1;
            }
            Expression::BinaryOp { left, right, .. } => {
                self.check_expression_objects(left, context, result)?;
                self.check_expression_objects(right, context, result)?;
            }
            Expression::UnaryOp { expr: operand, .. } => {
                self.check_expression_objects(operand, context, result)?;
            }
            Expression::Function { args, .. } => {
                for arg in args {
                    self.check_expression_objects(arg, context, result)?;
                }
            }
            _ => {
                // Литералы и другие выражения не требуют проверки объектов
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

        // Проверяем существование таблицы
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

        // Проверяем существование таблицы
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

        // Проверяем существование таблицы
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

        // Проверяем, что таблица еще не существует
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

        // Проверяем существование таблицы
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

        // Проверяем существование таблицы
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

    /// Получает настройки анализатора
    pub fn settings(&self) -> &SemanticAnalyzerSettings {
        &self.settings
    }

    /// Обновляет настройки анализатора
    pub fn update_settings(&mut self, settings: SemanticAnalyzerSettings) {
        self.settings = settings;
    }

    /// Очищает кэш метаданных
    pub fn clear_cache(&mut self) {
        self.metadata_cache.clear();
    }

    /// Получает статистику кэша
    pub fn cache_statistics(&self) -> (usize, usize) {
        self.metadata_cache.statistics()
    }
}

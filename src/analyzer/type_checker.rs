//! Модуль для проверки совместимости типов данных

use crate::analyzer::semantic_analyzer::{TypeConversion, TypeInformation};
use crate::common::Result;
use crate::parser::ast::*;
use std::collections::HashMap;

/// Результат проверки типов
#[derive(Debug, Clone)]
pub struct TypeCheckResult {
    /// Успешность проверки
    pub is_valid: bool,
    /// Ошибки типов
    pub errors: Vec<TypeCheckError>,
    /// Предупреждения
    pub warnings: Vec<TypeCheckWarning>,
    /// Информация о типах
    pub type_info: TypeInformation,
    /// Количество выполненных проверок
    pub checks_performed: usize,
}

impl TypeCheckResult {
    pub fn new() -> Self {
        Self {
            is_valid: true,
            errors: Vec::new(),
            warnings: Vec::new(),
            type_info: TypeInformation::new(),
            checks_performed: 0,
        }
    }

    pub fn add_error(&mut self, error: TypeCheckError) {
        self.errors.push(error);
        self.is_valid = false;
    }

    pub fn add_warning(&mut self, warning: TypeCheckWarning) {
        self.warnings.push(warning);
    }
}

/// Ошибка проверки типов
#[derive(Debug, Clone)]
pub struct TypeCheckError {
    pub message: String,
    pub location: Option<String>,
    pub expected_type: Option<DataType>,
    pub actual_type: Option<DataType>,
    pub suggested_fix: Option<String>,
}

/// Предупреждение проверки типов
#[derive(Debug, Clone)]
pub struct TypeCheckWarning {
    pub message: String,
    pub location: Option<String>,
    pub warning_type: TypeWarningType,
}

/// Тип предупреждения
#[derive(Debug, Clone)]
pub enum TypeWarningType {
    ImplicitConversion,
    PrecisionLoss,
    PerformanceImpact,
}

/// Совместимость типов
#[derive(Debug, Clone, PartialEq)]
pub enum TypeCompatibility {
    /// Типы полностью совместимы
    Compatible,
    /// Типы совместимы с неявным преобразованием
    CompatibleWithConversion,
    /// Типы совместимы с потерей точности
    CompatibleWithLoss,
    /// Типы несовместимы
    Incompatible,
}

/// Проверщик типов
pub struct TypeChecker {
    /// Правила совместимости типов
    compatibility_rules: HashMap<(DataType, DataType), TypeCompatibility>,
    /// Правила неявных преобразований
    conversion_rules: HashMap<DataType, Vec<DataType>>,
    /// Включена ли строгая проверка типов
    strict_mode: bool,
}

impl TypeChecker {
    /// Создает новый проверщик типов
    pub fn new() -> Self {
        let mut checker = Self {
            compatibility_rules: HashMap::new(),
            conversion_rules: HashMap::new(),
            strict_mode: false,
        };

        checker.initialize_rules();
        checker
    }

    /// Создает проверщик типов в строгом режиме
    pub fn strict() -> Self {
        let mut checker = Self::new();
        checker.strict_mode = true;
        checker
    }

    /// Проверяет типы в SQL запросе
    pub fn check_statement(
        &mut self,
        statement: &SqlStatement,
        _context: &super::AnalysisContext,
    ) -> Result<TypeCheckResult> {
        let mut result = TypeCheckResult::new();

        match statement {
            SqlStatement::Select(select) => {
                self.check_select_types(select, &mut result)?;
            }
            SqlStatement::Insert(insert) => {
                self.check_insert_types(insert, &mut result)?;
            }
            SqlStatement::Update(update) => {
                self.check_update_types(update, &mut result)?;
            }
            SqlStatement::Delete(delete) => {
                self.check_delete_types(delete, &mut result)?;
            }
            SqlStatement::CreateTable(create) => {
                self.check_create_table_types(create, &mut result)?;
            }
            _ => {
                // Другие типы запросов пока не требуют проверки типов
            }
        }

        Ok(result)
    }

    /// Проверяет совместимость двух типов
    pub fn check_compatibility(
        &self,
        from_type: &DataType,
        to_type: &DataType,
    ) -> TypeCompatibility {
        // Если типы одинаковые, они совместимы
        if from_type == to_type {
            return TypeCompatibility::Compatible;
        }

        // Проверяем правила совместимости
        if let Some(compatibility) = self
            .compatibility_rules
            .get(&(from_type.clone(), to_type.clone()))
        {
            return compatibility.clone();
        }

        // Проверяем возможность неявного преобразования
        if let Some(conversions) = self.conversion_rules.get(from_type) {
            if conversions.contains(to_type) {
                return TypeCompatibility::CompatibleWithConversion;
            }
        }

        // По умолчанию типы несовместимы
        TypeCompatibility::Incompatible
    }

    /// Проверяет возможность неявного преобразования
    pub fn can_convert_implicitly(&self, from_type: &DataType, to_type: &DataType) -> bool {
        let compatibility = self.check_compatibility(from_type, to_type);
        matches!(
            compatibility,
            TypeCompatibility::Compatible | TypeCompatibility::CompatibleWithConversion
        )
    }

    /// Получает результирующий тип для бинарной операции
    pub fn get_binary_operation_result_type(
        &self,
        left_type: &DataType,
        right_type: &DataType,
        operator: &BinaryOperator,
    ) -> Result<DataType> {
        match operator {
            BinaryOperator::Add
            | BinaryOperator::Subtract
            | BinaryOperator::Multiply
            | BinaryOperator::Divide => self.get_arithmetic_result_type(left_type, right_type),
            BinaryOperator::Equal
            | BinaryOperator::NotEqual
            | BinaryOperator::LessThan
            | BinaryOperator::LessThanOrEqual
            | BinaryOperator::GreaterThan
            | BinaryOperator::GreaterThanOrEqual => Ok(DataType::Boolean),
            BinaryOperator::And | BinaryOperator::Or => {
                // Логические операторы требуют булевых операндов
                if matches!(left_type, DataType::Boolean) && matches!(right_type, DataType::Boolean)
                {
                    Ok(DataType::Boolean)
                } else {
                    Err(crate::common::Error::semantic_analysis(
                        "Logical operators require boolean operands".to_string(),
                    ))
                }
            }
            _ => {
                // Для других операторов возвращаем булевый тип по умолчанию
                Ok(DataType::Boolean)
            }
        }
    }

    /// Получает результирующий тип для унарной операции
    pub fn get_unary_operation_result_type(
        &self,
        operand_type: &DataType,
        operator: &UnaryOperator,
    ) -> Result<DataType> {
        match operator {
            UnaryOperator::Plus | UnaryOperator::Minus => match operand_type {
                DataType::Integer | DataType::Real | DataType::Double => Ok(operand_type.clone()),
                _ => Err(crate::common::Error::semantic_analysis(
                    "Arithmetic unary operators require numeric operands".to_string(),
                )),
            },
            UnaryOperator::Not => match operand_type {
                DataType::Boolean => Ok(DataType::Boolean),
                _ => Err(crate::common::Error::semantic_analysis(
                    "NOT operator requires boolean operand".to_string(),
                )),
            },
        }
    }

    // Методы проверки для различных типов запросов

    fn check_select_types(
        &mut self,
        select: &SelectStatement,
        result: &mut TypeCheckResult,
    ) -> Result<()> {
        result.checks_performed += 1;

        // Проверяем типы в SELECT списке
        for item in &select.select_list {
            self.check_select_item_types(item, result)?;
        }

        // Проверяем типы в WHERE условии
        if let Some(where_clause) = &select.where_clause {
            let where_type = self.check_expression_types(where_clause, result)?;
            if !matches!(where_type, DataType::Boolean) {
                result.add_error(TypeCheckError {
                    message: "WHERE clause must be a boolean expression".to_string(),
                    location: Some("WHERE clause".to_string()),
                    expected_type: Some(DataType::Boolean),
                    actual_type: Some(where_type),
                    suggested_fix: Some(
                        "Use comparison operators to create boolean conditions".to_string(),
                    ),
                });
            }
        }

        // Проверяем типы в GROUP BY
        for expr in &select.group_by {
            self.check_expression_types(expr, result)?;
        }

        // Проверяем типы в HAVING
        if let Some(having) = &select.having {
            let having_type = self.check_expression_types(having, result)?;
            if !matches!(having_type, DataType::Boolean) {
                result.add_error(TypeCheckError {
                    message: "HAVING clause must be a boolean expression".to_string(),
                    location: Some("HAVING clause".to_string()),
                    expected_type: Some(DataType::Boolean),
                    actual_type: Some(having_type),
                    suggested_fix: Some(
                        "Use aggregate functions with comparison operators".to_string(),
                    ),
                });
            }
        }

        // Проверяем типы в ORDER BY
        for order_item in &select.order_by {
            self.check_expression_types(&order_item.expr, result)?;
        }

        Ok(())
    }

    fn check_insert_types(
        &mut self,
        insert: &InsertStatement,
        result: &mut TypeCheckResult,
    ) -> Result<()> {
        result.checks_performed += 1;

        // Проверяем совместимость типов вставляемых значений с колонками таблицы
        match &insert.values {
            InsertValues::Values(rows) => {
                for row in rows {
                    for expr in row {
                        let expr_type = self.check_expression_types(expr, result)?;

                        // В реальной реализации здесь будет проверка против схемы таблицы
                        // Пока добавляем информацию о типе в результат
                        result.type_info.result_types.push(expr_type);
                    }
                }
            }
            InsertValues::Select(select) => {
                // Проверяем типы в подзапросе
                self.check_select_types(select, result)?;
            }
        }

        Ok(())
    }

    fn check_update_types(
        &mut self,
        update: &UpdateStatement,
        result: &mut TypeCheckResult,
    ) -> Result<()> {
        result.checks_performed += 1;

        // Проверяем типы в присваиваниях
        for assignment in &update.assignments {
            let value_type = self.check_expression_types(&assignment.value, result)?;

            // В реальной реализации здесь будет проверка совместимости с типом колонки
            result.type_info.result_types.push(value_type);
        }

        // Проверяем типы в WHERE условии
        if let Some(where_clause) = &update.where_clause {
            let where_type = self.check_expression_types(where_clause, result)?;
            if !matches!(where_type, DataType::Boolean) {
                result.add_error(TypeCheckError {
                    message: "WHERE clause in UPDATE must be a boolean expression".to_string(),
                    location: Some("WHERE clause".to_string()),
                    expected_type: Some(DataType::Boolean),
                    actual_type: Some(where_type),
                    suggested_fix: None,
                });
            }
        }

        Ok(())
    }

    fn check_delete_types(
        &mut self,
        delete: &DeleteStatement,
        result: &mut TypeCheckResult,
    ) -> Result<()> {
        result.checks_performed += 1;

        // Проверяем типы в WHERE условии
        if let Some(where_clause) = &delete.where_clause {
            let where_type = self.check_expression_types(where_clause, result)?;
            if !matches!(where_type, DataType::Boolean) {
                result.add_error(TypeCheckError {
                    message: "WHERE clause in DELETE must be a boolean expression".to_string(),
                    location: Some("WHERE clause".to_string()),
                    expected_type: Some(DataType::Boolean),
                    actual_type: Some(where_type),
                    suggested_fix: None,
                });
            }
        }

        Ok(())
    }

    fn check_create_table_types(
        &mut self,
        create: &CreateTableStatement,
        result: &mut TypeCheckResult,
    ) -> Result<()> {
        result.checks_performed += 1;

        // Проверяем корректность типов колонок
        for column in &create.columns {
            // Проверяем валидность типа данных
            self.validate_data_type(&column.data_type, result)?;
        }

        Ok(())
    }

    fn check_select_item_types(
        &mut self,
        item: &SelectItem,
        result: &mut TypeCheckResult,
    ) -> Result<()> {
        match item {
            SelectItem::Expression { expr, .. } => {
                let expr_type = self.check_expression_types(expr, result)?;
                result.type_info.result_types.push(expr_type);
            }
            SelectItem::Wildcard => {
                // Для * нужно получить все колонки из FROM таблиц
                // Пока добавляем заглушку
            }
        }
        Ok(())
    }

    fn check_expression_types(
        &mut self,
        expr: &Expression,
        result: &mut TypeCheckResult,
    ) -> Result<DataType> {
        result.checks_performed += 1;

        match expr {
            Expression::Literal(literal) => Ok(self.get_literal_type(literal)),
            Expression::Identifier(_) | Expression::QualifiedIdentifier { .. } => {
                // В реальной реализации здесь будет получение типа колонки из схемы
                Ok(DataType::Text) // Заглушка
            }
            Expression::BinaryOp { left, op, right } => {
                let left_type = self.check_expression_types(left, result)?;
                let right_type = self.check_expression_types(right, result)?;

                // Проверяем совместимость операндов
                let compatibility = self.check_compatibility(&left_type, &right_type);
                if matches!(compatibility, TypeCompatibility::Incompatible) {
                    result.add_error(TypeCheckError {
                        message: format!(
                            "Incompatible types in binary operation: {:?} and {:?}",
                            left_type, right_type
                        ),
                        location: Some("binary expression".to_string()),
                        expected_type: Some(left_type.clone()),
                        actual_type: Some(right_type.clone()),
                        suggested_fix: Some("Cast one operand to match the other type".to_string()),
                    });
                    return Ok(DataType::Text); // Возвращаем заглушку для продолжения анализа
                }

                // Добавляем предупреждение о неявном преобразовании
                if matches!(compatibility, TypeCompatibility::CompatibleWithConversion) {
                    result.add_warning(TypeCheckWarning {
                        message: format!(
                            "Implicit type conversion from {:?} to {:?}",
                            right_type, left_type
                        ),
                        location: Some("binary expression".to_string()),
                        warning_type: TypeWarningType::ImplicitConversion,
                    });
                }

                self.get_binary_operation_result_type(&left_type, &right_type, op)
            }
            Expression::UnaryOp { op, expr: operand } => {
                let operand_type = self.check_expression_types(operand, result)?;
                self.get_unary_operation_result_type(&operand_type, op)
            }
            Expression::Function { name, args } => {
                // Проверяем типы аргументов функции
                let mut arg_types = Vec::new();
                for arg in args {
                    let arg_type = self.check_expression_types(arg, result)?;
                    arg_types.push(arg_type);
                }

                // Определяем тип результата функции
                self.get_function_result_type(name, &arg_types, result)
            }
            _ => {
                // Упрощенная обработка других выражений
                Ok(DataType::Text)
            }
        }
    }

    // Вспомогательные методы

    pub fn get_literal_type(&self, literal: &Literal) -> DataType {
        match literal {
            Literal::Integer(_) => DataType::Integer,
            Literal::Float(_) => DataType::Real,
            Literal::String(_) => DataType::Text,
            Literal::Boolean(_) => DataType::Boolean,
            Literal::Null => DataType::Text, // NULL может быть любого типа
        }
    }

    pub fn get_arithmetic_result_type(
        &self,
        left_type: &DataType,
        right_type: &DataType,
    ) -> Result<DataType> {
        match (left_type, right_type) {
            (DataType::Integer, DataType::Integer) => Ok(DataType::Integer),
            (DataType::Real, _) | (_, DataType::Real) => Ok(DataType::Real),
            (DataType::Double, _) | (_, DataType::Double) => Ok(DataType::Double),
            _ => Err(crate::common::Error::semantic_analysis(
                "Arithmetic operations require numeric operands".to_string(),
            )),
        }
    }

    pub fn get_function_result_type(
        &self,
        function_name: &str,
        arg_types: &[DataType],
        result: &mut TypeCheckResult,
    ) -> Result<DataType> {
        match function_name.to_uppercase().as_str() {
            "COUNT" => Ok(DataType::Integer),
            "SUM" => {
                if !arg_types.is_empty()
                    && matches!(
                        arg_types[0],
                        DataType::Integer | DataType::Real | DataType::Double
                    )
                {
                    Ok(arg_types[0].clone())
                } else {
                    Ok(DataType::Real)
                }
            }
            "AVG" => Ok(DataType::Real),
            "MIN" | "MAX" => {
                if !arg_types.is_empty() {
                    Ok(arg_types[0].clone())
                } else {
                    Ok(DataType::Text)
                }
            }
            "UPPER" | "LOWER" | "TRIM" => Ok(DataType::Text),
            "LENGTH" => Ok(DataType::Integer),
            _ => {
                result.add_warning(TypeCheckWarning {
                    message: format!("Unknown function: {}", function_name),
                    location: Some("function call".to_string()),
                    warning_type: TypeWarningType::PerformanceImpact,
                });
                Ok(DataType::Text) // По умолчанию возвращаем текстовый тип
            }
        }
    }

    fn validate_data_type(&self, data_type: &DataType, result: &mut TypeCheckResult) -> Result<()> {
        match data_type {
            DataType::Varchar { length } => {
                if let Some(size) = length {
                    if *size == 0 {
                        result.add_error(TypeCheckError {
                            message: "VARCHAR size must be greater than 0".to_string(),
                            location: Some("column definition".to_string()),
                            expected_type: None,
                            actual_type: Some(data_type.clone()),
                            suggested_fix: Some("Specify a positive size for VARCHAR".to_string()),
                        });
                    }
                }
            }
            _ => {
                // Другие типы пока не требуют специальной валидации
            }
        }
        Ok(())
    }

    fn initialize_rules(&mut self) {
        // Инициализируем правила совместимости типов

        // Числовые типы
        self.compatibility_rules.insert(
            (DataType::Integer, DataType::Real),
            TypeCompatibility::CompatibleWithConversion,
        );
        self.compatibility_rules.insert(
            (DataType::Real, DataType::Integer),
            TypeCompatibility::CompatibleWithLoss,
        );

        // Строковые типы
        self.compatibility_rules.insert(
            (DataType::Text, DataType::Varchar { length: Some(255) }),
            TypeCompatibility::CompatibleWithConversion,
        );
        self.compatibility_rules.insert(
            (DataType::Varchar { length: Some(255) }, DataType::Text),
            TypeCompatibility::Compatible,
        );

        // Правила неявных преобразований
        self.conversion_rules
            .insert(DataType::Integer, vec![DataType::Real, DataType::Text]);
        self.conversion_rules
            .insert(DataType::Real, vec![DataType::Text]);
        self.conversion_rules
            .insert(DataType::Boolean, vec![DataType::Text]);
    }

    /// Включает или отключает строгий режим
    pub fn set_strict_mode(&mut self, strict: bool) {
        self.strict_mode = strict;
    }

    /// Проверяет, включен ли строгий режим
    pub fn is_strict_mode(&self) -> bool {
        self.strict_mode
    }
}

impl Default for TypeChecker {
    fn default() -> Self {
        Self::new()
    }
}

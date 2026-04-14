//! Module for checking data type compatibility

use crate::analyzer::semantic_analyzer::{TypeConversion, TypeInformation};
use crate::common::Result;
use crate::parser::ast::*;
use std::collections::HashMap;

/// Type check result
#[derive(Debug, Clone)]
pub struct TypeCheckResult {
    /// Check success
    pub is_valid: bool,
    /// Type errors
    pub errors: Vec<TypeCheckError>,
    /// Warnings
    pub warnings: Vec<TypeCheckWarning>,
    /// Type information
    pub type_info: TypeInformation,
    /// Number of checks performed
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

/// Type check error
#[derive(Debug, Clone)]
pub struct TypeCheckError {
    pub message: String,
    pub location: Option<String>,
    pub expected_type: Option<DataType>,
    pub actual_type: Option<DataType>,
    pub suggested_fix: Option<String>,
}

/// Type check warning
#[derive(Debug, Clone)]
pub struct TypeCheckWarning {
    pub message: String,
    pub location: Option<String>,
    pub warning_type: TypeWarningType,
}

/// Warning type
#[derive(Debug, Clone)]
pub enum TypeWarningType {
    ImplicitConversion,
    PrecisionLoss,
    PerformanceImpact,
    /// SQL NULL-related semantic caveat (e.g. `= NULL` yields UNKNOWN).
    NullSemantics,
}

/// Type compatibility
#[derive(Debug, Clone, PartialEq)]
pub enum TypeCompatibility {
    /// Types are fully compatible
    Compatible,
    /// Types are compatible with implicit conversion
    CompatibleWithConversion,
    /// Types are compatible with precision loss
    CompatibleWithLoss,
    /// Types are incompatible
    Incompatible,
}

/// Type checker
pub struct TypeChecker {
    /// Type compatibility rules
    compatibility_rules: HashMap<(DataType, DataType), TypeCompatibility>,
    /// Implicit conversion rules
    conversion_rules: HashMap<DataType, Vec<DataType>>,
    /// Whether strict type checking is enabled
    strict_mode: bool,
}

impl TypeChecker {
    /// Create new type checker
    pub fn new() -> Self {
        let mut checker = Self {
            compatibility_rules: HashMap::new(),
            conversion_rules: HashMap::new(),
            strict_mode: false,
        };

        checker.initialize_rules();
        checker
    }

    /// Create type checker in strict mode
    pub fn strict() -> Self {
        let mut checker = Self::new();
        checker.strict_mode = true;
        checker
    }

    /// Check types in SQL query
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
                // Other query types do not require type checking yet
            }
        }

        Ok(result)
    }

    /// Check compatibility of two types
    pub fn check_compatibility(
        &self,
        from_type: &DataType,
        to_type: &DataType,
    ) -> TypeCompatibility {
        // If types are the same, they are compatible
        if from_type == to_type {
            return TypeCompatibility::Compatible;
        }

        // Check compatibility rules
        if let Some(compatibility) = self
            .compatibility_rules
            .get(&(from_type.clone(), to_type.clone()))
        {
            return compatibility.clone();
        }

        // Check possibility of implicit conversion
        if let Some(conversions) = self.conversion_rules.get(from_type) {
            if conversions.contains(to_type) {
                return TypeCompatibility::CompatibleWithConversion;
            }
        }

        // By default types are incompatible
        TypeCompatibility::Incompatible
    }

    /// Check possibility of implicit conversion
    pub fn can_convert_implicitly(&self, from_type: &DataType, to_type: &DataType) -> bool {
        let compatibility = self.check_compatibility(from_type, to_type);
        matches!(
            compatibility,
            TypeCompatibility::Compatible | TypeCompatibility::CompatibleWithConversion
        )
    }

    /// Get result type for binary operation
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
                // Logical operators require boolean operands
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
                // For other operators return boolean type by default
                Ok(DataType::Boolean)
            }
        }
    }

    /// Get result type for unary operation
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

    // Type checking methods for different query types

    fn check_select_types(
        &mut self,
        select: &SelectStatement,
        result: &mut TypeCheckResult,
    ) -> Result<()> {
        result.checks_performed += 1;

        // Check types in SELECT list
        for item in &select.select_list {
            self.check_select_item_types(item, result)?;
        }

        // Check types in WHERE clause
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

        // Check types in GROUP BY
        for expr in &select.group_by {
            self.check_expression_types(expr, result)?;
        }

        // Check types in HAVING
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

        // Check types in ORDER BY
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

        // Check compatibility of inserted value types with table columns
        match &insert.values {
            InsertValues::Values(rows) => {
                for row in rows {
                    for expr in row {
                        let expr_type = self.check_expression_types(expr, result)?;

                        // In a real implementation, there would be a check against the table schema here
                        // For now, add type information to the result
                        result.type_info.result_types.push(expr_type);
                    }
                }
            }
            InsertValues::Select(select) => {
                // Check types in subquery
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

        // Check types in assignments
        for assignment in &update.assignments {
            let value_type = self.check_expression_types(&assignment.value, result)?;

            // In a real implementation, there would be a compatibility check with the column type here
            result.type_info.result_types.push(value_type);
        }

        // Check types in WHERE clause
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

        // Check types in WHERE clause
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

        // Check column type correctness
        for column in &create.columns {
            // Check data type validity
            self.validate_data_type(&column.data_type, result)?;

            // Check column constraints with expressions.
            for c in &column.constraints {
                match c {
                    ColumnConstraint::Default(expr) => {
                        let _ = self.check_expression_types(expr, result)?;
                    }
                    ColumnConstraint::Check(expr) => {
                        let t = self.check_expression_types(expr, result)?;
                        if !matches!(t, DataType::Boolean) {
                            result.add_error(TypeCheckError {
                                message: "CHECK constraint must be boolean".to_string(),
                                location: Some("CREATE TABLE column CHECK".to_string()),
                                expected_type: Some(DataType::Boolean),
                                actual_type: Some(t),
                                suggested_fix: Some("Use a boolean predicate in CHECK".to_string()),
                            });
                        }
                    }
                    _ => {}
                }
            }
        }

        // Check table constraints with expressions.
        for tc in &create.constraints {
            if let TableConstraint::Check(expr) = tc {
                let t = self.check_expression_types(expr, result)?;
                if !matches!(t, DataType::Boolean) {
                    result.add_error(TypeCheckError {
                        message: "Table CHECK constraint must be boolean".to_string(),
                        location: Some("CREATE TABLE CHECK".to_string()),
                        expected_type: Some(DataType::Boolean),
                        actual_type: Some(t),
                        suggested_fix: Some("Use a boolean predicate in CHECK".to_string()),
                    });
                }
            }
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
                // For * need to get all columns from FROM tables
                // For now, add a placeholder
            }
        }
        Ok(())
    }

    /// Column identifiers are typed as [`DataType::Text`] without a schema; for comparisons with literals, align to the literal type so `WHERE a = 1` is accepted.
    fn comparison_pair_types(
        &self,
        left: &Expression,
        left_type: &DataType,
        right: &Expression,
        right_type: &DataType,
        op: &BinaryOperator,
    ) -> (DataType, DataType) {
        if matches!(
            op,
            BinaryOperator::Add
                | BinaryOperator::Subtract
                | BinaryOperator::Multiply
                | BinaryOperator::Divide
                | BinaryOperator::Modulo
        ) {
            match (left, right) {
                (
                    Expression::Identifier(_) | Expression::QualifiedIdentifier { .. },
                    Expression::Literal(lit),
                ) => {
                    let t = self.get_literal_type(lit);
                    return (t.clone(), t);
                }
                (
                    Expression::Literal(lit),
                    Expression::Identifier(_) | Expression::QualifiedIdentifier { .. },
                ) => {
                    let t = self.get_literal_type(lit);
                    return (t.clone(), t);
                }
                _ => {}
            }
        }
        if !matches!(
            op,
            BinaryOperator::Equal
                | BinaryOperator::NotEqual
                | BinaryOperator::LessThan
                | BinaryOperator::LessThanOrEqual
                | BinaryOperator::GreaterThan
                | BinaryOperator::GreaterThanOrEqual
        ) {
            return (left_type.clone(), right_type.clone());
        }
        match (left, right) {
            (
                Expression::Identifier(_) | Expression::QualifiedIdentifier { .. },
                Expression::Literal(lit),
            ) => {
                let t = self.get_literal_type(lit);
                (t.clone(), t)
            }
            (
                Expression::Literal(lit),
                Expression::Identifier(_) | Expression::QualifiedIdentifier { .. },
            ) => {
                let t = self.get_literal_type(lit);
                (t.clone(), t)
            }
            _ => (left_type.clone(), right_type.clone()),
        }
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
                // In a real implementation, column type would be retrieved from schema here
                Ok(DataType::Text) // Placeholder
            }
            Expression::BinaryOp { left, op, right } => {
                let left_type = self.check_expression_types(left, result)?;
                let right_type = self.check_expression_types(right, result)?;

                let (eff_left, eff_right) =
                    self.comparison_pair_types(left, &left_type, right, &right_type, op);

                // Check operand compatibility
                let compatibility = self.check_compatibility(&eff_left, &eff_right);
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
                    return Ok(DataType::Text); // Return placeholder to continue analysis
                }

                // Record cast strategy and NULL semantics warnings.
                self.maybe_record_cast(
                    &eff_left,
                    &eff_right,
                    compatibility.clone(),
                    result,
                    "binary expression",
                );
                self.maybe_warn_null_comparisons(left, op, right, result);

                self.get_binary_operation_result_type(&eff_left, &eff_right, op)
            }
            Expression::UnaryOp { op, expr: operand } => {
                let operand_type = self.check_expression_types(operand, result)?;
                self.get_unary_operation_result_type(&operand_type, op)
            }
            Expression::Function { name, args } => {
                // Check function argument types
                let mut arg_types = Vec::new();
                for arg in args {
                    let arg_type = self.check_expression_types(arg, result)?;
                    arg_types.push(arg_type);
                }

                // Determine function result type
                self.get_function_result_type(name, &arg_types, result)
            }
            Expression::IsNull { expr, .. } => {
                let _ = self.check_expression_types(expr, result)?;
                Ok(DataType::Boolean)
            }
            Expression::Like { expr, pattern, .. } => {
                let _ = self.check_expression_types(expr, result)?;
                let pat_t = self.check_expression_types(pattern, result)?;
                if !matches!(pat_t, DataType::Text | DataType::Varchar { .. }) {
                    result.add_warning(TypeCheckWarning {
                        message: format!("LIKE pattern is typically text, got {:?}", pat_t),
                        location: Some("LIKE".to_string()),
                        warning_type: TypeWarningType::ImplicitConversion,
                    });
                }
                Ok(DataType::Boolean)
            }
            Expression::Between { expr, low, high } => {
                let t_expr = self.check_expression_types(expr, result)?;
                let t_low = self.check_expression_types(low, result)?;
                let t_high = self.check_expression_types(high, result)?;
                if matches!(
                    self.check_compatibility(&t_low, &t_expr),
                    TypeCompatibility::Incompatible
                ) || matches!(
                    self.check_compatibility(&t_high, &t_expr),
                    TypeCompatibility::Incompatible
                ) {
                    result.add_error(TypeCheckError {
                        message: "BETWEEN bounds must be comparable to the expression".to_string(),
                        location: Some("BETWEEN".to_string()),
                        expected_type: Some(t_expr),
                        actual_type: Some(t_low),
                        suggested_fix: Some("Cast bounds to a compatible type".to_string()),
                    });
                }
                Ok(DataType::Boolean)
            }
            Expression::In { expr, list } => {
                let t_expr = self.check_expression_types(expr, result)?;
                match list {
                    InList::Values(vals) => {
                        for v in vals {
                            let t_v = self.check_expression_types(v, result)?;
                            if matches!(
                                self.check_compatibility(&t_v, &t_expr),
                                TypeCompatibility::Incompatible
                            ) {
                                result.add_error(TypeCheckError {
                                    message: "IN list value type is not comparable to expression"
                                        .to_string(),
                                    location: Some("IN".to_string()),
                                    expected_type: Some(t_expr.clone()),
                                    actual_type: Some(t_v),
                                    suggested_fix: Some(
                                        "Cast values to a compatible type".to_string(),
                                    ),
                                });
                            }
                        }
                    }
                    InList::Subquery(sel) => {
                        self.check_select_types(sel, result)?;
                    }
                }
                Ok(DataType::Boolean)
            }
            Expression::Exists(sel) => {
                self.check_select_types(sel, result)?;
                Ok(DataType::Boolean)
            }
            Expression::Case {
                expr,
                when_clauses,
                else_clause,
            } => {
                let case_expr_type = if let Some(e) = expr {
                    Some(self.check_expression_types(e, result)?)
                } else {
                    None
                };

                let mut result_type: Option<DataType> = None;
                for wc in when_clauses {
                    if let Some(ct) = &case_expr_type {
                        let when_t = self.check_expression_types(&wc.condition, result)?;
                        if matches!(
                            self.check_compatibility(&when_t, ct),
                            TypeCompatibility::Incompatible
                        ) {
                            result.add_error(TypeCheckError {
                                message: "CASE operand and WHEN expression must be comparable"
                                    .to_string(),
                                location: Some("CASE WHEN".to_string()),
                                expected_type: Some(ct.clone()),
                                actual_type: Some(when_t),
                                suggested_fix: Some(
                                    "Cast WHEN expression to match CASE operand type".to_string(),
                                ),
                            });
                        }
                    } else {
                        let cond_t = self.check_expression_types(&wc.condition, result)?;
                        if !matches!(cond_t, DataType::Boolean) {
                            result.add_error(TypeCheckError {
                                message: "CASE WHEN condition must be boolean".to_string(),
                                location: Some("CASE WHEN".to_string()),
                                expected_type: Some(DataType::Boolean),
                                actual_type: Some(cond_t),
                                suggested_fix: Some("Use a boolean predicate in WHEN".to_string()),
                            });
                        }
                    }

                    let then_t = self.check_expression_types(&wc.result, result)?;
                    result_type = Some(match &result_type {
                        None => then_t,
                        Some(acc) => {
                            let compat = self.check_compatibility(&then_t, acc);
                            if matches!(compat, TypeCompatibility::Incompatible) {
                                result.add_error(TypeCheckError {
                                    message: "CASE result expressions must have compatible types"
                                        .to_string(),
                                    location: Some("CASE THEN".to_string()),
                                    expected_type: Some(acc.clone()),
                                    actual_type: Some(then_t.clone()),
                                    suggested_fix: Some(
                                        "Cast CASE branches to a common type".to_string(),
                                    ),
                                });
                            }
                            self.maybe_record_cast(&then_t, acc, compat, result, "CASE THEN");
                            acc.clone()
                        }
                    });
                }

                if let Some(e) = else_clause {
                    let else_t = self.check_expression_types(e, result)?;
                    result_type = Some(match &result_type {
                        None => else_t,
                        Some(acc) => {
                            let compat = self.check_compatibility(&else_t, acc);
                            if matches!(compat, TypeCompatibility::Incompatible) {
                                result.add_error(TypeCheckError {
                                    message: "CASE ELSE expression must be compatible with THEN expressions"
                                        .to_string(),
                                    location: Some("CASE ELSE".to_string()),
                                    expected_type: Some(acc.clone()),
                                    actual_type: Some(else_t.clone()),
                                    suggested_fix: Some("Cast ELSE to match THEN type".to_string()),
                                });
                            }
                            self.maybe_record_cast(&else_t, acc, compat, result, "CASE ELSE");
                            acc.clone()
                        }
                    });
                }

                Ok(result_type.unwrap_or(DataType::Text))
            }
        }
    }

    /// Minimal cast strategy: when types are compatible via conversion/loss, record it in
    /// `TypeInformation.type_conversions` and emit a warning (unless strict mode escalates later).
    fn maybe_record_cast(
        &self,
        from_type: &DataType,
        to_type: &DataType,
        compatibility: TypeCompatibility,
        result: &mut TypeCheckResult,
        location: &str,
    ) {
        match compatibility {
            TypeCompatibility::Compatible => {}
            TypeCompatibility::CompatibleWithConversion => {
                result.type_info.type_conversions.push(TypeConversion {
                    from_type: from_type.clone(),
                    to_type: to_type.clone(),
                    is_implicit: true,
                    location: location.to_string(),
                });
                result.add_warning(TypeCheckWarning {
                    message: format!("Implicit cast required: {:?} -> {:?}", from_type, to_type),
                    location: Some(location.to_string()),
                    warning_type: TypeWarningType::ImplicitConversion,
                });
            }
            TypeCompatibility::CompatibleWithLoss => {
                result.type_info.type_conversions.push(TypeConversion {
                    from_type: from_type.clone(),
                    to_type: to_type.clone(),
                    is_implicit: true,
                    location: location.to_string(),
                });
                result.add_warning(TypeCheckWarning {
                    message: format!(
                        "Implicit cast with potential precision loss: {:?} -> {:?}",
                        from_type, to_type
                    ),
                    location: Some(location.to_string()),
                    warning_type: TypeWarningType::PrecisionLoss,
                });
            }
            TypeCompatibility::Incompatible => {}
        }
    }

    /// SQL NULL behavior (3-valued logic) reminders.
    ///
    /// We don't evaluate expressions here, but we can flag a common footgun:
    /// `col = NULL` and `col <> NULL` are UNKNOWN in SQL; use `IS NULL`.
    fn maybe_warn_null_comparisons(
        &self,
        left: &Expression,
        op: &BinaryOperator,
        right: &Expression,
        result: &mut TypeCheckResult,
    ) {
        let is_cmp = matches!(
            op,
            BinaryOperator::Equal
                | BinaryOperator::NotEqual
                | BinaryOperator::LessThan
                | BinaryOperator::LessThanOrEqual
                | BinaryOperator::GreaterThan
                | BinaryOperator::GreaterThanOrEqual
        );
        if !is_cmp {
            return;
        }

        fn is_null_literal(e: &Expression) -> bool {
            matches!(e, Expression::Literal(Literal::Null))
        }

        if is_null_literal(left) || is_null_literal(right) {
            result.add_warning(TypeCheckWarning {
                message: "Comparison with NULL yields UNKNOWN in SQL (use IS NULL / IS NOT NULL)"
                    .to_string(),
                location: Some("NULL comparison".to_string()),
                warning_type: TypeWarningType::NullSemantics,
            });
        }
    }

    // Helper methods

    pub fn get_literal_type(&self, literal: &Literal) -> DataType {
        match literal {
            Literal::Integer(_) => DataType::Integer,
            Literal::Float(_) => DataType::Real,
            Literal::String(_) => DataType::Text,
            Literal::Boolean(_) => DataType::Boolean,
            Literal::Null => DataType::Text, // NULL can be any type
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
                Ok(DataType::Text) // Return text type by default
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
                // Other types do not require special validation yet
            }
        }
        Ok(())
    }

    fn initialize_rules(&mut self) {
        // Initialize type compatibility rules

        // Numeric types
        self.compatibility_rules.insert(
            (DataType::Integer, DataType::Real),
            TypeCompatibility::CompatibleWithConversion,
        );
        self.compatibility_rules.insert(
            (DataType::Real, DataType::Integer),
            TypeCompatibility::CompatibleWithLoss,
        );

        // String types
        self.compatibility_rules.insert(
            (DataType::Text, DataType::Varchar { length: Some(255) }),
            TypeCompatibility::CompatibleWithConversion,
        );
        self.compatibility_rules.insert(
            (DataType::Varchar { length: Some(255) }, DataType::Text),
            TypeCompatibility::Compatible,
        );

        // Implicit conversion rules
        self.conversion_rules
            .insert(DataType::Integer, vec![DataType::Real, DataType::Text]);
        self.conversion_rules
            .insert(DataType::Real, vec![DataType::Text]);
        self.conversion_rules
            .insert(DataType::Boolean, vec![DataType::Text]);
    }

    /// Enable or disable strict mode
    pub fn set_strict_mode(&mut self, strict: bool) {
        self.strict_mode = strict;
    }

    /// Check if strict mode is enabled
    pub fn is_strict_mode(&self) -> bool {
        self.strict_mode
    }
}

impl Default for TypeChecker {
    fn default() -> Self {
        Self::new()
    }
}

//! Тесты для проверщика типов

use crate::analyzer::{TypeChecker, TypeCompatibility};
use crate::parser::ast::*;
use crate::common::Result;

#[test]
fn test_type_checker_creation() {
    let checker = TypeChecker::new();
    assert!(!checker.is_strict_mode());
    
    let strict_checker = TypeChecker::strict();
    assert!(strict_checker.is_strict_mode());
}

#[test]
fn test_type_compatibility() {
    let checker = TypeChecker::new();
    
    // Одинаковые типы совместимы
    assert_eq!(
        checker.check_compatibility(&DataType::Integer, &DataType::Integer),
        TypeCompatibility::Compatible
    );
    
    // Integer -> Real с преобразованием
    assert_eq!(
        checker.check_compatibility(&DataType::Integer, &DataType::Real),
        TypeCompatibility::CompatibleWithConversion
    );
    
    // Real -> Integer с потерей точности
    assert_eq!(
        checker.check_compatibility(&DataType::Real, &DataType::Integer),
        TypeCompatibility::CompatibleWithLoss
    );
    
    // Несовместимые типы
    assert_eq!(
        checker.check_compatibility(&DataType::Boolean, &DataType::Integer),
        TypeCompatibility::Incompatible
    );
}

#[test]
fn test_implicit_conversion() {
    let checker = TypeChecker::new();
    
    // Integer может быть неявно преобразован в Real
    assert!(checker.can_convert_implicitly(&DataType::Integer, &DataType::Real));
    
    // Boolean не может быть неявно преобразован в Integer
    assert!(!checker.can_convert_implicitly(&DataType::Boolean, &DataType::Integer));
}

#[test]
fn test_literal_types() {
    let checker = TypeChecker::new();
    
    assert_eq!(checker.get_literal_type(&Literal::Integer(42)), DataType::Integer);
    assert_eq!(checker.get_literal_type(&Literal::Float(3.14)), DataType::Real);
    assert_eq!(checker.get_literal_type(&Literal::String("hello".to_string())), DataType::Text);
    assert_eq!(checker.get_literal_type(&Literal::Boolean(true)), DataType::Boolean);
    assert_eq!(checker.get_literal_type(&Literal::Null), DataType::Text);
}

#[test]
fn test_arithmetic_result_types() -> Result<()> {
    let checker = TypeChecker::new();
    
    // Integer + Integer = Integer
    let result = checker.get_arithmetic_result_type(&DataType::Integer, &DataType::Integer)?;
    assert_eq!(result, DataType::Integer);
    
    // Integer + Real = Real
    let result = checker.get_arithmetic_result_type(&DataType::Integer, &DataType::Real)?;
    assert_eq!(result, DataType::Real);
    
    // Real + Double = Double
    let result = checker.get_arithmetic_result_type(&DataType::Real, &DataType::Double)?;
    assert_eq!(result, DataType::Real);
    
    Ok(())
}

#[test]
fn test_binary_operation_result_types() -> Result<()> {
    let checker = TypeChecker::new();
    
    // Арифметические операции
    let result = checker.get_binary_operation_result_type(
        &DataType::Integer, 
        &DataType::Integer, 
        &BinaryOperator::Add
    )?;
    assert_eq!(result, DataType::Integer);
    
    // Операции сравнения
    let result = checker.get_binary_operation_result_type(
        &DataType::Integer, 
        &DataType::Integer, 
        &BinaryOperator::Equal
    )?;
    assert_eq!(result, DataType::Boolean);
    
    // Логические операции
    let result = checker.get_binary_operation_result_type(
        &DataType::Boolean, 
        &DataType::Boolean, 
        &BinaryOperator::And
    )?;
    assert_eq!(result, DataType::Boolean);
    
    Ok(())
}

#[test]
fn test_unary_operation_result_types() -> Result<()> {
    let checker = TypeChecker::new();
    
    // Арифметические унарные операции
    let result = checker.get_unary_operation_result_type(&DataType::Integer, &UnaryOperator::Minus)?;
    assert_eq!(result, DataType::Integer);
    
    let result = checker.get_unary_operation_result_type(&DataType::Real, &UnaryOperator::Plus)?;
    assert_eq!(result, DataType::Real);
    
    // Логические унарные операции
    let result = checker.get_unary_operation_result_type(&DataType::Boolean, &UnaryOperator::Not)?;
    assert_eq!(result, DataType::Boolean);
    
    Ok(())
}

#[test]
fn test_function_result_types() -> Result<()> {
    let mut checker = TypeChecker::new();
    let mut result = crate::analyzer::TypeCheckResult::new();
    
    // COUNT всегда возвращает Integer
    let count_type = checker.get_function_result_type("COUNT", &[DataType::Text], &mut result)?;
    assert_eq!(count_type, DataType::Integer);
    
    // SUM возвращает тип аргумента для числовых типов
    let sum_type = checker.get_function_result_type("SUM", &[DataType::Integer], &mut result)?;
    assert_eq!(sum_type, DataType::Integer);
    
    // AVG всегда возвращает Real
    let avg_type = checker.get_function_result_type("AVG", &[DataType::Integer], &mut result)?;
    assert_eq!(avg_type, DataType::Real);
    
    // Строковые функции возвращают Text
    let upper_type = checker.get_function_result_type("UPPER", &[DataType::Text], &mut result)?;
    assert_eq!(upper_type, DataType::Text);
    
    Ok(())
}

#[test]
fn test_invalid_operations() {
    let checker = TypeChecker::new();
    
    // Логические операторы с неправильными типами
    let result = checker.get_binary_operation_result_type(
        &DataType::Integer, 
        &DataType::Text, 
        &BinaryOperator::And
    );
    assert!(result.is_err());
    
    // Арифметические унарные операторы с неправильными типами
    let result = checker.get_unary_operation_result_type(&DataType::Text, &UnaryOperator::Minus);
    assert!(result.is_err());
    
    // NOT с неправильным типом
    let result = checker.get_unary_operation_result_type(&DataType::Integer, &UnaryOperator::Not);
    assert!(result.is_err());
}

#[test]
fn test_strict_mode() {
    let mut checker = TypeChecker::new();
    
    assert!(!checker.is_strict_mode());
    
    checker.set_strict_mode(true);
    assert!(checker.is_strict_mode());
    
    checker.set_strict_mode(false);
    assert!(!checker.is_strict_mode());
}


//! Тесты для операторов сканирования

use crate::executor::operators::{
    Operator, TableScanOperator, IndexScanOperator, RangeScanOperator, 
    ConditionalScanOperator, ScanOperatorFactory, IndexCondition, IndexOperator
};
use crate::storage::{Row, PageId};
use crate::storage::index::BPlusTree;
use crate::storage::page_manager::PageManager;
use crate::common::Result;
use std::sync::{Arc, Mutex};

#[test]
fn test_table_scan_operator_creation() -> Result<()> {
    let page_manager = Arc::new(Mutex::new(PageManager::new()?));
    let schema = vec!["id".to_string(), "name".to_string(), "age".to_string()];
    
    let operator = TableScanOperator::new(
        "users".to_string(),
        page_manager,
        None,
        schema.clone(),
    )?;
    
    let operator_schema = operator.get_schema()?;
    assert_eq!(operator_schema, schema);
    
    Ok(())
}

#[test]
fn test_table_scan_with_filter() -> Result<()> {
    let page_manager = Arc::new(Mutex::new(PageManager::new()?));
    let schema = vec!["id".to_string(), "name".to_string(), "age".to_string()];
    
    let operator = TableScanOperator::new(
        "users".to_string(),
        page_manager,
        Some("age > 18".to_string()),
        schema,
    )?;
    
    // Проверяем, что оператор создался успешно
    let statistics = operator.get_statistics();
    assert_eq!(statistics.rows_processed, 0);
    assert_eq!(statistics.rows_returned, 0);
    
    Ok(())
}

#[test]
fn test_index_scan_operator_creation() -> Result<()> {
    let page_manager = Arc::new(Mutex::new(PageManager::new()?));
    let index = Arc::new(Mutex::new(BPlusTree::new()));
    let schema = vec!["id".to_string(), "name".to_string(), "age".to_string()];
    
    let search_conditions = vec![
        IndexCondition {
            column: "id".to_string(),
            operator: IndexOperator::Equal,
            value: "1".to_string(),
        }
    ];
    
    let operator = IndexScanOperator::new(
        "users".to_string(),
        "idx_users_id".to_string(),
        index,
        page_manager,
        search_conditions,
        schema.clone(),
    )?;
    
    let operator_schema = operator.get_schema()?;
    assert_eq!(operator_schema, schema);
    
    Ok(())
}

#[test]
fn test_range_scan_operator() -> Result<()> {
    let page_manager = Arc::new(Mutex::new(PageManager::new()?));
    let schema = vec!["id".to_string(), "name".to_string(), "age".to_string()];
    
    let base_operator = TableScanOperator::new(
        "users".to_string(),
        page_manager,
        None,
        schema,
    )?;
    
    let range_operator = RangeScanOperator::new(
        Box::new(base_operator),
        Some("1".to_string()),
        Some("10".to_string()),
    )?;
    
    let statistics = range_operator.get_statistics();
    assert_eq!(statistics.rows_processed, 0);
    assert_eq!(statistics.rows_returned, 0);
    
    Ok(())
}

#[test]
fn test_conditional_scan_operator() -> Result<()> {
    let page_manager = Arc::new(Mutex::new(PageManager::new()?));
    let schema = vec!["id".to_string(), "name".to_string(), "age".to_string()];
    
    let base_operator = TableScanOperator::new(
        "users".to_string(),
        page_manager,
        None,
        schema,
    )?;
    
    let conditional_operator = ConditionalScanOperator::new(
        Box::new(base_operator),
        "name LIKE 'John%'".to_string(),
    )?;
    
    let statistics = conditional_operator.get_statistics();
    assert_eq!(statistics.rows_processed, 0);
    assert_eq!(statistics.rows_returned, 0);
    
    Ok(())
}

#[test]
fn test_scan_operator_factory() -> Result<()> {
    let page_manager = Arc::new(Mutex::new(PageManager::new()?));
    let mut factory = ScanOperatorFactory::new(page_manager);
    
    let schema = vec!["id".to_string(), "name".to_string(), "age".to_string()];
    
    // Тестируем создание TableScan
    let table_scan = factory.create_table_scan(
        "users".to_string(),
        None,
        schema.clone(),
    )?;
    
    let table_scan_schema = table_scan.get_schema()?;
    assert_eq!(table_scan_schema, schema);
    
    // Тестируем создание RangeScan
    let range_scan = factory.create_range_scan(
        table_scan,
        Some("1".to_string()),
        Some("10".to_string()),
    )?;
    
    let range_scan_schema = range_scan.get_schema()?;
    assert_eq!(range_scan_schema, schema);
    
    // Тестируем создание ConditionalScan
    let base_operator = factory.create_table_scan(
        "users".to_string(),
        None,
        schema,
    )?;
    
    let conditional_scan = factory.create_conditional_scan(
        base_operator,
        "age > 18".to_string(),
    )?;
    
    let conditional_schema = conditional_scan.get_schema()?;
    assert_eq!(conditional_schema.len(), 3);
    
    Ok(())
}

#[test]
fn test_operator_reset() -> Result<()> {
    let page_manager = Arc::new(Mutex::new(PageManager::new()?));
    let schema = vec!["id".to_string(), "name".to_string(), "age".to_string()];
    
    let mut operator = TableScanOperator::new(
        "users".to_string(),
        page_manager,
        None,
        schema,
    )?;
    
    // Сбрасываем оператор
    operator.reset()?;
    
    let statistics = operator.get_statistics();
    assert_eq!(statistics.rows_processed, 0);
    assert_eq!(statistics.rows_returned, 0);
    assert_eq!(statistics.execution_time_ms, 0);
    
    Ok(())
}

#[test]
fn test_operator_statistics() -> Result<()> {
    let page_manager = Arc::new(Mutex::new(PageManager::new()?));
    let schema = vec!["id".to_string(), "name".to_string(), "age".to_string()];
    
    let operator = TableScanOperator::new(
        "users".to_string(),
        page_manager,
        None,
        schema,
    )?;
    
    let statistics = operator.get_statistics();
    
    // Проверяем, что все поля статистики инициализированы
    assert_eq!(statistics.rows_processed, 0);
    assert_eq!(statistics.rows_returned, 0);
    assert_eq!(statistics.execution_time_ms, 0);
    assert_eq!(statistics.io_operations, 0);
    assert_eq!(statistics.memory_operations, 0);
    assert_eq!(statistics.memory_used_bytes, 0);
    
    Ok(())
}

#[test]
fn test_index_conditions() {
    let condition = IndexCondition {
        column: "id".to_string(),
        operator: IndexOperator::Equal,
        value: "1".to_string(),
    };
    
    assert_eq!(condition.column, "id");
    assert!(matches!(condition.operator, IndexOperator::Equal));
    assert_eq!(condition.value, "1");
}

#[test]
fn test_index_operators() {
    let operators = vec![
        IndexOperator::Equal,
        IndexOperator::LessThan,
        IndexOperator::LessThanOrEqual,
        IndexOperator::GreaterThan,
        IndexOperator::GreaterThanOrEqual,
        IndexOperator::Between,
        IndexOperator::In,
    ];
    
    assert_eq!(operators.len(), 7);
}

#[test]
fn test_operator_trait_implementation() -> Result<()> {
    let page_manager = Arc::new(Mutex::new(PageManager::new()?));
    let schema = vec!["id".to_string(), "name".to_string(), "age".to_string()];
    
    let mut operator: Box<dyn Operator> = Box::new(TableScanOperator::new(
        "users".to_string(),
        page_manager,
        None,
        schema,
    )?);
    
    // Тестируем методы трейта
    let operator_schema = operator.get_schema()?;
    assert_eq!(operator_schema.len(), 3);
    
    let statistics = operator.get_statistics();
    assert_eq!(statistics.rows_processed, 0);
    
    operator.reset()?;
    
    Ok(())
}



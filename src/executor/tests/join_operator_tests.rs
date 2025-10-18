//! Тесты для операторов соединения

use crate::executor::operators::{
    Operator, NestedLoopJoinOperator, HashJoinOperator, MergeJoinOperator,
    JoinType, JoinCondition, JoinOperator, TableScanOperator, IndexCondition, IndexOperator
};
use crate::storage::{Row, PageId};
use crate::storage::index::BPlusTree;
use crate::storage::page_manager::PageManager;
use crate::common::Result;
use std::sync::{Arc, Mutex};

#[test]
fn test_nested_loop_join_creation() -> Result<()> {
    let page_manager = Arc::new(Mutex::new(PageManager::new()?));
    let left_schema = vec!["id".to_string(), "name".to_string()];
    let right_schema = vec!["user_id".to_string(), "email".to_string()];
    
    let left_operator = TableScanOperator::new(
        "users".to_string(),
        page_manager.clone(),
        None,
        left_schema,
    )?;
    
    let right_operator = TableScanOperator::new(
        "emails".to_string(),
        page_manager,
        None,
        right_schema,
    )?;
    
    let join_condition = JoinCondition {
        left_column: "id".to_string(),
        right_column: "user_id".to_string(),
        operator: JoinOperator::Equal,
    };
    
    let join_operator = NestedLoopJoinOperator::new(
        Box::new(left_operator),
        Box::new(right_operator),
        join_condition,
        JoinType::Inner,
        100, // block_size
    )?;
    
    let schema = join_operator.get_schema()?;
    assert_eq!(schema.len(), 4); // id, name, user_id, email
    
    Ok(())
}

#[test]
fn test_hash_join_creation() -> Result<()> {
    let page_manager = Arc::new(Mutex::new(PageManager::new()?));
    let left_schema = vec!["id".to_string(), "name".to_string()];
    let right_schema = vec!["user_id".to_string(), "email".to_string()];
    
    let left_operator = TableScanOperator::new(
        "users".to_string(),
        page_manager.clone(),
        None,
        left_schema,
    )?;
    
    let right_operator = TableScanOperator::new(
        "emails".to_string(),
        page_manager,
        None,
        right_schema,
    )?;
    
    let join_condition = JoinCondition {
        left_column: "id".to_string(),
        right_column: "user_id".to_string(),
        operator: JoinOperator::Equal,
    };
    
    let join_operator = HashJoinOperator::new(
        Box::new(left_operator),
        Box::new(right_operator),
        join_condition,
        JoinType::Inner,
        1000, // hash_table_size
    )?;
    
    let schema = join_operator.get_schema()?;
    assert_eq!(schema.len(), 4); // id, name, user_id, email
    
    Ok(())
}

#[test]
fn test_merge_join_creation() -> Result<()> {
    let page_manager = Arc::new(Mutex::new(PageManager::new()?));
    let left_schema = vec!["id".to_string(), "name".to_string()];
    let right_schema = vec!["user_id".to_string(), "email".to_string()];
    
    let left_operator = TableScanOperator::new(
        "users".to_string(),
        page_manager.clone(),
        None,
        left_schema,
    )?;
    
    let right_operator = TableScanOperator::new(
        "emails".to_string(),
        page_manager,
        None,
        right_schema,
    )?;
    
    let join_condition = JoinCondition {
        left_column: "id".to_string(),
        right_column: "user_id".to_string(),
        operator: JoinOperator::Equal,
    };
    
    let join_operator = MergeJoinOperator::new(
        Box::new(left_operator),
        Box::new(right_operator),
        join_condition,
        JoinType::Inner,
    )?;
    
    let schema = join_operator.get_schema()?;
    assert_eq!(schema.len(), 4); // id, name, user_id, email
    
    Ok(())
}

#[test]
fn test_join_conditions() {
    let conditions = vec![
        JoinCondition {
            left_column: "id".to_string(),
            right_column: "user_id".to_string(),
            operator: JoinOperator::Equal,
        },
        JoinCondition {
            left_column: "age".to_string(),
            right_column: "user_age".to_string(),
            operator: JoinOperator::GreaterThan,
        },
        JoinCondition {
            left_column: "score".to_string(),
            right_column: "user_score".to_string(),
            operator: JoinOperator::LessThanOrEqual,
        },
    ];
    
    assert_eq!(conditions.len(), 3);
    assert_eq!(conditions[0].left_column, "id");
    assert_eq!(conditions[0].right_column, "user_id");
    assert!(matches!(conditions[0].operator, JoinOperator::Equal));
}

#[test]
fn test_join_types() {
    let join_types = vec![
        JoinType::Inner,
        JoinType::LeftOuter,
        JoinType::RightOuter,
        JoinType::FullOuter,
    ];
    
    assert_eq!(join_types.len(), 4);
}

#[test]
fn test_join_operators() {
    let operators = vec![
        JoinOperator::Equal,
        JoinOperator::NotEqual,
        JoinOperator::LessThan,
        JoinOperator::LessThanOrEqual,
        JoinOperator::GreaterThan,
        JoinOperator::GreaterThanOrEqual,
    ];
    
    assert_eq!(operators.len(), 6);
}

#[test]
fn test_nested_loop_join_reset() -> Result<()> {
    let page_manager = Arc::new(Mutex::new(PageManager::new()?));
    let left_schema = vec!["id".to_string(), "name".to_string()];
    let right_schema = vec!["user_id".to_string(), "email".to_string()];
    
    let left_operator = TableScanOperator::new(
        "users".to_string(),
        page_manager.clone(),
        None,
        left_schema,
    )?;
    
    let right_operator = TableScanOperator::new(
        "emails".to_string(),
        page_manager,
        None,
        right_schema,
    )?;
    
    let join_condition = JoinCondition {
        left_column: "id".to_string(),
        right_column: "user_id".to_string(),
        operator: JoinOperator::Equal,
    };
    
    let mut join_operator = NestedLoopJoinOperator::new(
        Box::new(left_operator),
        Box::new(right_operator),
        join_condition,
        JoinType::Inner,
        100,
    )?;
    
    // Сбрасываем оператор
    join_operator.reset()?;
    
    let statistics = join_operator.get_statistics();
    assert_eq!(statistics.rows_processed, 0);
    assert_eq!(statistics.rows_returned, 0);
    assert_eq!(statistics.execution_time_ms, 0);
    
    Ok(())
}

#[test]
fn test_hash_join_reset() -> Result<()> {
    let page_manager = Arc::new(Mutex::new(PageManager::new()?));
    let left_schema = vec!["id".to_string(), "name".to_string()];
    let right_schema = vec!["user_id".to_string(), "email".to_string()];
    
    let left_operator = TableScanOperator::new(
        "users".to_string(),
        page_manager.clone(),
        None,
        left_schema,
    )?;
    
    let right_operator = TableScanOperator::new(
        "emails".to_string(),
        page_manager,
        None,
        right_schema,
    )?;
    
    let join_condition = JoinCondition {
        left_column: "id".to_string(),
        right_column: "user_id".to_string(),
        operator: JoinOperator::Equal,
    };
    
    let mut join_operator = HashJoinOperator::new(
        Box::new(left_operator),
        Box::new(right_operator),
        join_condition,
        JoinType::Inner,
        1000,
    )?;
    
    // Сбрасываем оператор
    join_operator.reset()?;
    
    let statistics = join_operator.get_statistics();
    assert_eq!(statistics.rows_processed, 0);
    assert_eq!(statistics.rows_returned, 0);
    assert_eq!(statistics.execution_time_ms, 0);
    
    Ok(())
}

#[test]
fn test_merge_join_reset() -> Result<()> {
    let page_manager = Arc::new(Mutex::new(PageManager::new()?));
    let left_schema = vec!["id".to_string(), "name".to_string()];
    let right_schema = vec!["user_id".to_string(), "email".to_string()];
    
    let left_operator = TableScanOperator::new(
        "users".to_string(),
        page_manager.clone(),
        None,
        left_schema,
    )?;
    
    let right_operator = TableScanOperator::new(
        "emails".to_string(),
        page_manager,
        None,
        right_schema,
    )?;
    
    let join_condition = JoinCondition {
        left_column: "id".to_string(),
        right_column: "user_id".to_string(),
        operator: JoinOperator::Equal,
    };
    
    let mut join_operator = MergeJoinOperator::new(
        Box::new(left_operator),
        Box::new(right_operator),
        join_condition,
        JoinType::Inner,
    )?;
    
    // Сбрасываем оператор
    join_operator.reset()?;
    
    let statistics = join_operator.get_statistics();
    assert_eq!(statistics.rows_processed, 0);
    assert_eq!(statistics.rows_returned, 0);
    assert_eq!(statistics.execution_time_ms, 0);
    
    Ok(())
}

#[test]
fn test_join_operator_statistics() -> Result<()> {
    let page_manager = Arc::new(Mutex::new(PageManager::new()?));
    let left_schema = vec!["id".to_string(), "name".to_string()];
    let right_schema = vec!["user_id".to_string(), "email".to_string()];
    
    let left_operator = TableScanOperator::new(
        "users".to_string(),
        page_manager.clone(),
        None,
        left_schema,
    )?;
    
    let right_operator = TableScanOperator::new(
        "emails".to_string(),
        page_manager,
        None,
        right_schema,
    )?;
    
    let join_condition = JoinCondition {
        left_column: "id".to_string(),
        right_column: "user_id".to_string(),
        operator: JoinOperator::Equal,
    };
    
    let join_operator = NestedLoopJoinOperator::new(
        Box::new(left_operator),
        Box::new(right_operator),
        join_condition,
        JoinType::Inner,
        100,
    )?;
    
    let statistics = join_operator.get_statistics();
    
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
fn test_join_operator_trait_implementation() -> Result<()> {
    let page_manager = Arc::new(Mutex::new(PageManager::new()?));
    let left_schema = vec!["id".to_string(), "name".to_string()];
    let right_schema = vec!["user_id".to_string(), "email".to_string()];
    
    let left_operator = TableScanOperator::new(
        "users".to_string(),
        page_manager.clone(),
        None,
        left_schema,
    )?;
    
    let right_operator = TableScanOperator::new(
        "emails".to_string(),
        page_manager,
        None,
        right_schema,
    )?;
    
    let join_condition = JoinCondition {
        left_column: "id".to_string(),
        right_column: "user_id".to_string(),
        operator: JoinOperator::Equal,
    };
    
    let mut operator: Box<dyn Operator> = Box::new(NestedLoopJoinOperator::new(
        Box::new(left_operator),
        Box::new(right_operator),
        join_condition,
        JoinType::Inner,
        100,
    )?);
    
    // Тестируем методы трейта
    let operator_schema = operator.get_schema()?;
    assert_eq!(operator_schema.len(), 4);
    
    let statistics = operator.get_statistics();
    assert_eq!(statistics.rows_processed, 0);
    
    operator.reset()?;
    
    Ok(())
}


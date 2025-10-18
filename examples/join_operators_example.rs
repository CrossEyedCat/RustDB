//! Пример использования операторов соединения

use rustdb::executor::{
    Operator, NestedLoopJoinOperator, HashJoinOperator, MergeJoinOperator,
    TableScanOperator, JoinType, JoinCondition, JoinOperator
};
use rustdb::storage::page_manager::{PageManager, PageManagerConfig};
use rustdb::common::Result;
use std::sync::{Arc, Mutex};
use std::path::PathBuf;

fn main() -> Result<()> {
    println!("=== Пример использования операторов соединения ===\n");

    // Создаем менеджеры страниц для разных таблиц
    let users_page_manager = Arc::new(Mutex::new(PageManager::new(
        PathBuf::from("./data"),
        "users",
        PageManagerConfig::default()
    )?));
    
    let emails_page_manager = Arc::new(Mutex::new(PageManager::new(
        PathBuf::from("./data"),
        "emails",
        PageManagerConfig::default()
    )?));

    let users_schema = vec!["id".to_string(), "name".to_string(), "age".to_string()];
    let emails_schema = vec!["user_id".to_string(), "email".to_string(), "type".to_string()];

    // Пример 1: Nested Loop Join
    println!("1. Nested Loop Join:");
    let users_operator = TableScanOperator::new(
        "users".to_string(),
        users_page_manager.clone(),
        None,
        users_schema.clone(),
    )?;
    
    let emails_operator = TableScanOperator::new(
        "emails".to_string(),
        emails_page_manager.clone(),
        None,
        emails_schema.clone(),
    )?;
    
    let join_condition = JoinCondition {
        left_column: "id".to_string(),
        right_column: "user_id".to_string(),
        operator: JoinOperator::Equal,
    };
    
    let nested_loop_join = NestedLoopJoinOperator::new(
        Box::new(users_operator),
        Box::new(emails_operator),
        join_condition.clone(),
        JoinType::Inner,
        100, // block_size
    )?;
    
    println!("   Создан Nested Loop Join оператор");
    println!("   Схема: {:?}", nested_loop_join.get_schema()?);
    println!("   Статистика: {:?}", nested_loop_join.get_statistics());
    println!();

    // Пример 2: Hash Join
    println!("2. Hash Join:");
    let users_operator_hash = TableScanOperator::new(
        "users".to_string(),
        users_page_manager.clone(),
        None,
        users_schema.clone(),
    )?;
    
    let emails_operator_hash = TableScanOperator::new(
        "emails".to_string(),
        emails_page_manager.clone(),
        None,
        emails_schema.clone(),
    )?;
    
    let hash_join = HashJoinOperator::new(
        Box::new(users_operator_hash),
        Box::new(emails_operator_hash),
        join_condition.clone(),
        JoinType::Inner,
        1000, // hash_table_size
    )?;
    
    println!("   Создан Hash Join оператор");
    println!("   Схема: {:?}", hash_join.get_schema()?);
    println!("   Статистика: {:?}", hash_join.get_statistics());
    println!();

    // Пример 3: Merge Join
    println!("3. Merge Join:");
    let users_operator_merge = TableScanOperator::new(
        "users".to_string(),
        users_page_manager.clone(),
        None,
        users_schema.clone(),
    )?;
    
    let emails_operator_merge = TableScanOperator::new(
        "emails".to_string(),
        emails_page_manager.clone(),
        None,
        emails_schema.clone(),
    )?;
    
    let merge_join = MergeJoinOperator::new(
        Box::new(users_operator_merge),
        Box::new(emails_operator_merge),
        join_condition.clone(),
        JoinType::Inner,
    )?;
    
    println!("   Создан Merge Join оператор");
    println!("   Схема: {:?}", merge_join.get_schema()?);
    println!("   Статистика: {:?}", merge_join.get_statistics());
    println!();

    // Пример 4: Различные типы соединений
    println!("4. Различные типы соединений:");
    let join_types = vec![
        JoinType::Inner,
        JoinType::LeftOuter,
        JoinType::RightOuter,
        JoinType::FullOuter,
    ];
    
    for (i, join_type) in join_types.iter().enumerate() {
        println!("   Тип {}: {:?}", i + 1, join_type);
    }
    println!();

    // Пример 5: Различные операторы соединения
    println!("5. Различные операторы соединения:");
    let join_operators = vec![
        JoinOperator::Equal,
        JoinOperator::NotEqual,
        JoinOperator::LessThan,
        JoinOperator::LessThanOrEqual,
        JoinOperator::GreaterThan,
        JoinOperator::GreaterThanOrEqual,
    ];
    
    for (i, operator) in join_operators.iter().enumerate() {
        println!("   Оператор {}: {:?}", i + 1, operator);
    }
    println!();

    // Пример 6: Сложные условия соединения
    println!("6. Сложные условия соединения:");
    let complex_conditions = vec![
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
    
    for (i, condition) in complex_conditions.iter().enumerate() {
        println!("   Условие {}: {} {} {}", 
            i + 1, 
            condition.left_column, 
            match condition.operator {
                JoinOperator::Equal => "=",
                JoinOperator::NotEqual => "!=",
                JoinOperator::LessThan => "<",
                JoinOperator::LessThanOrEqual => "<=",
                JoinOperator::GreaterThan => ">",
                JoinOperator::GreaterThanOrEqual => ">=",
            },
            condition.right_column
        );
    }
    println!();

    // Пример 7: Работа с операторами через трейт
    println!("7. Работа с операторами через трейт Operator:");
    let users_operator_trait = TableScanOperator::new(
        "users".to_string(),
        users_page_manager.clone(),
        None,
        users_schema.clone(),
    )?;
    
    let emails_operator_trait = TableScanOperator::new(
        "emails".to_string(),
        emails_page_manager.clone(),
        None,
        emails_schema.clone(),
    )?;
    
    let mut operator: Box<dyn Operator> = Box::new(NestedLoopJoinOperator::new(
        Box::new(users_operator_trait),
        Box::new(emails_operator_trait),
        join_condition.clone(),
        JoinType::Inner,
        100,
    )?);
    
    println!("   Создан оператор через трейт");
    println!("   Схема: {:?}", operator.get_schema()?);
    println!("   Статистика: {:?}", operator.get_statistics());
    
    // Сбрасываем оператор
    operator.reset()?;
    println!("   Оператор сброшен");
    println!("   Новая статистика: {:?}", operator.get_statistics());
    println!();

    // Пример 8: Сравнение производительности
    println!("8. Сравнение производительности:");
    
    // Nested Loop Join
    let start_time = std::time::Instant::now();
    let nested_loop_join_perf = NestedLoopJoinOperator::new(
        Box::new(TableScanOperator::new(
            "users".to_string(),
            users_page_manager.clone(),
            None,
            users_schema.clone(),
        )?),
        Box::new(TableScanOperator::new(
            "emails".to_string(),
            emails_page_manager.clone(),
            None,
            emails_schema.clone(),
        )?),
        join_condition.clone(),
        JoinType::Inner,
        100,
    )?;
    let nested_time = start_time.elapsed();
    println!("   Nested Loop Join создан за {:?}", nested_time);
    
    // Hash Join
    let start_time = std::time::Instant::now();
    let hash_join_perf = HashJoinOperator::new(
        Box::new(TableScanOperator::new(
            "users".to_string(),
            users_page_manager.clone(),
            None,
            users_schema.clone(),
        )?),
        Box::new(TableScanOperator::new(
            "emails".to_string(),
            emails_page_manager.clone(),
            None,
            emails_schema.clone(),
        )?),
        join_condition.clone(),
        JoinType::Inner,
        1000,
    )?;
    let hash_time = start_time.elapsed();
    println!("   Hash Join создан за {:?}", hash_time);
    
    // Merge Join
    let start_time = std::time::Instant::now();
    let merge_join_perf = MergeJoinOperator::new(
        Box::new(TableScanOperator::new(
            "users".to_string(),
            users_page_manager.clone(),
            None,
            users_schema.clone(),
        )?),
        Box::new(TableScanOperator::new(
            "emails".to_string(),
            emails_page_manager.clone(),
            None,
            emails_schema.clone(),
        )?),
        join_condition.clone(),
        JoinType::Inner,
    )?;
    let merge_time = start_time.elapsed();
    println!("   Merge Join создан за {:?}", merge_time);
    println!();

    // Пример 9: Статистика выполнения
    println!("9. Статистика выполнения:");
    let stats = nested_loop_join_perf.get_statistics();
    println!("   Обработанных строк: {}", stats.rows_processed);
    println!("   Возвращенных строк: {}", stats.rows_returned);
    println!("   Время выполнения: {} мс", stats.execution_time_ms);
    println!("   I/O операций: {}", stats.io_operations);
    println!("   Операций с памятью: {}", stats.memory_operations);
    println!("   Использовано памяти: {} байт", stats.memory_used_bytes);
    println!();

    // Пример 10: Комбинирование операторов
    println!("10. Комбинирование операторов:");
    let users_base = TableScanOperator::new(
        "users".to_string(),
        users_page_manager.clone(),
        None,
        users_schema.clone(),
    )?;
    
    let emails_base = TableScanOperator::new(
        "emails".to_string(),
        emails_page_manager.clone(),
        None,
        emails_schema.clone(),
    )?;
    
    let combined_join = NestedLoopJoinOperator::new(
        Box::new(users_base),
        Box::new(emails_base),
        join_condition,
        JoinType::Inner,
        50, // меньший размер блока для демонстрации
    )?;
    
    println!("   Создана комбинация операторов:");
    println!("   TableScan (users) -> NestedLoopJoin <- TableScan (emails)");
    println!("   Схема: {:?}", combined_join.get_schema()?);
    println!();

    println!("=== Пример завершен ===");
    Ok(())
}


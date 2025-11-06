//! Пример использования операторов сканирования

use rustdb::executor::{
    ConditionalScanOperator, IndexCondition, IndexOperator, IndexScanOperator, Operator,
    RangeScanOperator, ScanOperatorFactory, TableScanOperator,
};
// removed unused imports
use rustdb::common::Result;
use rustdb::storage::index::BPlusTree;
use rustdb::storage::page_manager::{PageManager, PageManagerConfig};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

fn main() -> Result<()> {
    println!("=== Пример использования операторов сканирования ===\n");

    // Создаем менеджер страниц
    let page_manager = Arc::new(Mutex::new(PageManager::new(
        PathBuf::from("./data"),
        "users",
        PageManagerConfig::default(),
    )?));
    let schema = vec!["id".to_string(), "name".to_string(), "age".to_string()];

    // Пример 1: TableScan оператор
    println!("1. TableScan оператор:");
    let table_scan = TableScanOperator::new(
        "users".to_string(),
        page_manager.clone(),
        None,
        schema.clone(),
    )?;

    println!("   Создан TableScan оператор для таблицы 'users'");
    println!("   Схема: {:?}", table_scan.get_schema()?);
    println!("   Статистика: {:?}", table_scan.get_statistics());
    println!();

    // Пример 2: TableScan с фильтром
    println!("2. TableScan с фильтром:");
    let table_scan_filtered = TableScanOperator::new(
        "users".to_string(),
        page_manager.clone(),
        Some("age > 18".to_string()),
        schema.clone(),
    )?;

    println!("   Создан TableScan оператор с фильтром 'age > 18'");
    println!("   Схема: {:?}", table_scan_filtered.get_schema()?);
    println!();

    // Пример 3: IndexScan оператор
    println!("3. IndexScan оператор:");
    let index = Arc::new(Mutex::new(BPlusTree::new(4)));
    let search_conditions = vec![IndexCondition {
        column: "id".to_string(),
        operator: IndexOperator::Equal,
        value: "1".to_string(),
    }];

    let index_scan = IndexScanOperator::new(
        "users".to_string(),
        "idx_users_id".to_string(),
        index,
        page_manager.clone(),
        search_conditions,
        schema.clone(),
    )?;

    println!("   Создан IndexScan оператор для индекса 'idx_users_id'");
    println!("   Условие поиска: id = 1");
    println!("   Схема: {:?}", index_scan.get_schema()?);
    println!();

    // Пример 4: RangeScan оператор
    println!("4. RangeScan оператор:");
    let base_operator = TableScanOperator::new(
        "users".to_string(),
        page_manager.clone(),
        None,
        schema.clone(),
    )?;

    let range_scan = RangeScanOperator::new(
        Box::new(base_operator),
        Some("1".to_string()),
        Some("10".to_string()),
    )?;

    println!("   Создан RangeScan оператор для диапазона [1, 10]");
    println!("   Схема: {:?}", range_scan.get_schema()?);
    println!();

    // Пример 5: ConditionalScan оператор
    println!("5. ConditionalScan оператор:");
    let base_operator_cond = TableScanOperator::new(
        "users".to_string(),
        page_manager.clone(),
        None,
        schema.clone(),
    )?;

    let conditional_scan = ConditionalScanOperator::new(
        Box::new(base_operator_cond),
        "name LIKE 'John%'".to_string(),
    )?;

    println!("   Создан ConditionalScan оператор с условием 'name LIKE John%'");
    println!("   Схема: {:?}", conditional_scan.get_schema()?);
    println!();

    // Пример 6: Фабрика операторов
    println!("6. Фабрика операторов:");
    let mut factory = ScanOperatorFactory::new(page_manager.clone());

    // Добавляем индекс
    let index_for_factory = Arc::new(Mutex::new(BPlusTree::new(4)));
    factory.add_index("users".to_string(), index_for_factory);

    // Создаем операторы через фабрику
    let table_scan_from_factory =
        factory.create_table_scan("users".to_string(), None, schema.clone())?;

    println!("   Создан TableScan через фабрику");
    println!("   Схема: {:?}", table_scan_from_factory.get_schema()?);

    let range_scan_from_factory = factory.create_range_scan(
        table_scan_from_factory,
        Some("1".to_string()),
        Some("5".to_string()),
    )?;

    println!("   Создан RangeScan через фабрику");
    println!("   Схема: {:?}", range_scan_from_factory.get_schema()?);
    println!();

    // Пример 7: Работа с операторами через трейт
    println!("7. Работа с операторами через трейт Operator:");
    let mut operator: Box<dyn Operator> = Box::new(TableScanOperator::new(
        "users".to_string(),
        page_manager.clone(),
        None,
        schema.clone(),
    )?);

    println!("   Создан оператор через трейт");
    println!("   Схема: {:?}", operator.get_schema()?);
    println!("   Статистика: {:?}", operator.get_statistics());

    // Сбрасываем оператор
    operator.reset()?;
    println!("   Оператор сброшен");
    println!("   Новая статистика: {:?}", operator.get_statistics());
    println!();

    // Пример 8: Различные типы условий индекса
    println!("8. Различные типы условий индекса:");
    let conditions = [
        IndexCondition {
            column: "id".to_string(),
            operator: IndexOperator::Equal,
            value: "1".to_string(),
        },
        IndexCondition {
            column: "age".to_string(),
            operator: IndexOperator::GreaterThan,
            value: "18".to_string(),
        },
        IndexCondition {
            column: "name".to_string(),
            operator: IndexOperator::LessThanOrEqual,
            value: "Z".to_string(),
        },
        IndexCondition {
            column: "score".to_string(),
            operator: IndexOperator::Between,
            value: "80,100".to_string(),
        },
    ];

    for (i, condition) in conditions.iter().enumerate() {
        println!(
            "   Условие {}: {} {} {}",
            i + 1,
            condition.column,
            match condition.operator {
                IndexOperator::Equal => "=",
                IndexOperator::LessThan => "<",
                IndexOperator::LessThanOrEqual => "<=",
                IndexOperator::GreaterThan => ">",
                IndexOperator::GreaterThanOrEqual => ">=",
                IndexOperator::Between => "BETWEEN",
                IndexOperator::In => "IN",
            },
            condition.value
        );
    }
    println!();

    // Пример 9: Статистика выполнения
    println!("9. Статистика выполнения:");
    let operator_for_stats =
        TableScanOperator::new("users".to_string(), page_manager.clone(), None, schema)?;

    let stats = operator_for_stats.get_statistics();
    println!("   Обработанных строк: {}", stats.rows_processed);
    println!("   Возвращенных строк: {}", stats.rows_returned);
    println!("   Время выполнения: {} мс", stats.execution_time_ms);
    println!("   I/O операций: {}", stats.io_operations);
    println!("   Операций с памятью: {}", stats.memory_operations);
    println!("   Использовано памяти: {} байт", stats.memory_used_bytes);
    println!();

    // Пример 10: Комбинирование операторов
    println!("10. Комбинирование операторов:");
    let base = TableScanOperator::new(
        "users".to_string(),
        page_manager.clone(),
        None,
        vec!["id".to_string(), "name".to_string(), "age".to_string()],
    )?;

    let range_filtered = RangeScanOperator::new(
        Box::new(base),
        Some("1".to_string()),
        Some("100".to_string()),
    )?;

    let final_operator =
        ConditionalScanOperator::new(Box::new(range_filtered), "age > 18".to_string())?;

    println!("   Создана цепочка операторов:");
    println!("   TableScan -> RangeScan -> ConditionalScan");
    println!("   Схема: {:?}", final_operator.get_schema()?);
    println!();

    println!("=== Пример завершен ===");
    Ok(())
}

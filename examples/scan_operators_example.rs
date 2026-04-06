//! Example of using scan operators

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
    println!("=== Example of using scan operators ===\n");

    // Creating a page manager
    let page_manager = Arc::new(Mutex::new(PageManager::new(
        PathBuf::from("./data"),
        "users",
        PageManagerConfig::default(),
    )?));
    let schema = vec!["id".to_string(), "name".to_string(), "age".to_string()];

    // Example 1: TableScan operator
    println!("1. TableScan operator:");
    let table_scan = TableScanOperator::new(
        "users".to_string(),
        page_manager.clone(),
        None,
        schema.clone(),
    )?;

    println!("Created TableScan operator for table 'users'");
    println!("Scheme: {:?}", table_scan.get_schema()?);
    println!("Statistics: {:?}", table_scan.get_statistics());
    println!();

    // Example 2: TableScan with filter
    println!("2. TableScan with filter:");
    let table_scan_filtered = TableScanOperator::new(
        "users".to_string(),
        page_manager.clone(),
        Some("age > 18".to_string()),
        schema.clone(),
    )?;

    println!("Created TableScan operator with filter 'age > 18'");
    println!("Scheme: {:?}", table_scan_filtered.get_schema()?);
    println!();

    // Example 3: IndexScan operator
    println!("3. IndexScan operator:");
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

    println!("Created IndexScan operator for index 'idx_users_id'");
    println!("Search condition: id = 1");
    println!("Scheme: {:?}", index_scan.get_schema()?);
    println!();

    // Example 4: RangeScan operator
    println!("4. RangeScan operator:");
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

    println!("Created a RangeScan operator for the range [1, 10]");
    println!("Scheme: {:?}", range_scan.get_schema()?);
    println!();

    // Example 5: ConditionalScan operator
    println!("5. ConditionalScan operator:");
    let base_operator_cond = TableScanOperator::new(
        "users".to_string(),
        page_manager.clone(),
        None,
        schema.clone(),
    )?;

    let conditional_scan = ConditionalScanOperator::new(
        Box::new(base_operator_cond),
        "name LIKE 'John%'".to_string(),
        None,
        None,
    )?;

    println!("Created a ConditionalScan operator with the condition 'name LIKE John%'");
    println!("Scheme: {:?}", conditional_scan.get_schema()?);
    println!();

    // Example 6: Operator Factory
    println!("6. Factory operators:");
    let mut factory = ScanOperatorFactory::new(page_manager.clone());

    // Adding an index
    let index_for_factory = Arc::new(Mutex::new(BPlusTree::new_default()));
    factory.add_index("users", "idx_users_id", index_for_factory);

    // Creating operators through a factory
    let table_scan_from_factory =
        factory.create_table_scan("users".to_string(), None, schema.clone())?;

    println!("Created TableScan via factory");
    println!("Scheme: {:?}", table_scan_from_factory.get_schema()?);

    let range_scan_from_factory = factory.create_range_scan(
        table_scan_from_factory,
        Some("1".to_string()),
        Some("5".to_string()),
    )?;

    println!("Created RangeScan via factory");
    println!("Scheme: {:?}", range_scan_from_factory.get_schema()?);
    println!();

    // Example 7: Working with operators through traits
    println!("7. Working with operators using the Operator trait:");
    let mut operator: Box<dyn Operator> = Box::new(TableScanOperator::new(
        "users".to_string(),
        page_manager.clone(),
        None,
        schema.clone(),
    )?);

    println!("An operator via trait has been created");
    println!("Scheme: {:?}", operator.get_schema()?);
    println!("Statistics: {:?}", operator.get_statistics());

    // Resetting the operator
    operator.reset()?;
    println!("Operator reset");
    println!("New statistics: {:?}", operator.get_statistics());
    println!();

    // Example 8: Different Types of Index Conditions
    println!("8. Different types of index conditions:");
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
            "Condition {}: {} {} {}",
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

    // Example 9: Execution Statistics
    println!("9. Execution statistics:");
    let operator_for_stats =
        TableScanOperator::new("users".to_string(), page_manager.clone(), None, schema)?;

    let stats = operator_for_stats.get_statistics();
    println!("Rows processed: {}", stats.rows_processed);
    println!("Returned rows: {}", stats.rows_returned);
    println!("Execution time: {}ms", stats.execution_time_ms);
    println!("I/O operations: {}", stats.io_operations);
    println!("Memory operations: {}", stats.memory_operations);
    println!("Memory used: {} bytes", stats.memory_used_bytes);
    println!();

    // Example 10: Combining Operators
    println!("10. Combining operators:");
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
        ConditionalScanOperator::new(Box::new(range_filtered), "age > 18".to_string(), None, None)?;

    println!("A chain of operators has been created:");
    println!("   TableScan -> RangeScan -> ConditionalScan");
    println!("Scheme: {:?}", final_operator.get_schema()?);
    println!();

    println!("=== Example complete ===");
    Ok(())
}

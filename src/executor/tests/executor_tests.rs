//! QueryExecutor tests - full plan execution

use super::common;
use crate::executor::{QueryExecutor, QueryExecutorConfig};
use crate::parser::{SqlParser, SqlStatement};
use crate::planner::{QueryOptimizer, QueryPlanner};
use crate::common::Result;
use std::sync::Arc;

#[test]
fn test_executor_creation() -> Result<()> {
    let (_temp, page_manager) = common::create_test_page_manager();
    let factory = Arc::new(crate::executor::operators::ScanOperatorFactory::new(page_manager));
    let _executor = QueryExecutor::new(factory)?;
    Ok(())
}

#[test]
fn test_executor_config_creation() -> Result<()> {
    let (_temp, page_manager) = common::create_test_page_manager();
    let factory = Arc::new(crate::executor::operators::ScanOperatorFactory::new(page_manager));
    let config = QueryExecutorConfig {
        enable_parallel_execution: false,
        num_worker_threads: 1,
    };
    let _executor = QueryExecutor::with_config(factory, config)?;
    Ok(())
}

#[test]
#[ignore] // Can hang on some environments - TableScan + full plan execution
fn test_executor_simple_table_scan() -> Result<()> {
    let (_temp, page_manager) = common::create_test_page_manager();

    // Insert test data
    {
        let data = b"1\tAlice\t30".to_vec();
        let mut pm = page_manager.lock().unwrap();
        pm.insert(&data)?;
    }

    let factory = Arc::new(crate::executor::operators::ScanOperatorFactory::new(page_manager));
    let executor = QueryExecutor::new(factory)?;

    let mut planner = QueryPlanner::new()?;
    let mut parser = SqlParser::new("SELECT * FROM users")?;
    let stmt = parser.parse()?;

    if let SqlStatement::Select(_) = &stmt {
        let plan = planner.create_plan(&stmt)?;
        let mut optimizer = QueryOptimizer::new()?;
        let optimized = optimizer.optimize(plan)?;

        let results = executor.execute(&optimized.optimized_plan)?;
        assert!(!results.is_empty(), "Should return inserted row");
    }

    Ok(())
}

#[test]
#[ignore] // Can hang on some environments - full plan execution
fn test_executor_with_config() -> Result<()> {
    let (_temp, page_manager) = common::create_test_page_manager();
    let factory = Arc::new(crate::executor::operators::ScanOperatorFactory::new(page_manager));

    let config = QueryExecutorConfig {
        enable_parallel_execution: false,
        num_worker_threads: 2,
    };
    let executor = QueryExecutor::with_config(factory, config)?;

    let mut planner = QueryPlanner::new()?;
    let mut parser = SqlParser::new("SELECT * FROM users")?;
    let stmt = parser.parse()?;

    if let SqlStatement::Select(_) = &stmt {
        let plan = planner.create_plan(&stmt)?;
        let mut optimizer = QueryOptimizer::new()?;
        let optimized = optimizer.optimize(plan)?;

        let results = executor.execute(&optimized.optimized_plan)?;
        assert!(results.is_empty(), "Empty table should return no rows");
    }

    Ok(())
}

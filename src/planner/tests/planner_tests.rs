//! Тесты для планировщика запросов

use crate::planner::{QueryPlanner, ExecutionPlan, PlanNode};
use crate::parser::{SqlParser, SqlStatement};
use crate::common::Result;

#[test]
fn test_create_simple_select_plan() -> Result<()> {
    let mut planner = QueryPlanner::new()?;
    let mut parser = SqlParser::new("SELECT * FROM users")?;
    
    let statement = parser.parse()?;
    let plan = planner.create_plan(&statement)?;
    
    assert!(matches!(plan.root, PlanNode::Projection(_)));
    
    Ok(())
}

#[test]
fn test_create_select_with_where_plan() -> Result<()> {
    let mut planner = QueryPlanner::new()?;
    let mut parser = SqlParser::new("SELECT name, age FROM users WHERE age > 18")?;
    
    let statement = parser.parse()?;
    let plan = planner.create_plan(&statement)?;
    
    // Проверяем, что план создан успешно
    assert!(matches!(plan.root, PlanNode::Projection(_)));
    
    Ok(())
}

#[test]
fn test_create_insert_plan() -> Result<()> {
    let mut planner = QueryPlanner::new()?;
    let mut parser = SqlParser::new("INSERT INTO users (name, age) VALUES ('John', 25)")?;
    
    let statement = parser.parse()?;
    let plan = planner.create_plan(&statement)?;
    
    assert!(matches!(plan.root, PlanNode::Insert(_)));
    
    Ok(())
}

#[test]
fn test_create_update_plan() -> Result<()> {
    let mut planner = QueryPlanner::new()?;
    let mut parser = SqlParser::new("UPDATE users SET age = 26")?;
    
    let statement = parser.parse()?;
    let plan = planner.create_plan(&statement)?;
    
    assert!(matches!(plan.root, PlanNode::Update(_)));
    
    Ok(())
}

#[test]
fn test_create_delete_plan() -> Result<()> {
    let mut planner = QueryPlanner::new()?;
    let mut parser = SqlParser::new("DELETE FROM users")?;
    
    let statement = parser.parse()?;
    let plan = planner.create_plan(&statement)?;
    
    assert!(matches!(plan.root, PlanNode::Delete(_)));
    
    Ok(())
}

#[test]
fn test_plan_metadata() -> Result<()> {
    let mut planner = QueryPlanner::new()?;
    let mut parser = SqlParser::new("SELECT * FROM users")?;
    
    let statement = parser.parse()?;
    let plan = planner.create_plan(&statement)?;
    
    // Проверяем, что метаданные созданы
    assert!(plan.metadata.estimated_cost >= 0.0);
    assert!(plan.metadata.estimated_rows >= 0);
    assert!(plan.metadata.statistics.operator_count > 0);
    
    Ok(())
}

#[test]
fn test_planner_settings() -> Result<()> {
    let planner = QueryPlanner::new()?;
    
    let settings = planner.settings();
    assert!(settings.enable_plan_cache);
    assert!(settings.max_cache_size > 0);
    
    Ok(())
}

#[test]
fn test_cache_statistics() -> Result<()> {
    let planner = QueryPlanner::new()?;
    
    let stats = planner.cache_stats();
    assert_eq!(stats.size, 0);
    assert!(stats.max_size > 0);
    
    Ok(())
}

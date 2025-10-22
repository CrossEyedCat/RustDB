//! Тесты для оптимизатора запросов

use crate::common::Result;
use crate::parser::{SqlParser, SqlStatement};
use crate::planner::{ExecutionPlan, PlanNode, QueryOptimizer, QueryPlanner};

#[test]
fn test_optimizer_creation() -> Result<()> {
    let optimizer = QueryOptimizer::new()?;
    let settings = optimizer.settings();

    assert!(settings.enable_join_reordering);
    assert!(settings.enable_index_selection);
    assert!(settings.enable_expression_simplification);
    assert!(settings.enable_predicate_pushdown);

    Ok(())
}

#[test]
fn test_optimize_simple_plan() -> Result<()> {
    let mut planner = QueryPlanner::new()?;
    let mut optimizer = QueryOptimizer::new()?;
    let mut parser = SqlParser::new("SELECT * FROM users")?;

    let statement = parser.parse()?;
    let plan = planner.create_plan(&statement)?;
    let result = optimizer.optimize(plan)?;

    // Проверяем, что оптимизация прошла успешно
    assert!(result.statistics.optimization_time_ms >= 0);
    assert!(result.messages.len() >= 0);

    Ok(())
}

#[test]
fn test_optimize_plan_with_where() -> Result<()> {
    let mut planner = QueryPlanner::new()?;
    let mut optimizer = QueryOptimizer::new()?;
    let mut parser = SqlParser::new("SELECT name FROM users WHERE age > 18")?;

    let statement = parser.parse()?;
    let plan = planner.create_plan(&statement)?;
    let result = optimizer.optimize(plan)?;

    // Проверяем, что оптимизация прошла успешно
    assert!(result.statistics.optimization_time_ms >= 0);

    Ok(())
}

#[test]
fn test_optimizer_settings() -> Result<()> {
    let optimizer = QueryOptimizer::new()?;

    let settings = optimizer.settings();
    assert!(settings.enable_join_reordering);
    assert!(settings.enable_index_selection);
    assert!(settings.enable_expression_simplification);
    assert!(settings.enable_predicate_pushdown);
    assert!(settings.max_optimization_iterations > 0);
    assert!(settings.cost_threshold > 0.0);

    Ok(())
}

#[test]
fn test_optimizer_statistics() -> Result<()> {
    let optimizer = QueryOptimizer::new()?;

    let stats = optimizer.statistics();
    assert_eq!(stats.optimizations_applied, 0);
    assert_eq!(stats.optimization_time_ms, 0);
    assert_eq!(stats.cost_improvement_percent, 0.0);

    Ok(())
}

#[test]
fn test_reset_statistics() -> Result<()> {
    let mut optimizer = QueryOptimizer::new()?;

    // Сначала получаем статистику
    let _initial_stats = optimizer.statistics().clone();

    // Сбрасываем статистику
    optimizer.reset_statistics();

    // Проверяем, что статистика сброшена
    let reset_stats = optimizer.statistics();
    assert_eq!(reset_stats.optimizations_applied, 0);
    assert_eq!(reset_stats.optimization_time_ms, 0);
    assert_eq!(reset_stats.cost_improvement_percent, 0.0);

    Ok(())
}

#[test]
fn test_optimizer_with_custom_settings() -> Result<()> {
    use crate::planner::OptimizerSettings;

    let settings = OptimizerSettings {
        enable_join_reordering: false,
        enable_index_selection: false,
        enable_expression_simplification: true,
        enable_predicate_pushdown: true,
        max_optimization_iterations: 5,
        cost_threshold: 500.0,
        enable_debug_logging: true,
    };

    let optimizer = QueryOptimizer::with_settings(settings)?;
    let actual_settings = optimizer.settings();

    assert!(!actual_settings.enable_join_reordering);
    assert!(!actual_settings.enable_index_selection);
    assert!(actual_settings.enable_expression_simplification);
    assert!(actual_settings.enable_predicate_pushdown);
    assert_eq!(actual_settings.max_optimization_iterations, 5);
    assert_eq!(actual_settings.cost_threshold, 500.0);
    assert!(actual_settings.enable_debug_logging);

    Ok(())
}

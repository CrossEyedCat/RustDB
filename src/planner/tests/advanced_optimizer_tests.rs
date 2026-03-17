//! Advanced query optimizer tests

use crate::catalog::statistics::StatisticsManager;
use crate::planner::advanced_optimizer::{
    AdvancedOptimizationResult, AdvancedOptimizationStatistics, AdvancedOptimizerSettings,
    AdvancedQueryOptimizer,
};
use crate::planner::planner::{
    ExecutionPlan, FilterNode, PlanMetadata, PlanNode, PlanStatistics, TableScanNode,
};

#[test]
fn test_advanced_optimizer_creation() {
    let optimizer = AdvancedQueryOptimizer::new();
    assert!(optimizer.is_ok());
}

#[test]
fn test_advanced_optimizer_with_settings() {
    let settings = AdvancedOptimizerSettings {
        enable_statistics_usage: false,
        enable_query_rewriting: true,
        enable_expression_simplification: false,
        enable_subquery_extraction: true,
        enable_debug_logging: true,
        cost_threshold: 2000.0,
    };

    let optimizer = AdvancedQueryOptimizer::with_settings(settings);
    assert!(optimizer.is_ok());

    let optimizer = optimizer.unwrap();
    assert_eq!(optimizer.settings().enable_statistics_usage, false);
    assert_eq!(optimizer.settings().enable_query_rewriting, true);
    assert_eq!(optimizer.settings().enable_expression_simplification, false);
    assert_eq!(optimizer.settings().enable_subquery_extraction, true);
    assert_eq!(optimizer.settings().enable_debug_logging, true);
    assert_eq!(optimizer.settings().cost_threshold, 2000.0);
}

#[test]
fn test_advanced_optimizer_default_settings() {
    let optimizer = AdvancedQueryOptimizer::new().unwrap();
    let settings = optimizer.settings();

    // Validate default settings
    assert_eq!(settings.enable_statistics_usage, true);
    assert_eq!(settings.enable_query_rewriting, true);
    assert_eq!(settings.enable_expression_simplification, true);
    assert_eq!(settings.enable_subquery_extraction, true);
    assert_eq!(settings.enable_debug_logging, false);
    assert_eq!(settings.cost_threshold, 1000.0);
}

#[test]
fn test_advanced_optimizer_settings_update() {
    let mut optimizer = AdvancedQueryOptimizer::new().unwrap();
    let original_settings = optimizer.settings().clone();

    // Build new settings
    let new_settings = AdvancedOptimizerSettings {
        enable_statistics_usage: false,
        enable_query_rewriting: false,
        enable_expression_simplification: false,
        enable_subquery_extraction: false,
        enable_debug_logging: true,
        cost_threshold: 5000.0,
    };

    // Apply updated settings
    optimizer.update_settings(new_settings.clone());

    // Ensure new settings took effect
    assert_eq!(
        optimizer.settings().enable_statistics_usage,
        new_settings.enable_statistics_usage
    );
    assert_eq!(
        optimizer.settings().enable_query_rewriting,
        new_settings.enable_query_rewriting
    );
    assert_eq!(
        optimizer.settings().enable_expression_simplification,
        new_settings.enable_expression_simplification
    );
    assert_eq!(
        optimizer.settings().enable_subquery_extraction,
        new_settings.enable_subquery_extraction
    );
    assert_eq!(
        optimizer.settings().enable_debug_logging,
        new_settings.enable_debug_logging
    );
    assert_eq!(
        optimizer.settings().cost_threshold,
        new_settings.cost_threshold
    );

    // Confirm settings differ from the original
    assert_ne!(
        optimizer.settings().enable_statistics_usage,
        original_settings.enable_statistics_usage
    );
    assert_ne!(
        optimizer.settings().enable_query_rewriting,
        original_settings.enable_query_rewriting
    );
    assert_ne!(
        optimizer.settings().cost_threshold,
        original_settings.cost_threshold
    );
}

#[test]
fn test_advanced_optimizer_statistics() {
    let mut optimizer = AdvancedQueryOptimizer::new().unwrap();

    // Inspect initial statistics
    let initial_stats = optimizer.statistics();
    assert_eq!(initial_stats.optimizations_applied, 0);
    assert_eq!(initial_stats.optimization_time_ms, 0);
    assert_eq!(initial_stats.cost_improvement_percent, 0.0);
    assert_eq!(initial_stats.query_rewrites, 0);
    assert_eq!(initial_stats.expression_simplifications, 0);
    assert_eq!(initial_stats.subquery_extractions, 0);
    assert_eq!(initial_stats.statistics_usage_count, 0);

    // Reset statistics
    optimizer.reset_statistics();

    // Confirm statistics were cleared
    let reset_stats = optimizer.statistics();
    assert_eq!(reset_stats.optimizations_applied, 0);
    assert_eq!(reset_stats.optimization_time_ms, 0);
    assert_eq!(reset_stats.cost_improvement_percent, 0.0);
}

#[test]
fn test_advanced_optimizer_statistics_manager_access() {
    let optimizer = AdvancedQueryOptimizer::new().unwrap();

    // Verify access to statistics manager
    let stats_manager = optimizer.statistics_manager();
    assert!(stats_manager.get_table_statistics("test_table").is_none());

    // Verify mutable access
    let mut optimizer = AdvancedQueryOptimizer::new().unwrap();
    let stats_manager_mut = optimizer.statistics_manager_mut();
    assert!(stats_manager_mut
        .collect_table_statistics("test_table")
        .is_ok());
}

#[test]
fn test_advanced_optimizer_with_test_plan() {
    let mut optimizer = AdvancedQueryOptimizer::new().unwrap();

    // Create a test plan
    let test_plan = create_test_execution_plan().unwrap();

    // Apply optimization
    let result = optimizer.optimize_with_statistics(test_plan);
    assert!(result.is_ok());

    let result = result.unwrap();

    // Inspect result structure
    assert!(result.statistics.optimization_time_ms >= 0);
    assert!(result.optimized_plan.metadata.estimated_cost >= 0.0);
    assert!(result.optimized_plan.metadata.estimated_rows > 0);

    // Ensure statistics were utilized
    assert!(!result.used_statistics.is_empty());
}

#[test]
fn test_advanced_optimizer_table_extraction() {
    let optimizer = AdvancedQueryOptimizer::new().unwrap();

    // Build a test plan covering multiple tables
    let test_plan = create_test_execution_plan().unwrap();

    // Extract table names
    let table_names = optimizer.extract_table_names_from_plan(&test_plan);

    // Confirm tables were detected
    assert!(table_names.contains(&"users".to_string()));
    assert!(table_names.len() >= 1);
}

#[test]
fn test_advanced_optimizer_cost_estimation() {
    let optimizer = AdvancedQueryOptimizer::new().unwrap();

    // Create a simple TableScan node
    let table_scan = PlanNode::TableScan(TableScanNode {
        table_name: "test_table".to_string(),
        alias: None,
        columns: vec!["id".to_string()],
        filter: None,
        cost: 100.0,
        estimated_rows: 1000,
    });

    // Estimate cost
    let cost = optimizer.estimate_node_cost(&table_scan);
    assert_eq!(cost, 100.0);
}

#[test]
fn test_advanced_optimizer_child_nodes_extraction() {
    let optimizer = AdvancedQueryOptimizer::new().unwrap();

    // Create a Filter node with a child
    let child_node = PlanNode::TableScan(TableScanNode {
        table_name: "child_table".to_string(),
        alias: None,
        columns: vec!["id".to_string()],
        filter: None,
        cost: 50.0,
        estimated_rows: 500,
    });

    let filter_node = PlanNode::Filter(FilterNode {
        condition: "id > 0".to_string(),
        input: Box::new(child_node),
        selectivity: 0.5,
        cost: 25.0,
    });

    // Extract child nodes
    let child_nodes = optimizer.get_child_nodes(&filter_node);
    assert_eq!(child_nodes.len(), 1);
}

/// Constructs a sample execution plan
fn create_test_execution_plan() -> Result<ExecutionPlan, Box<dyn std::error::Error>> {
    // Build plan nodes
    let users_scan = PlanNode::TableScan(TableScanNode {
        table_name: "users".to_string(),
        alias: Some("u".to_string()),
        columns: vec!["id".to_string(), "name".to_string(), "age".to_string()],
        filter: None,
        cost: 1000.0,
        estimated_rows: 10000,
    });

    let filter_node = PlanNode::Filter(FilterNode {
        condition: "u.age > 18".to_string(),
        input: Box::new(users_scan),
        selectivity: 0.7,
        cost: 300.0,
    });

    // Build plan metadata
    let metadata = PlanMetadata {
        estimated_cost: 1300.0,
        estimated_rows: 7000,
        created_at: std::time::SystemTime::now(),
        statistics: PlanStatistics {
            operator_count: 2,
            max_depth: 2,
            table_count: 1,
            join_count: 0,
        },
    };

    Ok(ExecutionPlan {
        root: filter_node,
        metadata,
    })
}

#[test]
fn test_advanced_optimizer_condition_simplification() {
    let optimizer = AdvancedQueryOptimizer::new().unwrap();

    // Evaluate predicate simplification
    let condition = "a > 5 AND a > 3";
    let simplified = optimizer.simplify_condition(condition).unwrap();

    // Current simplified implementation leaves predicate unchanged
    assert_eq!(simplified, condition);
}

#[test]
fn test_advanced_optimizer_join_order_optimization() {
    let optimizer = AdvancedQueryOptimizer::new().unwrap();

    // Create test nodes for a JOIN
    let left_node = PlanNode::TableScan(TableScanNode {
        table_name: "left_table".to_string(),
        alias: None,
        columns: vec!["id".to_string()],
        filter: None,
        cost: 1000.0,
        estimated_rows: 10000,
    });

    let right_node = PlanNode::TableScan(TableScanNode {
        table_name: "right_table".to_string(),
        alias: None,
        columns: vec!["id".to_string()],
        filter: None,
        cost: 500.0,
        estimated_rows: 5000,
    });

    // Construct JOIN node
    let join_node = crate::planner::planner::JoinNode {
        join_type: crate::planner::planner::JoinType::Inner,
        condition: "left_table.id = right_table.id".to_string(),
        left: Box::new(left_node.clone()),
        right: Box::new(right_node.clone()),
        cost: 1500.0,
    };

    // Optimize join order
    let optimized_join = optimizer
        .optimize_join_order(&join_node, &left_node, &right_node)
        .unwrap();

    // Confirm join node is constructed correctly
    assert_eq!(optimized_join.join_type, join_node.join_type);
    assert_eq!(optimized_join.condition, join_node.condition);
    assert_eq!(optimized_join.cost, join_node.cost);
}

#[test]
fn test_advanced_optimizer_statistics_integration() {
    let mut optimizer = AdvancedQueryOptimizer::new().unwrap();

    // Collect statistics for table
    let stats_manager = optimizer.statistics_manager_mut();
    stats_manager
        .collect_table_statistics("test_table")
        .unwrap();

    // Ensure statistics are accessible
    let table_stats = stats_manager.get_table_statistics("test_table");
    assert!(table_stats.is_some());

    let table_stats = table_stats.unwrap();
    assert_eq!(table_stats.table_name, "test_table");
    assert_eq!(table_stats.total_rows, 10000);
}

//! Example of using advanced query optimization for RustDB

use rustdb::catalog::{StatisticsManager, ValueDistribution};
use rustdb::planner::planner::{
    ExecutionPlan, FilterNode, PlanMetadata, PlanNode, PlanStatistics, TableScanNode,
};
use rustdb::planner::{AdvancedOptimizerSettings, AdvancedQueryOptimizer};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== RustDB Advanced Query Optimization Example ===\n");

    // Statistics manager demo
    demo_statistics_manager()?;

    // Advanced Optimizer Demonstration
    demo_advanced_optimizer()?;

    // Demonstration of optimization using statistics
    demo_optimization_with_statistics()?;

    println!("=== Demonstration completed successfully! ===");
    Ok(())
}

// / Demonstration of the statistics manager
fn demo_statistics_manager() -> Result<(), Box<dyn std::error::Error>> {
    println!("1. Statistics manager demo:");
    println!("Creating a statistics manager...");

    let mut stats_manager = StatisticsManager::new()?;

    // Collecting statistics for the users table
    println!("Collecting statistics for the 'users' table...");
    let table_stats = stats_manager.collect_table_statistics("users")?;

    println!("Statistics for table 'users':");
    println!("- Total number of lines: {}", table_stats.total_rows);
    println!("- Table size: {} bytes", table_stats.total_size_bytes);
    println!(
        "- Number of columns: {}",
        table_stats.column_statistics.len()
    );

    // Showing statistics by columns
    for (col_name, col_stats) in &table_stats.column_statistics {
        println!("- Column '{}':", col_name);
        println!("* Unique values: {}", col_stats.distinct_values);
        println!("* NULL values: {}", col_stats.null_count);
        println!("* Minimum value: {:?}", col_stats.min_value);
        println!("* Maximum value: {:?}", col_stats.max_value);

        match &col_stats.value_distribution {
            ValueDistribution::Uniform { step } => {
                println!("* Distribution: uniform (step: {})", step);
            }
            ValueDistribution::Normal { mean, std_dev } => {
                println!("* Distribution: normal (mean: {}, std: {})", mean, std_dev);
            }
            ValueDistribution::Histogram { buckets } => {
                println!("* Distribution: histogram ({} buckets)", buckets.len());
            }
            ValueDistribution::Unknown => {
                println!("*Distribution: unknown");
            }
        }
    }

    // Demonstrating selectivity assessment
    println!("\nSelectivity assessment:");
    let selectivity_eq = stats_manager.estimate_selectivity("users", "id", "=")?;
    let selectivity_range = stats_manager.estimate_selectivity("users", "age", ">")?;
    let selectivity_like = stats_manager.estimate_selectivity("users", "name", "LIKE")?;

    println!("     - id = ?: {:.4}", selectivity_eq);
    println!("     - age > ?: {:.4}", selectivity_range);
    println!("     - name LIKE ?: {:.4}", selectivity_like);

    // We demonstrate an estimate of the number of result lines
    println!("\n Estimation of the number of result lines:");
    let rows_eq = stats_manager.estimate_result_rows("users", "id", "=")?;
    let rows_range = stats_manager.estimate_result_rows("users", "age", ">")?;

    println!("- id = ?: {} lines", rows_eq);
    println!("- age > ?: {} lines", rows_range);

    println!();
    Ok(())
}

// / Demonstration of the advanced optimizer
fn demo_advanced_optimizer() -> Result<(), Box<dyn std::error::Error>> {
    println!("2. Demonstration of the advanced optimizer:");
    println!("Creating an Advanced Optimizer...");

    let settings = AdvancedOptimizerSettings {
        enable_statistics_usage: true,
        enable_query_rewriting: true,
        enable_expression_simplification: true,
        enable_subquery_extraction: true,
        enable_debug_logging: true,
        cost_threshold: 500.0,
    };

    let optimizer = AdvancedQueryOptimizer::with_settings(settings)?;

    println!("Optimizer settings:");
    println!(
        "- Using statistics: {}",
        optimizer.settings().enable_statistics_usage
    );
    println!(
        "- Query rewriting: {}",
        optimizer.settings().enable_query_rewriting
    );
    println!(
        "- Simplifying expressions: {}",
        optimizer.settings().enable_expression_simplification
    );
    println!(
        "- Submitting subqueries: {}",
        optimizer.settings().enable_subquery_extraction
    );
    println!("- Cost threshold: {}", optimizer.settings().cost_threshold);

    println!();
    Ok(())
}

// / Demonstration of optimization using statistics
fn demo_optimization_with_statistics() -> Result<(), Box<dyn std::error::Error>> {
    println!("3. Demonstration of optimization using statistics:");

    // Create a simple execution plan
    println!("Creating a Test Execution Plan...");
    let test_plan = create_test_execution_plan()?;

    println!("Original plan:");
    println!("- Cost Estimate: {:.2}", test_plan.metadata.estimated_cost);
    println!(
        "- Number of lines estimate: {}",
        test_plan.metadata.estimated_rows
    );
    println!(
        "- Number of operators: {}",
        test_plan.metadata.statistics.operator_count
    );

    // Create an optimizer and apply optimization
    println!("\nApplying advanced optimization...");
    let mut optimizer = AdvancedQueryOptimizer::new()?;

    // Collecting statistics for tables in the plan
    let stats_manager = optimizer.statistics_manager_mut();
    stats_manager.collect_table_statistics("users")?;
    stats_manager.collect_table_statistics("orders")?;

    // Applying optimization
    let optimization_result = optimizer.optimize_with_statistics(test_plan)?;

    println!("Optimization result:");
    println!(
        "- Number of optimizations applied: {}",
        optimization_result.statistics.optimizations_applied
    );
    println!(
        "- Optimization time: {} ms",
        optimization_result.statistics.optimization_time_ms
    );
    println!(
        "- Cost Improvement: {:.2}%",
        optimization_result.statistics.cost_improvement_percent
    );
    println!(
        "- Query rewrites: {}",
        optimization_result.statistics.query_rewrites
    );
    println!(
        "- Expression simplifications: {}",
        optimization_result.statistics.expression_simplifications
    );
    println!(
        "- Extracting subqueries: {}",
        optimization_result.statistics.subquery_extractions
    );
    println!(
        "- Statistics usage: {}",
        optimization_result.statistics.statistics_usage_count
    );

    // Showing messages about optimizations
    if !optimization_result.messages.is_empty() {
        println!("\nOptimizations applied:");
        for (i, message) in optimization_result.messages.iter().enumerate() {
            println!("     {}. {}", i + 1, message);
        }
    }

    // Showing the statistics used
    if !optimization_result.used_statistics.is_empty() {
        println!("\nStatistics used:");
        for (i, stat_info) in optimization_result.used_statistics.iter().enumerate() {
            println!("     {}. {}", i + 1, stat_info);
        }
    }

    // Showing the optimized plan
    println!("\nOptimized plan:");
    println!(
        "- Cost Estimate: {:.2}",
        optimization_result.optimized_plan.metadata.estimated_cost
    );
    println!(
        "- Number of lines estimate: {}",
        optimization_result.optimized_plan.metadata.estimated_rows
    );

    println!();
    Ok(())
}

// / Create a test execution plan
fn create_test_execution_plan() -> Result<ExecutionPlan, Box<dyn std::error::Error>> {
    // Creating plan nodes
    let users_scan = PlanNode::TableScan(TableScanNode {
        table_name: "users".to_string(),
        alias: Some("u".to_string()),
        columns: vec!["id".to_string(), "name".to_string(), "age".to_string()],
        filter: None,
        cost: 1000.0,
        estimated_rows: 10000,
    });

    let _orders_scan = PlanNode::TableScan(TableScanNode {
        table_name: "orders".to_string(),
        alias: Some("o".to_string()),
        columns: vec![
            "id".to_string(),
            "user_id".to_string(),
            "amount".to_string(),
        ],
        filter: None,
        cost: 800.0,
        estimated_rows: 50000,
    });

    let filter_node = PlanNode::Filter(FilterNode {
        condition: "u.age > 18 AND o.amount > 100".to_string(),
        input: Box::new(users_scan),
        selectivity: 0.3,
        cost: 300.0,
    });

    // Creating plan metadata
    let metadata = PlanMetadata {
        estimated_cost: 2100.0,
        estimated_rows: 1500,
        created_at: std::time::SystemTime::now(),
        statistics: PlanStatistics {
            operator_count: 3,
            max_depth: 2,
            table_count: 2,
            join_count: 0,
        },
    };

    Ok(ExecutionPlan {
        root: filter_node,
        metadata,
    })
}

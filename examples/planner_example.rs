//! Query Planner Example

use rustdb::common::Result;
use rustdb::parser::SqlParser;
use rustdb::planner::{QueryOptimizer, QueryPlanner};

fn main() -> Result<()> {
    println!("=== Example of using the query planner ===\n");

    // Creating a scheduler and optimizer
    let mut planner = QueryPlanner::new()?;
    let mut optimizer = QueryOptimizer::new()?;

    // Example 1: Simple SELECT query
    println!("1. Creating a plan for a simple SELECT query:");
    let mut parser = SqlParser::new("SELECT * FROM users")?;
    let statement = parser.parse()?;
    let plan = planner.create_plan(&statement)?;

    println!("Plan created:");
    println!("- Cost Estimate: {:.2}", plan.metadata.estimated_cost);
    println!("- String evaluation: {}", plan.metadata.estimated_rows);
    println!(
        "- Number of operators: {}",
        plan.metadata.statistics.operator_count
    );
    println!(
        "- Number of tables: {}",
        plan.metadata.statistics.table_count
    );
    println!();

    // Example 2: SELECT with WHERE condition
    println!("2. Create a plan for SELECT with WHERE:");
    let mut parser = SqlParser::new("SELECT name, age FROM users WHERE age > 18")?;
    let statement = parser.parse()?;
    let plan = planner.create_plan(&statement)?;

    println!("Plan created:");
    println!("- Cost Estimate: {:.2}", plan.metadata.estimated_cost);
    println!("- String evaluation: {}", plan.metadata.estimated_rows);
    println!(
        "- Number of operators: {}",
        plan.metadata.statistics.operator_count
    );
    println!();

    // Example 3: INSERT request
    println!("3. Create a plan for INSERT:");
    let mut parser = SqlParser::new("INSERT INTO users (name, age) VALUES ('John', 25)")?;
    let statement = parser.parse()?;
    let plan = planner.create_plan(&statement)?;

    println!("Plan created:");
    println!("- Cost Estimate: {:.2}", plan.metadata.estimated_cost);
    println!("- String evaluation: {}", plan.metadata.estimated_rows);
    println!();

    // Example 4: UPDATE request
    println!("4. Create a plan for UPDATE:");
    let mut parser = SqlParser::new("UPDATE users SET age = 26 WHERE name = 'John'")?;
    let statement = parser.parse()?;
    let plan = planner.create_plan(&statement)?;

    println!("Plan created:");
    println!("- Cost Estimate: {:.2}", plan.metadata.estimated_cost);
    println!("- String evaluation: {}", plan.metadata.estimated_rows);
    println!();

    // Example 5: DELETE request
    println!("5. Create a plan for DELETE:");
    let mut parser = SqlParser::new("DELETE FROM users WHERE age < 18")?;
    let statement = parser.parse()?;
    let plan = planner.create_plan(&statement)?;

    println!("Plan created:");
    println!("- Cost Estimate: {:.2}", plan.metadata.estimated_cost);
    println!("- String evaluation: {}", plan.metadata.estimated_rows);
    println!();

    // Example 6: Plan Optimization
    println!("6. Plan optimization:");
    let mut parser = SqlParser::new(
        "SELECT u.name, p.title FROM users u JOIN posts p ON u.id = p.user_id WHERE u.age > 18",
    )?;
    let statement = parser.parse()?;
    let original_plan = planner.create_plan(&statement)?;

    println!("Original plan:");
    println!(
        "- Cost Estimate: {:.2}",
        original_plan.metadata.estimated_cost
    );
    println!(
        "- String evaluation: {}",
        original_plan.metadata.estimated_rows
    );
    println!(
        "- Number of JOINs: {}",
        original_plan.metadata.statistics.join_count
    );

    let optimization_result = optimizer.optimize(original_plan)?;

    println!("Optimized plan:");
    println!(
        "- Cost Estimate: {:.2}",
        optimization_result.optimized_plan.metadata.estimated_cost
    );
    println!(
        "- String evaluation: {}",
        optimization_result.optimized_plan.metadata.estimated_rows
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
        "- Optimizations applied: {}",
        optimization_result.statistics.optimizations_applied
    );

    if !optimization_result.messages.is_empty() {
        println!("Optimization messages:");
        for msg in &optimization_result.messages {
            println!("   - {}", msg);
        }
    }
    println!();

    // Example 7: Scheduler settings
    println!("7. Scheduler settings:");
    let settings = planner.settings();
    println!("- Plan caching: {}", settings.enable_plan_cache);
    println!("- Maximum cache size: {}", settings.max_cache_size);
    println!("- Optimization: {}", settings.enable_optimization);
    println!(
        "- Maximum recursion depth: {}",
        settings.max_recursion_depth
    );
    println!();

    // Example 8: Cache Statistics
    println!("8. Cache statistics:");
    let cache_stats = planner.cache_stats();
    println!("- Current size: {}", cache_stats.size);
    println!("- Maximum size: {}", cache_stats.max_size);
    println!();

    // Example 9: Optimizer settings
    println!("9. Optimizer settings:");
    let opt_settings = optimizer.settings();
    println!(
        "- JOIN permutation: {}",
        opt_settings.enable_join_reordering
    );
    println!("- Index selection: {}", opt_settings.enable_index_selection);
    println!(
        "- Simplifying expressions: {}",
        opt_settings.enable_expression_simplification
    );
    println!(
        "- Predicate popping: {}",
        opt_settings.enable_predicate_pushdown
    );
    println!(
        "- Maximum iterations: {}",
        opt_settings.max_optimization_iterations
    );
    println!("- Cost threshold: {:.2}", opt_settings.cost_threshold);
    println!();

    // Example 10: Optimizer Statistics
    println!("10. Optimizer statistics:");
    let opt_stats = optimizer.statistics();
    println!(
        "- Optimizations applied: {}",
        opt_stats.optimizations_applied
    );
    println!("- Optimization time: {} ms", opt_stats.optimization_time_ms);
    println!(
        "- Cost Improvement: {:.2}%",
        opt_stats.cost_improvement_percent
    );
    println!("- JOIN permutations: {}", opt_stats.join_reorders);
    println!("- Indexes applied: {}", opt_stats.indexes_applied);
    println!(
        "- Expression simplifications: {}",
        opt_stats.expression_simplifications
    );
    println!();

    println!("=== Example complete ===");
    Ok(())
}

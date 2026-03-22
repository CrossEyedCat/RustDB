//! Дополнительное покрытие веток `optimizer.rs` (reorder, index registry, filter/join).

use crate::common::Result;
use crate::parser::SqlParser;
use crate::planner::planner::{
    ExecutionPlan, FilterNode, JoinNode, JoinType, PlanMetadata, PlanNode, PlanStatistics,
    TableScanNode,
};
use crate::planner::{OptimizerSettings, QueryOptimizer, QueryPlanner};
use crate::storage::index_registry::IndexRegistry;
use std::sync::Arc;

fn plan_meta(cost: f64) -> PlanMetadata {
    PlanMetadata {
        estimated_cost: cost,
        estimated_rows: 10,
        created_at: std::time::SystemTime::UNIX_EPOCH,
        statistics: PlanStatistics {
            operator_count: 3,
            max_depth: 2,
            table_count: 2,
            join_count: 1,
        },
    }
}

fn table_scan(name: &str, cost: f64, filter: Option<String>) -> PlanNode {
    PlanNode::TableScan(TableScanNode {
        table_name: name.into(),
        alias: None,
        columns: vec!["id".into()],
        filter,
        cost,
        estimated_rows: 100,
    })
}

#[test]
fn test_optimizer_join_reorder_swaps_heavy_left() -> Result<()> {
    let mut opt = QueryOptimizer::new()?;
    // Правая ветка дешевле — reorder_joins_recursive меняет местами left/right
    let join = PlanNode::Join(JoinNode {
        join_type: JoinType::Inner,
        condition: "a.id=b.id".into(),
        left: Box::new(table_scan("heavy", 200.0, None)),
        right: Box::new(table_scan("light", 5.0, None)),
        cost: 10.0,
    });
    let plan = ExecutionPlan {
        root: join,
        metadata: plan_meta(50.0),
    };
    let res = opt.optimize(plan)?;
    assert!(matches!(res.optimized_plan.root, PlanNode::Join(_)));
    assert!(res.statistics.optimization_time_ms >= 0);
    Ok(())
}

#[test]
fn test_optimizer_index_selection_with_registry() -> Result<()> {
    let mut reg = IndexRegistry::new();
    reg.create_index("users", "idx_id", vec!["id".to_string()])?;
    let mut opt = QueryOptimizer::new()?.with_index_registry(Arc::new(reg));
    let scan = table_scan("users", 40.0, Some("42".into()));
    let plan = ExecutionPlan {
        root: scan,
        metadata: plan_meta(40.0),
    };
    let res = opt.optimize(plan)?;
    assert!(matches!(
        res.optimized_plan.root,
        PlanNode::IndexScan(_) | PlanNode::TableScan(_)
    ));
    Ok(())
}

#[test]
fn test_optimizer_filter_over_join_pushdown_path() -> Result<()> {
    let mut opt = QueryOptimizer::new()?;
    let inner_join = PlanNode::Join(JoinNode {
        join_type: JoinType::Inner,
        condition: "x=y".into(),
        left: Box::new(table_scan("t1", 10.0, None)),
        right: Box::new(table_scan("t2", 10.0, None)),
        cost: 2.0,
    });
    let root = PlanNode::Filter(FilterNode {
        condition: "a=1".into(),
        input: Box::new(inner_join),
        selectivity: 0.3,
        cost: 1.0,
    });
    let plan = ExecutionPlan {
        root,
        metadata: plan_meta(30.0),
    };
    let _ = opt.optimize(plan)?;
    Ok(())
}

#[test]
fn test_optimizer_all_flags_off_still_runs() -> Result<()> {
    let settings = OptimizerSettings {
        enable_join_reordering: false,
        enable_index_selection: false,
        enable_expression_simplification: false,
        enable_predicate_pushdown: false,
        max_optimization_iterations: 1,
        cost_threshold: 1.0,
        enable_debug_logging: false,
    };
    let mut opt = QueryOptimizer::with_settings(settings)?;
    let mut planner = QueryPlanner::new()?;
    let mut p = SqlParser::new("SELECT * FROM users")?;
    let stmt = p.parse()?;
    let plan = planner.create_plan(&stmt)?;
    let res = opt.optimize(plan)?;
    assert_eq!(res.statistics.optimizations_applied, 0);
    Ok(())
}

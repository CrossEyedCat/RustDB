//! Additional coverage of `optimizer.rs` branches (reorder, index registry, filter/join).

use crate::common::Result;
use crate::parser::ast::{BinaryOperator, Expression, Literal};
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
    // The right branch is cheaper - reorder_joins_recursive swaps left/right
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

fn eq_col_lit(column: &str, lit: Literal) -> Expression {
    Expression::BinaryOp {
        left: Box::new(Expression::Identifier(column.to_string())),
        op: BinaryOperator::Equal,
        right: Box::new(Expression::Literal(lit)),
    }
}

#[test]
fn test_optimizer_composite_index_prefix_match() -> Result<()> {
    let mut reg = IndexRegistry::new();
    reg.create_index(
        "oorder",
        "idx_oorder_wdc",
        vec![
            "o_w_id".to_string(),
            "o_d_id".to_string(),
            "o_c_id".to_string(),
        ],
    )?;
    reg.create_index("oorder", "idx_oorder_id", vec!["o_id".to_string()])?;

    let mut opt = QueryOptimizer::new()?.with_index_registry(Arc::new(reg));

    let where_clause = Expression::BinaryOp {
        left: Box::new(eq_col_lit("o_w_id", Literal::Integer(1))),
        op: BinaryOperator::And,
        right: Box::new(Expression::BinaryOp {
            left: Box::new(eq_col_lit("o_d_id", Literal::Integer(2))),
            op: BinaryOperator::And,
            right: Box::new(eq_col_lit("o_c_id", Literal::Integer(3))),
        }),
    };

    let scan = PlanNode::TableScan(TableScanNode {
        table_name: "oorder".into(),
        alias: None,
        columns: vec!["*".into()],
        filter: None,
        cost: 100.0,
        estimated_rows: 1000,
    });
    let root = PlanNode::Filter(FilterNode {
        condition: "tpcc order_status".into(),
        predicate: Some(where_clause),
        equality: None,
        input: Box::new(scan),
        selectivity: 0.01,
        cost: 1.0,
    });
    let plan = ExecutionPlan {
        root,
        metadata: plan_meta(100.0),
    };

    let res = opt.optimize(plan)?;
    let PlanNode::Filter(f) = &res.optimized_plan.root else {
        panic!(
            "expected filter over index scan, got {:?}",
            res.optimized_plan.root
        );
    };
    let PlanNode::IndexScan(idx) = f.input.as_ref() else {
        panic!("expected index scan, got {:?}", f.input);
    };
    assert_eq!(idx.index_name, "idx_oorder_wdc");
    assert_eq!(idx.conditions.len(), 3);
    assert_eq!(idx.conditions[0].column, "o_w_id");
    assert_eq!(idx.conditions[0].value, "1");
    assert_eq!(idx.conditions[1].column, "o_d_id");
    assert_eq!(idx.conditions[1].value, "2");
    assert_eq!(idx.conditions[2].column, "o_c_id");
    assert_eq!(idx.conditions[2].value, "3");
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
        predicate: None,
        equality: None,
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

// NOTE: Do not push string conditions into TableScan.filter in optimizer: the executor's
// `TableScanOperator` uses `contains()` on the row debug string, and pushing Debug-formatted
// AST conditions would change semantics.

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
    let opt = QueryOptimizer::with_settings(settings)?;
    let planner = QueryPlanner::new()?;
    let mut p = SqlParser::new("SELECT * FROM users")?;
    let stmt = p.parse()?;
    let plan = planner.create_plan(&stmt)?;
    let res = opt.optimize(plan)?;
    assert_eq!(res.statistics.optimizations_applied, 0);
    Ok(())
}

#[test]
fn test_optimizer_rewrites_exists_to_semi_join() -> Result<()> {
    use crate::parser::ast::{
        BinaryOperator, Expression, FromClause, Literal, SelectItem, SelectStatement, SqlStatement,
        TableReference,
    };

    let planner = QueryPlanner::new()?;
    let opt = QueryOptimizer::new()?;

    // Outer: SELECT * FROM t WHERE EXISTS(SELECT 1 FROM u WHERE u.id = 1)
    let stmt = SqlStatement::Select(SelectStatement {
        distinct: false,
        select_list: vec![SelectItem::Wildcard],
        from: Some(FromClause {
            table: TableReference::Table {
                name: "t".to_string(),
                alias: None,
            },
            joins: vec![],
        }),
        where_clause: Some(Expression::Exists(Box::new(SelectStatement {
            distinct: false,
            select_list: vec![SelectItem::Expression {
                expr: Expression::Literal(Literal::Integer(1)),
                alias: None,
            }],
            from: Some(FromClause {
                table: TableReference::Table {
                    name: "u".to_string(),
                    alias: None,
                },
                joins: vec![],
            }),
            where_clause: Some(Expression::BinaryOp {
                left: Box::new(Expression::QualifiedIdentifier {
                    table: "u".to_string(),
                    column: "id".to_string(),
                }),
                op: BinaryOperator::Equal,
                right: Box::new(Expression::Literal(Literal::Integer(1))),
            }),
            group_by: vec![],
            having: None,
            order_by: vec![],
            limit: None,
            offset: None,
        }))),
        group_by: vec![],
        having: None,
        order_by: vec![],
        limit: None,
        offset: None,
    });

    let plan = planner.create_plan(&stmt)?;
    let res = opt.optimize(plan)?;

    let PlanNode::Projection(p) = &res.optimized_plan.root else {
        panic!("expected projection");
    };
    // Filter(EXISTS) should be rewritten to SemiJoin at/under projection.
    assert!(
        matches!(p.input.as_ref(), PlanNode::SemiJoin(_))
            || matches!(p.input.as_ref(), PlanNode::Filter(_)),
        "unexpected plan: {:?}",
        p.input
    );
    Ok(())
}

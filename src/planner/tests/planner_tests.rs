//! Query planner tests

use crate::common::Result;
use crate::parser::{SqlParser, SqlStatement};
use crate::planner::planner::InsertNode;
use crate::planner::{ExecutionPlan, PlanNode, QueryPlanner};

#[test]
fn test_create_simple_select_plan() -> Result<()> {
    let planner = QueryPlanner::new()?;
    let mut parser = SqlParser::new("SELECT * FROM users")?;

    let statement = parser.parse()?;
    let plan = planner.create_plan(&statement)?;

    assert!(matches!(plan.root, PlanNode::Projection(_)));

    Ok(())
}

#[test]
fn test_create_select_with_where_plan() -> Result<()> {
    let planner = QueryPlanner::new()?;
    let mut parser = SqlParser::new("SELECT name, age FROM users WHERE age > 18")?;

    let statement = parser.parse()?;
    let plan = planner.create_plan(&statement)?;

    // Ensure a plan was produced successfully
    assert!(matches!(plan.root, PlanNode::Projection(_)));

    Ok(())
}

#[test]
fn test_create_insert_plan() -> Result<()> {
    let planner = QueryPlanner::new()?;
    let mut parser = SqlParser::new("INSERT INTO users (name, age) VALUES ('John', 25)")?;

    let statement = parser.parse()?;
    let plan = planner.create_plan(&statement)?;

    assert!(matches!(plan.root, PlanNode::Insert(_)));

    Ok(())
}

#[test]
fn test_insert_select_plan_has_subplan() -> Result<()> {
    let planner = QueryPlanner::new()?;
    let mut parser = SqlParser::new("INSERT INTO t (a) SELECT id FROM users WHERE id > 0")?;

    let statement = parser.parse()?;
    let plan = planner.create_plan(&statement)?;

    let PlanNode::Insert(InsertNode {
        insert_subplan: Some(sub),
        values,
        ..
    }) = &plan.root
    else {
        panic!("expected Insert with subplan");
    };
    assert!(values.is_empty());
    assert!(matches!(**sub, PlanNode::Projection(_)));

    Ok(())
}

#[test]
fn test_create_select_plan_columns_and_groupby_aggregates() -> Result<()> {
    use crate::parser::ast::{
        Expression, FromClause, SelectItem, SelectStatement, SqlStatement, TableReference,
    };

    let planner = QueryPlanner::new()?;
    // Parser `parse_select` is minimal (no functions/GROUP BY); build AST for planner coverage.
    let statement = SqlStatement::Select(SelectStatement {
        distinct: false,
        select_list: vec![
            SelectItem::Expression {
                expr: Expression::Identifier("dept".to_string()),
                alias: None,
            },
            SelectItem::Expression {
                expr: Expression::Function {
                    name: "COUNT".to_string(),
                    args: vec![Expression::Identifier("id".to_string())],
                },
                alias: None,
            },
        ],
        from: Some(FromClause {
            table: TableReference::Table {
                name: "employees".to_string(),
                alias: None,
            },
            joins: vec![],
        }),
        where_clause: None,
        group_by: vec![Expression::Identifier("dept".to_string())],
        having: None,
        order_by: vec![],
        limit: None,
        offset: None,
    });

    let plan = planner.create_plan(&statement)?;

    let PlanNode::Projection(proj) = &plan.root else {
        panic!("expected projection root");
    };
    let PlanNode::GroupBy(gb) = proj.input.as_ref() else {
        panic!("expected group by");
    };
    assert_eq!(gb.group_columns.len(), 1);
    assert_eq!(gb.aggregates.len(), 1);
    assert_eq!(gb.aggregates[0].name, "COUNT");

    let PlanNode::TableScan(ts) = gb.input.as_ref() else {
        panic!("expected table scan under group by");
    };
    assert_eq!(ts.columns, vec!["dept", "COUNT(id)"]);

    Ok(())
}

#[test]
fn test_create_update_plan() -> Result<()> {
    let planner = QueryPlanner::new()?;
    let mut parser = SqlParser::new("UPDATE users SET age = 26")?;

    let statement = parser.parse()?;
    let plan = planner.create_plan(&statement)?;

    assert!(matches!(plan.root, PlanNode::Update(_)));

    Ok(())
}

#[test]
fn test_create_delete_plan() -> Result<()> {
    let planner = QueryPlanner::new()?;
    let mut parser = SqlParser::new("DELETE FROM users")?;

    let statement = parser.parse()?;
    let plan = planner.create_plan(&statement)?;

    assert!(matches!(plan.root, PlanNode::Delete(_)));

    Ok(())
}

#[test]
fn test_plan_metadata() -> Result<()> {
    let planner = QueryPlanner::new()?;
    let mut parser = SqlParser::new("SELECT * FROM users")?;

    let statement = parser.parse()?;
    let plan = planner.create_plan(&statement)?;

    // Confirm plan metadata is populated
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

#[test]
fn test_planner_rejects_set_operations_for_now() -> Result<()> {
    let planner = QueryPlanner::new()?;
    let mut parser = SqlParser::new("SELECT 1 UNION ALL SELECT 2")?;
    let statement = parser.parse()?;

    let plan = planner.create_plan(&statement)?;
    assert!(matches!(plan.root, PlanNode::SetOp(_)));
    Ok(())
}

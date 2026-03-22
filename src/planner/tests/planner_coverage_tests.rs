//! Дополнительные тесты планировщика и оптимизатора для покрытия веток.

use crate::common::Result;
use crate::parser::SqlParser;
use crate::planner::planner::{PlanMetadata, PlanStatistics, TableScanNode};
use crate::planner::{ExecutionPlan, PlanNode, QueryOptimizer, QueryPlanner};

#[test]
fn test_planner_multiple_sql_variants() -> Result<()> {
    let mut planner = QueryPlanner::new()?;
    // UPDATE/DELETE: WHERE парсится как `parse_simple_expression()` — только один литерал/идентификатор,
    // не полноценное сравнение; для Boolean нужен `true`/`false` (см. parser.rs).
    // SELECT в упрощённом парсере не читает WHERE — условия не добавляем.
    let sqls = [
        "SELECT * FROM users",
        "SELECT name FROM users",
        "INSERT INTO users (name, age) VALUES ('x', 1)",
        "UPDATE users SET name = 'y' WHERE true",
        "DELETE FROM users WHERE false",
    ];
    for sql in sqls {
        let mut p = SqlParser::new(sql)?;
        let stmt = p.parse()?;
        let _plan = planner.create_plan(&stmt)?;
    }
    Ok(())
}

#[test]
fn test_optimizer_multiple_plans() -> Result<()> {
    let mut planner = QueryPlanner::new()?;
    let mut opt = QueryOptimizer::new()?;
    let sqls = ["SELECT * FROM users", "SELECT * FROM users WHERE age > 0"];
    for sql in sqls {
        let mut p = SqlParser::new(sql)?;
        let stmt = p.parse()?;
        let plan = planner.create_plan(&stmt)?;
        let _ = opt.optimize(plan)?;
    }
    Ok(())
}

#[test]
fn test_execution_plan_clone() {
    let root = PlanNode::TableScan(TableScanNode {
        table_name: "x".into(),
        alias: None,
        columns: vec!["*".into()],
        filter: None,
        cost: 1.0,
        estimated_rows: 1,
    });
    let meta = PlanMetadata {
        estimated_cost: 1.0,
        estimated_rows: 1,
        created_at: std::time::SystemTime::now(),
        statistics: PlanStatistics {
            operator_count: 1,
            max_depth: 1,
            table_count: 1,
            join_count: 0,
        },
    };
    let p1 = ExecutionPlan {
        root: root.clone(),
        metadata: meta.clone(),
    };
    let p2 = p1.clone();
    assert_eq!(p1, p2);
}

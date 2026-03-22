//! Покрытие `QueryExecutor::execute` для всех поддерживаемых узлов плана.

use super::common;
use crate::executor::{QueryExecutor, QueryExecutorConfig};
use crate::planner::planner::{
    AggregateFunction, ExecutionPlan, FilterNode, GroupByNode, IndexCondition, IndexScanNode,
    InsertNode, JoinNode, JoinType, LimitNode, OffsetNode, PlanMetadata, PlanNode, PlanStatistics,
    ProjectionColumn, ProjectionNode, SortColumn, SortDirection, SortNode, TableScanNode,
};
use std::sync::Arc;
use std::time::SystemTime;

fn plan_meta() -> PlanMetadata {
    PlanMetadata {
        estimated_cost: 1.0,
        estimated_rows: 10,
        created_at: SystemTime::now(),
        statistics: PlanStatistics {
            operator_count: 3,
            max_depth: 3,
            table_count: 1,
            join_count: 0,
        },
    }
}

fn table_scan_node() -> TableScanNode {
    TableScanNode {
        table_name: "t".to_string(),
        alias: None,
        columns: vec!["id".to_string(), "data".to_string()],
        filter: None,
        cost: 1.0,
        estimated_rows: 100,
    }
}

#[test]
fn test_executor_table_scan_only() -> crate::common::Result<()> {
    let (_tmp, pm) = common::create_test_page_manager();
    let factory = Arc::new(crate::executor::operators::ScanOperatorFactory::new(pm));
    let ex = QueryExecutor::new(factory)?;
    let plan = ExecutionPlan {
        root: PlanNode::Limit(LimitNode {
            limit: 5,
            input: Box::new(PlanNode::TableScan(table_scan_node())),
            cost: 1.0,
        }),
        metadata: plan_meta(),
    };
    let rows = ex.execute(&plan)?;
    assert!(!rows.is_empty());
    Ok(())
}

#[test]
fn test_executor_limit_table_scan() -> crate::common::Result<()> {
    let (_tmp, pm) = common::create_test_page_manager();
    let factory = Arc::new(crate::executor::operators::ScanOperatorFactory::new(pm));
    let ex = QueryExecutor::new(factory)?;
    let plan = ExecutionPlan {
        root: PlanNode::Limit(LimitNode {
            limit: 3,
            input: Box::new(PlanNode::TableScan(table_scan_node())),
            cost: 1.0,
        }),
        metadata: plan_meta(),
    };
    let rows = ex.execute(&plan)?;
    assert!(rows.len() <= 3);
    Ok(())
}

#[test]
fn test_executor_offset_limit() -> crate::common::Result<()> {
    let (_tmp, pm) = common::create_test_page_manager();
    let factory = Arc::new(crate::executor::operators::ScanOperatorFactory::new(pm));
    let ex = QueryExecutor::new(factory)?;
    let inner = PlanNode::Limit(LimitNode {
        limit: 5,
        input: Box::new(PlanNode::TableScan(table_scan_node())),
        cost: 1.0,
    });
    let plan = ExecutionPlan {
        root: PlanNode::Offset(OffsetNode {
            offset: 1,
            input: Box::new(inner),
            cost: 1.0,
        }),
        metadata: plan_meta(),
    };
    let _ = ex.execute(&plan)?;
    Ok(())
}

#[test]
fn test_executor_filter() -> crate::common::Result<()> {
    let (_tmp, pm) = common::create_test_page_manager();
    let factory = Arc::new(crate::executor::operators::ScanOperatorFactory::new(pm));
    let ex = QueryExecutor::new(factory)?;
    let plan = ExecutionPlan {
        root: PlanNode::Filter(FilterNode {
            condition: "id".to_string(),
            input: Box::new(PlanNode::Limit(LimitNode {
                limit: 8,
                input: Box::new(PlanNode::TableScan(table_scan_node())),
                cost: 1.0,
            })),
            selectivity: 0.5,
            cost: 1.0,
        }),
        metadata: plan_meta(),
    };
    let _ = ex.execute(&plan)?;
    Ok(())
}

#[test]
fn test_executor_projection() -> crate::common::Result<()> {
    let (_tmp, pm) = common::create_test_page_manager();
    let factory = Arc::new(crate::executor::operators::ScanOperatorFactory::new(pm));
    let ex = QueryExecutor::new(factory)?;
    let plan = ExecutionPlan {
        root: PlanNode::Projection(ProjectionNode {
            columns: vec![ProjectionColumn {
                name: "id".to_string(),
                expression: None,
                alias: None,
            }],
            input: Box::new(PlanNode::Limit(LimitNode {
                limit: 8,
                input: Box::new(PlanNode::TableScan(table_scan_node())),
                cost: 1.0,
            })),
            cost: 1.0,
        }),
        metadata: plan_meta(),
    };
    let _ = ex.execute(&plan)?;
    Ok(())
}

#[test]
fn test_executor_sort() -> crate::common::Result<()> {
    let (_tmp, pm) = common::create_test_page_manager();
    let factory = Arc::new(crate::executor::operators::ScanOperatorFactory::new(pm));
    let ex = QueryExecutor::new(factory)?;
    let plan = ExecutionPlan {
        root: PlanNode::Sort(SortNode {
            sort_columns: vec![SortColumn {
                column: "id".to_string(),
                direction: SortDirection::Desc,
            }],
            input: Box::new(PlanNode::Limit(LimitNode {
                limit: 5,
                input: Box::new(PlanNode::TableScan(table_scan_node())),
                cost: 1.0,
            })),
            cost: 1.0,
        }),
        metadata: plan_meta(),
    };
    let _ = ex.execute(&plan)?;
    Ok(())
}

#[test]
fn test_executor_group_by() -> crate::common::Result<()> {
    let (_tmp, pm) = common::create_test_page_manager();
    let factory = Arc::new(crate::executor::operators::ScanOperatorFactory::new(pm));
    let ex = QueryExecutor::new(factory)?;
    let plan = ExecutionPlan {
        root: PlanNode::GroupBy(GroupByNode {
            group_columns: vec!["id".to_string()],
            aggregates: vec![AggregateFunction {
                name: "count".to_string(),
                argument: "*".to_string(),
                alias: Some("c".to_string()),
            }],
            input: Box::new(PlanNode::Limit(LimitNode {
                limit: 4,
                input: Box::new(PlanNode::TableScan(table_scan_node())),
                cost: 1.0,
            })),
            cost: 1.0,
        }),
        metadata: plan_meta(),
    };
    let _ = ex.execute(&plan)?;
    Ok(())
}

#[test]
fn test_executor_join_small() -> crate::common::Result<()> {
    let (_tmp, pm) = common::create_test_page_manager();
    let factory = Arc::new(crate::executor::operators::ScanOperatorFactory::new(pm));
    let ex = QueryExecutor::new(factory)?;
    let left = PlanNode::Limit(LimitNode {
        limit: 2,
        input: Box::new(PlanNode::TableScan(TableScanNode {
            table_name: "a".to_string(),
            alias: None,
            columns: vec!["id".to_string(), "data".to_string()],
            filter: None,
            cost: 1.0,
            estimated_rows: 10,
        })),
        cost: 1.0,
    });
    let right = PlanNode::Limit(LimitNode {
        limit: 2,
        input: Box::new(PlanNode::TableScan(TableScanNode {
            table_name: "b".to_string(),
            alias: None,
            columns: vec!["id".to_string(), "data".to_string()],
            filter: None,
            cost: 1.0,
            estimated_rows: 10,
        })),
        cost: 1.0,
    });
    let plan = ExecutionPlan {
        root: PlanNode::Join(JoinNode {
            join_type: JoinType::Inner,
            condition: "id=id".to_string(),
            left: Box::new(left),
            right: Box::new(right),
            cost: 1.0,
        }),
        metadata: plan_meta(),
    };
    let _ = ex.execute(&plan)?;
    Ok(())
}

#[test]
fn test_executor_join_types() -> crate::common::Result<()> {
    let (_tmp, pm) = common::create_test_page_manager();
    let factory = Arc::new(crate::executor::operators::ScanOperatorFactory::new(pm));
    let ex = QueryExecutor::with_config(
        factory,
        QueryExecutorConfig {
            enable_parallel_execution: false,
            num_worker_threads: 1,
        },
    )?;
    let make_join = |jt: JoinType| {
        ExecutionPlan {
            root: PlanNode::Join(JoinNode {
                join_type: jt,
                condition: "id=id".to_string(),
                left: Box::new(PlanNode::Limit(LimitNode {
                    limit: 1,
                    input: Box::new(PlanNode::TableScan(table_scan_node())),
                    cost: 1.0,
                })),
                right: Box::new(PlanNode::Limit(LimitNode {
                    limit: 1,
                    input: Box::new(PlanNode::TableScan(table_scan_node())),
                    cost: 1.0,
                })),
                cost: 1.0,
            }),
            metadata: plan_meta(),
        }
    };
    for jt in [
        JoinType::Inner,
        JoinType::Left,
        JoinType::Right,
        JoinType::Full,
        JoinType::Cross,
    ] {
        let _ = ex.execute(&make_join(jt))?;
    }
    Ok(())
}

#[test]
fn test_executor_index_scan_with_index() -> crate::common::Result<()> {
    let (_tmp, pm) = common::create_test_page_manager();
    let mut factory = crate::executor::operators::ScanOperatorFactory::new(pm.clone());
    let idx = Arc::new(std::sync::Mutex::new(
        crate::storage::index::BPlusTree::new(4),
    ));
    factory.add_index("t", "ix_id", idx);
    let factory = Arc::new(factory);
    let ex = QueryExecutor::new(factory)?;
    let plan = ExecutionPlan {
        root: PlanNode::IndexScan(IndexScanNode {
            table_name: "t".to_string(),
            index_name: "ix_id".to_string(),
            conditions: vec![IndexCondition {
                column: "id".to_string(),
                operator: "=".to_string(),
                value: "1".to_string(),
            }],
            cost: 1.0,
            estimated_rows: 1,
        }),
        metadata: plan_meta(),
    };
    let _ = ex.execute(&plan)?;
    Ok(())
}

#[test]
fn test_executor_unsupported_plan() {
    let (_tmp, pm) = common::create_test_page_manager();
    let factory = Arc::new(crate::executor::operators::ScanOperatorFactory::new(pm));
    let ex = QueryExecutor::new(factory).unwrap();
    let plan = ExecutionPlan {
        root: PlanNode::Insert(InsertNode {
            table_name: "x".to_string(),
            columns: vec![],
            values: vec![],
            cost: 1.0,
        }),
        metadata: plan_meta(),
    };
    assert!(ex.execute(&plan).is_err());
}

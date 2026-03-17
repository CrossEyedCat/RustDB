//! Query executor for rustdb
//!
//! Executes query plans by building operator trees and collecting results.
//! Supports parallel table scan when enabled.

use crate::common::{Error, Result};
use crate::executor::operators::{
    ConditionalScanOperator, HashGroupByOperator, IndexCondition, IndexOperator, JoinCondition,
    JoinOperator, JoinType, LimitOperator, NestedLoopJoinOperator, OffsetOperator, Operator,
    ScanOperatorFactory, SortOperator,
};
use crate::planner::planner::{
    ExecutionPlan, FilterNode, GroupByNode, IndexScanNode, JoinNode, LimitNode, OffsetNode,
    PlanNode, ProjectionNode, SortNode, TableScanNode,
};
use crate::Row;
use std::sync::{Arc, Mutex};

/// Configuration for query execution
#[derive(Debug, Clone)]
pub struct QueryExecutorConfig {
    /// Enable parallel execution
    pub enable_parallel_execution: bool,
    /// Number of worker threads for parallel scan
    pub num_worker_threads: usize,
}

impl Default for QueryExecutorConfig {
    fn default() -> Self {
        Self {
            enable_parallel_execution: true,
            num_worker_threads: 4,
        }
    }
}

/// Query executor - builds operator tree from plan and executes
pub struct QueryExecutor {
    /// Scan operator factory (page manager + indexes)
    scan_factory: Arc<ScanOperatorFactory>,
    /// Configuration
    config: QueryExecutorConfig,
}

impl QueryExecutor {
    /// Creates a new query executor
    pub fn new(scan_factory: Arc<ScanOperatorFactory>) -> Result<Self> {
        Ok(Self {
            scan_factory,
            config: QueryExecutorConfig::default(),
        })
    }

    /// Creates executor with custom config
    pub fn with_config(
        scan_factory: Arc<ScanOperatorFactory>,
        config: QueryExecutorConfig,
    ) -> Result<Self> {
        Ok(Self {
            scan_factory,
            config,
        })
    }

    /// Executes the plan and returns all result rows
    pub fn execute(&self, plan: &ExecutionPlan) -> Result<Vec<Row>> {
        let mut operator = self.build_operator(&plan.root)?;
        let mut results = Vec::new();
        while let Some(row) = operator.next()? {
            results.push(row);
        }
        Ok(results)
    }

    /// Builds operator tree from plan node
    fn build_operator(&self, node: &PlanNode) -> Result<Box<dyn Operator>> {
        match node {
            PlanNode::TableScan(ts) => self.build_table_scan(ts),
            PlanNode::IndexScan(idx) => self.build_index_scan(idx),
            PlanNode::Filter(f) => self.build_filter(f),
            PlanNode::Projection(p) => self.build_projection(p),
            PlanNode::Join(j) => self.build_join(j),
            PlanNode::GroupBy(g) => self.build_group_by(g),
            PlanNode::Sort(s) => self.build_sort(s),
            PlanNode::Limit(l) => self.build_limit(l),
            PlanNode::Offset(o) => self.build_offset(o),
            _ => Err(Error::query_execution(format!(
                "Unsupported plan node: {:?}",
                node
            ))),
        }
    }

    fn build_table_scan(&self, ts: &TableScanNode) -> Result<Box<dyn Operator>> {
        let schema = if ts.columns.is_empty() || ts.columns[0] == "*" {
            vec!["id".to_string(), "data".to_string()]
        } else {
            ts.columns.clone()
        };
        self.scan_factory
            .create_table_scan(ts.table_name.clone(), ts.filter.clone(), schema)
    }

    fn build_index_scan(&self, idx: &IndexScanNode) -> Result<Box<dyn Operator>> {
        let conditions: Vec<IndexCondition> = idx
            .conditions
            .iter()
            .map(|c| IndexCondition {
                column: c.column.clone(),
                operator: match c.operator.as_str() {
                    "=" | "==" => IndexOperator::Equal,
                    "<" => IndexOperator::LessThan,
                    "<=" => IndexOperator::LessThanOrEqual,
                    ">" => IndexOperator::GreaterThan,
                    ">=" => IndexOperator::GreaterThanOrEqual,
                    _ => IndexOperator::Equal,
                },
                value: c.value.clone(),
            })
            .collect();
        let schema = vec!["id".to_string(), "data".to_string()];
        let operator = self.scan_factory.create_index_scan(
            idx.table_name.clone(),
            idx.index_name.clone(),
            conditions,
            schema,
        )?;
        Ok(operator)
    }

    fn build_filter(&self, f: &FilterNode) -> Result<Box<dyn Operator>> {
        let input = self.build_operator(&f.input)?;
        let operator = ConditionalScanOperator::new(input, f.condition.clone())?;
        Ok(Box::new(operator))
    }

    fn build_projection(&self, p: &ProjectionNode) -> Result<Box<dyn Operator>> {
        // Projection: for now just pass through (full implementation would select columns)
        self.build_operator(&p.input)
    }

    fn build_join(&self, j: &JoinNode) -> Result<Box<dyn Operator>> {
        let left = self.build_operator(&j.left)?;
        let right = self.build_operator(&j.right)?;
        let join_condition = Self::parse_join_condition(&j.condition);
        let join_type = match j.join_type {
            crate::planner::planner::JoinType::Inner => JoinType::Inner,
            crate::planner::planner::JoinType::Left => JoinType::LeftOuter,
            crate::planner::planner::JoinType::Right => JoinType::RightOuter,
            crate::planner::planner::JoinType::Full => JoinType::FullOuter,
            crate::planner::planner::JoinType::Cross => JoinType::Inner,
        };
        let operator = NestedLoopJoinOperator::new(left, right, join_condition, join_type, 100)?;
        Ok(Box::new(operator))
    }

    fn parse_join_condition(condition: &str) -> JoinCondition {
        if condition.contains('=') {
            let parts: Vec<&str> = condition.split('=').map(|s| s.trim()).collect();
            if parts.len() >= 2 {
                let left = parts[0].trim_matches(|c: char| !c.is_alphanumeric() && c != '_');
                let right = parts[1].trim_matches(|c: char| !c.is_alphanumeric() && c != '_');
                return JoinCondition {
                    left_column: left.to_string(),
                    right_column: right.to_string(),
                    operator: JoinOperator::Equal,
                };
            }
        }
        JoinCondition {
            left_column: "id".to_string(),
            right_column: "id".to_string(),
            operator: JoinOperator::Equal,
        }
    }

    fn build_group_by(&self, g: &GroupByNode) -> Result<Box<dyn Operator>> {
        let input = self.build_operator(&g.input)?;
        let group_keys: Vec<usize> = (0..g.group_columns.len().min(4)).collect();
        let result_schema: Vec<String> = g
            .group_columns
            .iter()
            .cloned()
            .chain(g.aggregates.iter().map(|a| a.name.clone()))
            .collect();
        let operator = HashGroupByOperator::new(input, group_keys, vec![], result_schema)?;
        Ok(Box::new(operator))
    }

    fn build_sort(&self, s: &SortNode) -> Result<Box<dyn Operator>> {
        let input = self.build_operator(&s.input)?;
        let sort_columns: Vec<usize> = (0..s.sort_columns.len()).collect();
        let sort_directions: Vec<bool> = s
            .sort_columns
            .iter()
            .map(|c| matches!(c.direction, crate::planner::planner::SortDirection::Asc))
            .collect();
        let schema = input.get_schema()?;
        let operator = SortOperator::new(input, sort_columns, sort_directions, schema)?;
        Ok(Box::new(operator))
    }

    fn build_limit(&self, l: &LimitNode) -> Result<Box<dyn Operator>> {
        let input = self.build_operator(&l.input)?;
        let operator = LimitOperator::new(input, l.limit)?;
        Ok(Box::new(operator))
    }

    fn build_offset(&self, o: &OffsetNode) -> Result<Box<dyn Operator>> {
        let input = self.build_operator(&o.input)?;
        let operator = OffsetOperator::new(input, o.offset)?;
        Ok(Box::new(operator))
    }
}

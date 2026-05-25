//! Text formatting for `EXPLAIN` query plans.

use crate::planner::optimizer::OptimizationResult;
use crate::planner::planner::{ExecutionPlan, PlanNode, SetOpType};

/// Options controlling plan text output.
#[derive(Debug, Clone, Copy, Default)]
pub struct ExplainFormatOptions {
    pub verbose: bool,
    pub analyze: bool,
    pub execution_time_ms: Option<u64>,
    pub rows_returned: Option<usize>,
    pub rows_affected: Option<u64>,
}

/// Format a full EXPLAIN result as lines suitable for a single-column result set.
pub fn format_explain_output(
    plan: &ExecutionPlan,
    opt: &OptimizationResult,
    opts: ExplainFormatOptions,
) -> Vec<String> {
    let mut lines = Vec::new();
    format_plan_node(&plan.root, 0, &mut lines);
    lines.push(String::new());
    lines.push(format!(
        "Planning: cost={:.2} rows={} ops={} depth={} tables={} joins={}",
        plan.metadata.estimated_cost,
        plan.metadata.estimated_rows,
        plan.metadata.statistics.operator_count,
        plan.metadata.statistics.max_depth,
        plan.metadata.statistics.table_count,
        plan.metadata.statistics.join_count,
    ));
    if opts.verbose && !opt.messages.is_empty() {
        lines.push("Optimizer:".to_string());
        for msg in &opt.messages {
            lines.push(format!("  - {}", msg));
        }
    }
    if opts.analyze {
        if let Some(ms) = opts.execution_time_ms {
            lines.push(format!("Execution Time: {} ms", ms));
        }
        if let Some(n) = opts.rows_returned {
            lines.push(format!("Rows: {}", n));
        }
        if let Some(n) = opts.rows_affected {
            lines.push(format!("Rows Affected: {}", n));
        }
    }
    lines
}

fn indent(depth: usize) -> String {
    "  ".repeat(depth)
}

fn format_plan_node(node: &PlanNode, depth: usize, lines: &mut Vec<String>) {
    let pad = indent(depth);
    match node {
        PlanNode::TableScan(n) => {
            let alias = n
                .alias
                .as_ref()
                .map(|a| format!(" AS {}", a))
                .unwrap_or_default();
            let filt = n
                .filter
                .as_ref()
                .map(|f| format!(" filter={}", f))
                .unwrap_or_default();
            lines.push(format!(
                "{pad}Table Scan on {}{} (cost={:.2} rows={}){}",
                n.table_name, alias, n.cost, n.estimated_rows, filt
            ));
        }
        PlanNode::IndexScan(n) => {
            lines.push(format!(
                "{pad}Index Scan on {} using {} (cost={:.2} rows={})",
                n.table_name, n.index_name, n.cost, n.estimated_rows
            ));
            for c in &n.conditions {
                lines.push(format!(
                    "{pad}  Index Cond: {} {} {}",
                    c.column, c.operator, c.value
                ));
            }
        }
        PlanNode::Filter(n) => {
            lines.push(format!(
                "{pad}Filter: {} (cost={:.2} selectivity={:.4})",
                n.condition, n.cost, n.selectivity
            ));
            format_plan_node(&n.input, depth + 1, lines);
        }
        PlanNode::Projection(n) => {
            let cols = if n.columns.is_empty() {
                "*".to_string()
            } else {
                n.columns
                    .iter()
                    .map(|c| c.name.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            };
            lines.push(format!("{pad}Projection: {} (cost={:.2})", cols, n.cost));
            format_plan_node(&n.input, depth + 1, lines);
        }
        PlanNode::Join(n) => {
            lines.push(format!(
                "{pad}{:?} Join: {} (cost={:.2})",
                n.join_type, n.condition, n.cost
            ));
            lines.push(format!("{pad}  -> Left:"));
            format_plan_node(&n.left, depth + 2, lines);
            lines.push(format!("{pad}  -> Right:"));
            format_plan_node(&n.right, depth + 2, lines);
        }
        PlanNode::GroupBy(n) => {
            lines.push(format!(
                "{pad}Group By: {:?} aggregates={:?} (cost={:.2})",
                n.group_columns, n.aggregates, n.cost
            ));
            format_plan_node(&n.input, depth + 1, lines);
        }
        PlanNode::Sort(n) => {
            lines.push(format!(
                "{pad}Sort: {:?} (cost={:.2})",
                n.sort_columns, n.cost
            ));
            format_plan_node(&n.input, depth + 1, lines);
        }
        PlanNode::Limit(n) => {
            lines.push(format!("{pad}Limit: {} (cost={:.2})", n.limit, n.cost));
            format_plan_node(&n.input, depth + 1, lines);
        }
        PlanNode::Offset(n) => {
            lines.push(format!("{pad}Offset: {} (cost={:.2})", n.offset, n.cost));
            format_plan_node(&n.input, depth + 1, lines);
        }
        PlanNode::Aggregate(n) => {
            lines.push(format!(
                "{pad}Aggregate: {:?} (cost={:.2})",
                n.aggregates, n.cost
            ));
            format_plan_node(&n.input, depth + 1, lines);
        }
        PlanNode::Insert(n) => {
            lines.push(format!(
                "{pad}Insert on {} (cost={:.2})",
                n.table_name, n.cost
            ));
            if let Some(sub) = &n.insert_subplan {
                lines.push(format!("{pad}  -> Subplan:"));
                format_plan_node(sub, depth + 2, lines);
            }
        }
        PlanNode::Update(n) => {
            let wh = n
                .where_condition
                .as_ref()
                .map(|w| format!(" WHERE {}", w))
                .unwrap_or_default();
            lines.push(format!(
                "{pad}Update on {}{} (cost={:.2})",
                n.table_name, wh, n.cost
            ));
        }
        PlanNode::Delete(n) => {
            let wh = n
                .where_condition
                .as_ref()
                .map(|w| format!(" WHERE {}", w))
                .unwrap_or_default();
            lines.push(format!(
                "{pad}Delete on {}{} (cost={:.2})",
                n.table_name, wh, n.cost
            ));
        }
        PlanNode::Distinct(n) => {
            lines.push(format!("{pad}Distinct (cost={:.2})", n.cost));
            format_plan_node(&n.input, depth + 1, lines);
        }
        PlanNode::SetOp(n) => {
            let op = match n.op {
                SetOpType::Union => "Union",
                SetOpType::Intersect => "Intersect",
                SetOpType::Except => "Except",
            };
            let all = if n.all { " ALL" } else { "" };
            lines.push(format!("{pad}{op}{all} (cost={:.2})", n.cost));
            lines.push(format!("{pad}  -> Left:"));
            format_plan_node(&n.left, depth + 2, lines);
            lines.push(format!("{pad}  -> Right:"));
            format_plan_node(&n.right, depth + 2, lines);
        }
        PlanNode::SemiJoin(n) => {
            lines.push(format!(
                "{pad}Semi Join: {} (cost={:.2})",
                n.condition, n.cost
            ));
            format_plan_node(&n.left, depth + 1, lines);
            format_plan_node(&n.right, depth + 1, lines);
        }
        PlanNode::AntiJoin(n) => {
            lines.push(format!(
                "{pad}Anti Join: {} (cost={:.2})",
                n.condition, n.cost
            ));
            format_plan_node(&n.left, depth + 1, lines);
            format_plan_node(&n.right, depth + 1, lines);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::planner::optimizer::{OptimizationResult, OptimizationStatistics};
    use crate::planner::planner::{PlanMetadata, PlanStatistics, TableScanNode};
    use std::time::SystemTime;

    #[test]
    fn format_table_scan_line() {
        let plan = ExecutionPlan {
            root: PlanNode::TableScan(TableScanNode {
                table_name: "t".to_string(),
                alias: None,
                columns: vec!["a".to_string()],
                filter: None,
                cost: 1.5,
                estimated_rows: 10,
            }),
            metadata: PlanMetadata {
                estimated_cost: 1.5,
                estimated_rows: 10,
                created_at: SystemTime::UNIX_EPOCH,
                statistics: PlanStatistics {
                    operator_count: 1,
                    max_depth: 1,
                    table_count: 1,
                    join_count: 0,
                },
            },
        };
        let opt = OptimizationResult {
            optimized_plan: plan.clone(),
            statistics: OptimizationStatistics::default(),
            messages: vec!["index selected".to_string()],
        };
        let lines = format_explain_output(
            &plan,
            &opt,
            ExplainFormatOptions {
                verbose: true,
                ..Default::default()
            },
        );
        assert!(lines.iter().any(|l| l.contains("Table Scan on t")));
        assert!(lines.iter().any(|l| l.contains("index selected")));
    }
}

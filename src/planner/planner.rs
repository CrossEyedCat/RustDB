//! Query planner for rustdb

use crate::analyzer::{AnalysisContext, SemanticAnalyzer};
use crate::common::{Error, Result};
use crate::parser::ast::{
    DeleteStatement, InsertStatement, SelectStatement, SqlStatement, UpdateStatement,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Query execution plan
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExecutionPlan {
    /// Root operator of the plan
    pub root: PlanNode,
    /// Plan metadata
    pub metadata: PlanMetadata,
}

/// Execution plan metadata
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PlanMetadata {
    /// Estimated execution cost
    pub estimated_cost: f64,
    /// Estimated number of result rows
    pub estimated_rows: usize,
    /// Plan creation time
    pub created_at: std::time::SystemTime,
    /// Plan statistics
    pub statistics: PlanStatistics,
}

/// Execution plan statistics
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PlanStatistics {
    /// Number of operators in the plan
    pub operator_count: usize,
    /// Maximum plan depth
    pub max_depth: usize,
    /// Number of tables in the query
    pub table_count: usize,
    /// Number of JOIN operations
    pub join_count: usize,
}

/// Execution plan node
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum PlanNode {
    /// Table scan
    TableScan(TableScanNode),
    /// Index scan
    IndexScan(IndexScanNode),
    /// Filtering
    Filter(FilterNode),
    /// Projection (column selection)
    Projection(ProjectionNode),
    /// Table join
    Join(JoinNode),
    /// Grouping
    GroupBy(GroupByNode),
    /// Sorting
    Sort(SortNode),
    /// Row count limit
    Limit(LimitNode),
    /// Offset
    Offset(OffsetNode),
    /// Aggregation
    Aggregate(AggregateNode),
    /// Data insertion
    Insert(InsertNode),
    /// Data update
    Update(UpdateNode),
    /// Data deletion
    Delete(DeleteNode),
}

/// Table scan node
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TableScanNode {
    /// Table name
    pub table_name: String,
    /// Table alias
    pub alias: Option<String>,
    /// List of columns to read
    pub columns: Vec<String>,
    /// Filter condition (if any)
    pub filter: Option<String>,
    /// Cost estimate
    pub cost: f64,
    /// Estimated number of rows
    pub estimated_rows: usize,
}

/// Index scan node
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IndexScanNode {
    /// Table name
    pub table_name: String,
    /// Index name
    pub index_name: String,
    /// Index search conditions
    pub conditions: Vec<IndexCondition>,
    /// Cost estimate
    pub cost: f64,
    /// Estimated number of rows
    pub estimated_rows: usize,
}

/// Index search condition
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IndexCondition {
    /// Column name
    pub column: String,
    /// Comparison operator
    pub operator: String,
    /// Value for comparison
    pub value: String,
}

/// Filter node
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FilterNode {
    /// Filter condition
    pub condition: String,
    /// Input node
    pub input: Box<PlanNode>,
    /// Selectivity estimate
    pub selectivity: f64,
    /// Cost estimate
    pub cost: f64,
}

/// Projection node
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProjectionNode {
    /// List of columns for projection
    pub columns: Vec<ProjectionColumn>,
    /// Input node
    pub input: Box<PlanNode>,
    /// Cost estimate
    pub cost: f64,
}

/// Projection column
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProjectionColumn {
    /// Column name
    pub name: String,
    /// Expression to compute
    pub expression: Option<String>,
    /// Alias
    pub alias: Option<String>,
}

/// Join node
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JoinNode {
    /// Join type
    pub join_type: JoinType,
    /// Join condition
    pub condition: String,
    /// Left input node
    pub left: Box<PlanNode>,
    /// Right input node
    pub right: Box<PlanNode>,
    /// Cost estimate
    pub cost: f64,
}

/// Join type
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
    Cross,
}

/// GroupBy node
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GroupByNode {
    /// Columns for grouping
    pub group_columns: Vec<String>,
    /// Aggregate functions
    pub aggregates: Vec<AggregateFunction>,
    /// Input node
    pub input: Box<PlanNode>,
    /// Cost estimate
    pub cost: f64,
}

/// Aggregate function
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AggregateFunction {
    /// Function name
    pub name: String,
    /// Function argument
    pub argument: String,
    /// Result alias
    pub alias: Option<String>,
}

/// Sort node
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SortNode {
    /// Columns for sorting
    pub sort_columns: Vec<SortColumn>,
    /// Input node
    pub input: Box<PlanNode>,
    /// Cost estimate
    pub cost: f64,
}

/// Sort column
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SortColumn {
    /// Column name
    pub column: String,
    /// Sort direction
    pub direction: SortDirection,
}

/// Sort direction
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum SortDirection {
    Asc,
    Desc,
}

/// Limit node
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LimitNode {
    /// Number of rows to limit
    pub limit: usize,
    /// Input node
    pub input: Box<PlanNode>,
    /// Cost estimate
    pub cost: f64,
}

/// Offset node
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OffsetNode {
    /// Number of rows to skip
    pub offset: usize,
    /// Input node
    pub input: Box<PlanNode>,
    /// Cost estimate
    pub cost: f64,
}

/// Aggregate node
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AggregateNode {
    /// Aggregate functions
    pub aggregates: Vec<AggregateFunction>,
    /// Input node
    pub input: Box<PlanNode>,
    /// Cost estimate
    pub cost: f64,
}

/// Insert node
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InsertNode {
    /// Table name
    pub table_name: String,
    /// Columns for insertion
    pub columns: Vec<String>,
    /// Values for insertion
    pub values: Vec<Vec<String>>,
    /// Cost estimate
    pub cost: f64,
}

/// Update node
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UpdateNode {
    /// Table name
    pub table_name: String,
    /// Assignments (column = value)
    pub assignments: Vec<Assignment>,
    /// WHERE condition
    pub where_condition: Option<String>,
    /// Cost estimate
    pub cost: f64,
}

/// Assignment in UPDATE
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Assignment {
    /// Column name
    pub column: String,
    /// New value
    pub value: String,
}

/// Delete node
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DeleteNode {
    /// Table name
    pub table_name: String,
    /// WHERE condition
    pub where_condition: Option<String>,
    /// Cost estimate
    pub cost: f64,
}

/// Query planner
pub struct QueryPlanner {
    /// Semantic analyzer
    semantic_analyzer: SemanticAnalyzer,
    /// Planner settings
    settings: PlannerSettings,
    /// Plan cache
    plan_cache: HashMap<String, ExecutionPlan>,
}

/// Planner settings
#[derive(Debug, Clone)]
pub struct PlannerSettings {
    /// Enable plan caching
    pub enable_plan_cache: bool,
    /// Maximum plan cache size
    pub max_cache_size: usize,
    /// Enable optimization
    pub enable_optimization: bool,
    /// Maximum recursion depth
    pub max_recursion_depth: usize,
    /// Enable detailed logging
    pub enable_debug_logging: bool,
}

impl Default for PlannerSettings {
    fn default() -> Self {
        Self {
            enable_plan_cache: true,
            max_cache_size: 1000,
            enable_optimization: true,
            max_recursion_depth: 100,
            enable_debug_logging: false,
        }
    }
}

impl QueryPlanner {
    /// Creates a new query planner
    pub fn new() -> Result<Self> {
        let semantic_analyzer = SemanticAnalyzer::default();
        Ok(Self {
            semantic_analyzer,
            settings: PlannerSettings::default(),
            plan_cache: HashMap::new(),
        })
    }

    /// Creates planner with settings
    pub fn with_settings(settings: PlannerSettings) -> Result<Self> {
        let semantic_analyzer = SemanticAnalyzer::default();
        Ok(Self {
            semantic_analyzer,
            settings,
            plan_cache: HashMap::new(),
        })
    }

    /// Creates execution plan for SQL query
    pub fn create_plan(&mut self, sql_statement: &SqlStatement) -> Result<ExecutionPlan> {
        // First perform semantic analysis
        let context = AnalysisContext::default();
        let analysis_result = self.semantic_analyzer.analyze(sql_statement, &context)?;

        if !analysis_result.errors.is_empty() {
            return Err(Error::semantic_analysis(format!(
                "Semantic errors: {:?}",
                analysis_result.errors
            )));
        }

        // Create plan based on query type
        let root = match sql_statement {
            SqlStatement::Select(select) => self.create_select_plan(select)?,
            SqlStatement::Insert(insert) => self.create_insert_plan(insert)?,
            SqlStatement::Update(update) => self.create_update_plan(update)?,
            SqlStatement::Delete(delete) => self.create_delete_plan(delete)?,
            _ => return Err(Error::semantic_analysis("Unsupported query type")),
        };

        // Create plan metadata
        let metadata = self.create_plan_metadata(&root)?;

        Ok(ExecutionPlan { root, metadata })
    }

    /// Creates plan for SELECT query
    fn create_select_plan(&self, select: &SelectStatement) -> Result<PlanNode> {
        // Create base table scan plan
        let mut current_plan = if let Some(from) = &select.from {
            self.create_table_scan_plan(&from.table)?
        } else {
            // If no FROM, create empty plan
            PlanNode::TableScan(TableScanNode {
                table_name: "".to_string(),
                alias: None,
                columns: vec![],
                filter: None,
                cost: 0.0,
                estimated_rows: 0,
            })
        };

        // Add JOIN operations
        if let Some(from) = &select.from {
            for join in &from.joins {
                let join_plan = self.create_table_scan_plan(&join.table)?;
                current_plan = PlanNode::Join(JoinNode {
                    join_type: match join.join_type {
                        crate::parser::ast::JoinType::Inner => JoinType::Inner,
                        crate::parser::ast::JoinType::Left => JoinType::Left,
                        crate::parser::ast::JoinType::Right => JoinType::Right,
                        crate::parser::ast::JoinType::Full => JoinType::Full,
                        crate::parser::ast::JoinType::Cross => JoinType::Cross,
                    },
                    condition: join
                        .condition
                        .as_ref()
                        .map(|e| format!("{:?}", e))
                        .unwrap_or_default(),
                    left: Box::new(current_plan),
                    right: Box::new(join_plan),
                    cost: 0.0, // TODO: Calculate cost
                });
            }
        }

        // Add WHERE condition
        if let Some(where_clause) = &select.where_clause {
            current_plan = PlanNode::Filter(FilterNode {
                condition: format!("{:?}", where_clause),
                input: Box::new(current_plan),
                selectivity: 0.5, // TODO: Calculate selectivity
                cost: 0.0,
            });
        }

        // Add GROUP BY
        if !select.group_by.is_empty() {
            current_plan = PlanNode::GroupBy(GroupByNode {
                group_columns: select.group_by.iter().map(|e| format!("{:?}", e)).collect(),
                aggregates: vec![], // TODO: Extract aggregate functions
                input: Box::new(current_plan),
                cost: 0.0,
            });
        }

        // Add HAVING
        if let Some(having) = &select.having {
            current_plan = PlanNode::Filter(FilterNode {
                condition: format!("{:?}", having),
                input: Box::new(current_plan),
                selectivity: 0.5,
                cost: 0.0,
            });
        }

        // Add ORDER BY
        if !select.order_by.is_empty() {
            current_plan = PlanNode::Sort(SortNode {
                sort_columns: select
                    .order_by
                    .iter()
                    .map(|item| SortColumn {
                        column: format!("{:?}", item.expr),
                        direction: match item.direction {
                            crate::parser::ast::OrderDirection::Asc => SortDirection::Asc,
                            crate::parser::ast::OrderDirection::Desc => SortDirection::Desc,
                        },
                    })
                    .collect(),
                input: Box::new(current_plan),
                cost: 0.0,
            });
        }

        // Add LIMIT
        if let Some(limit) = select.limit {
            current_plan = PlanNode::Limit(LimitNode {
                limit: limit as usize,
                input: Box::new(current_plan),
                cost: 0.0,
            });
        }

        // Add OFFSET
        if let Some(offset) = select.offset {
            current_plan = PlanNode::Offset(OffsetNode {
                offset: offset as usize,
                input: Box::new(current_plan),
                cost: 0.0,
            });
        }

        // Add projection
        current_plan = PlanNode::Projection(ProjectionNode {
            columns: select
                .select_list
                .iter()
                .map(|item| match item {
                    crate::parser::ast::SelectItem::Wildcard => ProjectionColumn {
                        name: "*".to_string(),
                        expression: None,
                        alias: None,
                    },
                    crate::parser::ast::SelectItem::Expression { expr, alias } => {
                        ProjectionColumn {
                            name: format!("{:?}", expr),
                            expression: Some(format!("{:?}", expr)),
                            alias: alias.clone(),
                        }
                    }
                })
                .collect(),
            input: Box::new(current_plan),
            cost: 0.0,
        });

        Ok(current_plan)
    }

    /// Creates table scan plan
    fn create_table_scan_plan(
        &self,
        table_ref: &crate::parser::ast::TableReference,
    ) -> Result<PlanNode> {
        match table_ref {
            crate::parser::ast::TableReference::Table { name, alias } => {
                Ok(PlanNode::TableScan(TableScanNode {
                    table_name: name.clone(),
                    alias: alias.clone(),
                    columns: vec!["*".to_string()], // TODO: Determine specific columns
                    filter: None,
                    cost: 1.0,            // Base scan cost
                    estimated_rows: 1000, // TODO: Get from statistics
                }))
            }
            crate::parser::ast::TableReference::Subquery { query, alias } => {
                // Recursively create plan for subquery
                let subquery_plan = self.create_select_plan(query)?;
                Ok(PlanNode::Projection(ProjectionNode {
                    columns: vec![ProjectionColumn {
                        name: alias.clone(),
                        expression: None,
                        alias: Some(alias.clone()),
                    }],
                    input: Box::new(subquery_plan),
                    cost: 0.0,
                }))
            }
        }
    }

    /// Creates plan for INSERT query
    fn create_insert_plan(&self, insert: &InsertStatement) -> Result<PlanNode> {
        Ok(PlanNode::Insert(InsertNode {
            table_name: insert.table.clone(),
            columns: insert.columns.clone().unwrap_or_default(),
            values: match &insert.values {
                crate::parser::ast::InsertValues::Values(values) => values
                    .iter()
                    .map(|row| row.iter().map(|val| format!("{:?}", val)).collect())
                    .collect(),
                crate::parser::ast::InsertValues::Select(_) => {
                    vec![] // TODO: Handle INSERT ... SELECT
                }
            },
            cost: 1.0,
        }))
    }

    /// Creates plan for UPDATE query
    fn create_update_plan(&self, update: &UpdateStatement) -> Result<PlanNode> {
        Ok(PlanNode::Update(UpdateNode {
            table_name: update.table.clone(),
            assignments: update
                .assignments
                .iter()
                .map(|assignment| Assignment {
                    column: assignment.column.clone(),
                    value: format!("{:?}", assignment.value),
                })
                .collect(),
            where_condition: update.where_clause.as_ref().map(|e| format!("{:?}", e)),
            cost: 1.0,
        }))
    }

    /// Creates plan for DELETE query
    fn create_delete_plan(&self, delete: &DeleteStatement) -> Result<PlanNode> {
        Ok(PlanNode::Delete(DeleteNode {
            table_name: delete.table.clone(),
            where_condition: delete.where_clause.as_ref().map(|e| format!("{:?}", e)),
            cost: 1.0,
        }))
    }

    /// Creates plan metadata
    fn create_plan_metadata(&self, root: &PlanNode) -> Result<PlanMetadata> {
        let (operator_count, max_depth, table_count, join_count) =
            self.analyze_plan_structure(root, 0);

        Ok(PlanMetadata {
            estimated_cost: self.estimate_plan_cost(root),
            estimated_rows: self.estimate_plan_rows(root),
            created_at: std::time::SystemTime::now(),
            statistics: PlanStatistics {
                operator_count,
                max_depth,
                table_count,
                join_count,
            },
        })
    }

    /// Analyzes plan structure
    fn analyze_plan_structure(
        &self,
        node: &PlanNode,
        depth: usize,
    ) -> (usize, usize, usize, usize) {
        let mut operator_count = 1;
        let mut max_depth = depth;
        let mut table_count = 0;
        let mut join_count = 0;

        // Count specific operators
        match node {
            PlanNode::TableScan(_) => table_count += 1,
            PlanNode::Join(_) => join_count += 1,
            _ => {}
        }

        // Recursively analyze child nodes
        let child_nodes = self.get_child_nodes(node);
        for child in child_nodes {
            let (child_ops, child_depth, child_tables, child_joins) =
                self.analyze_plan_structure(child, depth + 1);
            operator_count += child_ops;
            max_depth = max_depth.max(child_depth);
            table_count += child_tables;
            join_count += child_joins;
        }

        (operator_count, max_depth, table_count, join_count)
    }

    /// Gets child nodes of the plan
    fn get_child_nodes<'a>(&self, node: &'a PlanNode) -> Vec<&'a PlanNode> {
        match node {
            PlanNode::Filter(node) => vec![&node.input],
            PlanNode::Projection(node) => vec![&node.input],
            PlanNode::Join(node) => vec![&node.left, &node.right],
            PlanNode::GroupBy(node) => vec![&node.input],
            PlanNode::Sort(node) => vec![&node.input],
            PlanNode::Limit(node) => vec![&node.input],
            PlanNode::Offset(node) => vec![&node.input],
            PlanNode::Aggregate(node) => vec![&node.input],
            _ => vec![],
        }
    }

    /// Estimates plan cost
    fn estimate_plan_cost(&self, node: &PlanNode) -> f64 {
        match node {
            PlanNode::TableScan(node) => node.cost,
            PlanNode::IndexScan(node) => node.cost,
            PlanNode::Filter(node) => node.cost + self.estimate_plan_cost(&node.input),
            PlanNode::Projection(node) => node.cost + self.estimate_plan_cost(&node.input),
            PlanNode::Join(node) => {
                node.cost
                    + self.estimate_plan_cost(&node.left)
                    + self.estimate_plan_cost(&node.right)
            }
            PlanNode::GroupBy(node) => node.cost + self.estimate_plan_cost(&node.input),
            PlanNode::Sort(node) => node.cost + self.estimate_plan_cost(&node.input),
            PlanNode::Limit(node) => node.cost + self.estimate_plan_cost(&node.input),
            PlanNode::Offset(node) => node.cost + self.estimate_plan_cost(&node.input),
            PlanNode::Aggregate(node) => node.cost + self.estimate_plan_cost(&node.input),
            PlanNode::Insert(node) => node.cost,
            PlanNode::Update(node) => node.cost,
            PlanNode::Delete(node) => node.cost,
        }
    }

    /// Estimates number of rows in result
    fn estimate_plan_rows(&self, node: &PlanNode) -> usize {
        match node {
            PlanNode::TableScan(node) => node.estimated_rows,
            PlanNode::IndexScan(node) => node.estimated_rows,
            PlanNode::Filter(node) => {
                (self.estimate_plan_rows(&node.input) as f64 * node.selectivity) as usize
            }
            PlanNode::Projection(node) => self.estimate_plan_rows(&node.input),
            PlanNode::Join(node) => {
                let left_rows = self.estimate_plan_rows(&node.left);
                let right_rows = self.estimate_plan_rows(&node.right);
                left_rows * right_rows / 1000 // Simplified estimate
            }
            PlanNode::GroupBy(node) => self.estimate_plan_rows(&node.input) / 10, // Simplified estimate
            PlanNode::Sort(node) => self.estimate_plan_rows(&node.input),
            PlanNode::Limit(node) => node.limit.min(self.estimate_plan_rows(&node.input)),
            PlanNode::Offset(node) => {
                let input_rows = self.estimate_plan_rows(&node.input);
                if node.offset >= input_rows {
                    0
                } else {
                    input_rows - node.offset
                }
            }
            PlanNode::Aggregate(node) => self.estimate_plan_rows(&node.input) / 10,
            PlanNode::Insert(_) => 1,
            PlanNode::Update(_) => 1,
            PlanNode::Delete(_) => 1,
        }
    }

    /// Gets planner settings
    pub fn settings(&self) -> &PlannerSettings {
        &self.settings
    }

    /// Updates planner settings
    pub fn update_settings(&mut self, settings: PlannerSettings) {
        self.settings = settings;
    }

    /// Clears plan cache
    pub fn clear_cache(&mut self) {
        self.plan_cache.clear();
    }

    /// Gets cache statistics
    pub fn cache_stats(&self) -> CacheStats {
        CacheStats {
            size: self.plan_cache.len(),
            max_size: self.settings.max_cache_size,
        }
    }
}

/// Plan cache statistics
#[derive(Debug, Clone)]
pub struct CacheStats {
    /// Current cache size
    pub size: usize,
    /// Maximum cache size
    pub max_size: usize,
}

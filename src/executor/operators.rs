//! Execution operators for rustdb

use crate::common::types::{ColumnValue, DataType};
use crate::common::{Error, Result};
use crate::parser::ast::Literal;
use crate::parser::ast::{BinaryOperator, Expression, UnaryOperator};
use crate::planner::planner::AggregateFunction as PlanAggregateFunction;
use crate::planner::planner::ProjectionColumn;
use crate::planner::planner::SimpleEqualityFilter;
use crate::planner::{ExecutionPlan, PlanNode};
use crate::storage::index::BPlusTree;
use crate::storage::index::Index;
use crate::storage::page_manager::PageManager as StoragePageManager;
use crate::storage::page_manager::PageManagerConfig;
use crate::storage::tuple::Tuple;
use crate::{RecordId, Row};

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Instant;

/// Base trait for all operators
pub trait Operator {
    /// Get next result row
    fn next(&mut self) -> Result<Option<Row>>;

    /// Reset operator for re-execution
    fn reset(&mut self) -> Result<()>;

    /// Get result schema
    fn get_schema(&self) -> Result<Vec<String>>;

    /// Get execution statistics
    fn get_statistics(&self) -> OperatorStatistics;
}

/// Operator execution statistics
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct OperatorStatistics {
    /// Number of processed rows
    pub rows_processed: usize,
    /// Number of returned rows
    pub rows_returned: usize,
    /// Execution time in milliseconds
    pub execution_time_ms: u64,
    /// Number of I/O operations
    pub io_operations: usize,
    /// Number of memory operations
    pub memory_operations: usize,
    /// Memory used in bytes
    pub memory_used_bytes: usize,
}

pub struct ProjectionOperator {
    input: Box<dyn Operator>,
    columns: Vec<ProjectionColumn>,
    wildcard: bool,
    statistics: OperatorStatistics,
}

fn eval_value_to_column_value(v: EvalValue) -> ColumnValue {
    match v {
        EvalValue::Null => ColumnValue::null(),
        EvalValue::Bool(b) => ColumnValue::new(DataType::Boolean(b)),
        EvalValue::Int(n) => {
            if n >= i32::MIN as i64 && n <= i32::MAX as i64 {
                ColumnValue::new(DataType::Integer(n as i32))
            } else {
                ColumnValue::new(DataType::BigInt(n))
            }
        }
        EvalValue::Float(f) => ColumnValue::new(DataType::Double(f)),
        EvalValue::String(s) => ColumnValue::new(DataType::Varchar(s)),
    }
}

impl ProjectionOperator {
    pub fn new(input: Box<dyn Operator>, columns: Vec<ProjectionColumn>) -> Result<Self> {
        let wildcard = columns.iter().any(|c| c.name == "*");
        Ok(Self {
            input,
            columns,
            wildcard,
            statistics: OperatorStatistics::default(),
        })
    }
}

impl Operator for ProjectionOperator {
    fn next(&mut self) -> Result<Option<Row>> {
        let start_time = std::time::Instant::now();
        let Some(row) = self.input.next()? else {
            self.statistics.execution_time_ms = start_time.elapsed().as_millis() as u64;
            return Ok(None);
        };
        self.statistics.rows_processed += 1;

        if self.wildcard {
            self.statistics.rows_returned += 1;
            self.statistics.execution_time_ms = start_time.elapsed().as_millis() as u64;
            return Ok(Some(row));
        }

        let mut out = Row::new();
        out.version = row.version;
        out.created_at = row.created_at;
        out.updated_at = row.updated_at;
        for c in &self.columns {
            let out_name = c.alias.clone().unwrap_or_else(|| c.name.clone());
            let v = match &c.expression {
                Some(expr) => eval_expression(&row, expr),
                None => EvalValue::Null,
            };
            out.set_value(&out_name, eval_value_to_column_value(v));
        }

        self.statistics.rows_returned += 1;
        self.statistics.execution_time_ms = start_time.elapsed().as_millis() as u64;
        Ok(Some(out))
    }

    fn reset(&mut self) -> Result<()> {
        self.input.reset()?;
        self.statistics = OperatorStatistics::default();
        Ok(())
    }

    fn get_schema(&self) -> Result<Vec<String>> {
        if self.wildcard {
            return self.input.get_schema();
        }
        let mut cols = Vec::new();
        for c in &self.columns {
            cols.push(c.alias.clone().unwrap_or_else(|| c.name.clone()));
        }
        Ok(cols)
    }

    fn get_statistics(&self) -> OperatorStatistics {
        self.statistics.clone()
    }
}

pub struct GroupByOperator {
    input: Box<dyn Operator>,
    group_columns: Vec<String>,
    aggregates: Vec<PlanAggregateFunction>,
    result_schema: Vec<String>,
    statistics: OperatorStatistics,
    results: Vec<Row>,
    cursor: usize,
}

impl GroupByOperator {
    pub fn new(
        input: Box<dyn Operator>,
        group_columns: Vec<String>,
        aggregates: Vec<PlanAggregateFunction>,
        result_schema: Vec<String>,
    ) -> Result<Self> {
        Ok(Self {
            input,
            group_columns,
            aggregates,
            result_schema,
            statistics: OperatorStatistics::default(),
            results: Vec::new(),
            cursor: 0,
        })
    }

    fn materialize(&mut self) -> Result<()> {
        if !self.results.is_empty() {
            return Ok(());
        }
        use std::collections::HashMap;
        let mut groups: HashMap<Vec<String>, Vec<Row>> = HashMap::new();
        while let Some(row) = self.input.next()? {
            self.statistics.rows_processed += 1;
            let key: Vec<String> = self
                .group_columns
                .iter()
                .map(|c| {
                    row.values
                        .get(c)
                        .map(|cv| format!("{:?}", cv.data_type))
                        .unwrap_or_else(|| "NULL".to_string())
                })
                .collect();
            groups.entry(key).or_default().push(row);
        }

        for (key, rows) in groups {
            let mut out = Row::new();
            for (i, col) in self.group_columns.iter().enumerate() {
                let v = key.get(i).cloned().unwrap_or_else(|| "NULL".to_string());
                out.set_value(col, ColumnValue::new(DataType::Text(v)));
            }
            for agg in &self.aggregates {
                let out_name = agg
                    .alias
                    .clone()
                    .unwrap_or_else(|| format!("{}({})", agg.name, agg.argument));
                let v = self.eval_aggregate(&rows, agg);
                out.set_value(&out_name, v.clone());
                out.set_value(&format!("{}({})", agg.name.to_uppercase(), agg.argument), v);
            }
            self.results.push(out);
        }
        Ok(())
    }

    fn eval_aggregate(&self, rows: &[Row], agg: &PlanAggregateFunction) -> ColumnValue {
        let name = agg.name.to_uppercase();
        let arg = agg.argument.clone();
        match name.as_str() {
            "COUNT" => {
                if arg == "*" {
                    ColumnValue::new(DataType::BigInt(rows.len() as i64))
                } else {
                    let mut c = 0i64;
                    for r in rows {
                        if let Some(v) = r.values.get(&arg) {
                            if !v.is_null {
                                c += 1;
                            }
                        }
                    }
                    ColumnValue::new(DataType::BigInt(c))
                }
            }
            "SUM" | "AVG" => {
                let mut sum = 0f64;
                let mut n = 0f64;
                for r in rows {
                    let Some(cv) = r.values.get(&arg) else {
                        continue;
                    };
                    match column_value_to_eval(cv) {
                        EvalValue::Int(i) => {
                            sum += i as f64;
                            n += 1.0;
                        }
                        EvalValue::Float(f) => {
                            sum += f;
                            n += 1.0;
                        }
                        _ => {}
                    }
                }
                if n == 0.0 {
                    ColumnValue::null()
                } else if name == "SUM" {
                    ColumnValue::new(DataType::Double(sum))
                } else {
                    ColumnValue::new(DataType::Double(sum / n))
                }
            }
            _ => ColumnValue::null(),
        }
    }
}

impl Operator for GroupByOperator {
    fn next(&mut self) -> Result<Option<Row>> {
        self.materialize()?;
        if self.cursor >= self.results.len() {
            return Ok(None);
        }
        let row = self.results[self.cursor].clone();
        self.cursor += 1;
        self.statistics.rows_returned += 1;
        Ok(Some(row))
    }

    fn reset(&mut self) -> Result<()> {
        self.input.reset()?;
        self.results.clear();
        self.cursor = 0;
        self.statistics = OperatorStatistics::default();
        Ok(())
    }

    fn get_schema(&self) -> Result<Vec<String>> {
        Ok(self.result_schema.clone())
    }

    fn get_statistics(&self) -> OperatorStatistics {
        self.statistics.clone()
    }
}

/// Table scan operator — reads heap records via [`StoragePageManager::select`] and deserializes [`Tuple`] to [`Row`].
pub struct TableScanOperator {
    #[allow(dead_code)]
    table_name: String,
    page_manager: Arc<Mutex<StoragePageManager>>,
    filter_condition: Option<String>,
    pushdown_equality: Option<SimpleEqualityFilter>,
    /// Projection column names from the plan (`*` = all tuple columns).
    schema: Vec<String>,
    statistics: OperatorStatistics,
    // Streaming scan state (avoid materializing all rows).
    page_ids: Option<Vec<u64>>,
    page_pos: usize,
    page_records: Vec<(u32, Vec<u8>)>,
    record_pos: usize,
}

impl TableScanOperator {
    /// Create new table scan operator
    pub fn new(
        table_name: String,
        page_manager: Arc<Mutex<StoragePageManager>>,
        filter_condition: Option<String>,
        pushdown_equality: Option<SimpleEqualityFilter>,
        schema: Vec<String>,
    ) -> Result<Self> {
        Ok(Self {
            table_name,
            page_manager,
            filter_condition,
            pushdown_equality,
            schema,
            statistics: OperatorStatistics::default(),
            page_ids: None,
            page_pos: 0,
            page_records: Vec::new(),
            record_pos: 0,
        })
    }

    fn ensure_initialized(&mut self) -> Result<()> {
        if self.page_ids.is_some() {
            return Ok(());
        }
        let span = tracing::info_span!(
            "operator.table_scan.init",
            table = %self.table_name
        );
        let _g = span.enter();
        let mut pm = self
            .page_manager
            .lock()
            .map_err(|_| Error::lock("page manager poisoned"))?;
        let ids = pm.all_page_ids()?;
        self.statistics.io_operations = self.statistics.io_operations.saturating_add(1);
        self.page_ids = Some(ids);
        self.page_pos = 0;
        self.page_records.clear();
        self.record_pos = 0;
        Ok(())
    }

    fn load_next_page(&mut self) -> Result<bool> {
        let Some(ids) = self.page_ids.as_ref() else {
            return Ok(false);
        };
        if self.page_pos >= ids.len() {
            return Ok(false);
        }
        let page_id = ids[self.page_pos];
        self.page_pos += 1;

        let span = tracing::info_span!(
            "operator.table_scan.load_page",
            table = %self.table_name,
            page_id
        );
        let _g = span.enter();

        let mut pm = self
            .page_manager
            .lock()
            .map_err(|_| Error::lock("page manager poisoned"))?;
        self.page_records = pm.records_from_page(page_id)?;
        self.record_pos = 0;
        self.statistics.io_operations = self.statistics.io_operations.saturating_add(1);
        Ok(true)
    }

    fn tuple_to_row(tuple: &Tuple, projection: &[String]) -> Row {
        let wildcard = projection.is_empty() || projection.iter().any(|c| c == "*");
        let cap = if wildcard {
            tuple.values.len().saturating_add(1)
        } else {
            projection.len().saturating_add(1)
        };

        // Avoid per-column timestamp updates in the scan hot-path.
        let mut row = Row::with_capacity(cap);
        row.version = tuple.version;
        row.created_at = tuple.created_at;
        row.updated_at = tuple.updated_at;

        if wildcard {
            if !tuple.values.contains_key("id") {
                row.set_value_fast("id", ColumnValue::new(DataType::BigInt(tuple.id as i64)));
            }
            let mut keys: Vec<_> = tuple.values.keys().cloned().collect();
            keys.sort();
            for k in keys {
                if let Some(v) = tuple.values.get(&k) {
                    row.set_value_fast(&k, v.clone());
                }
            }
        } else {
            for p in projection {
                if p == "id" {
                    let cv = tuple
                        .values
                        .get("id")
                        .cloned()
                        .unwrap_or_else(|| ColumnValue::new(DataType::BigInt(tuple.id as i64)));
                    row.set_value_fast("id", cv);
                } else if let Some(v) = tuple.values.get(p) {
                    row.set_value_fast(p, v.clone());
                }
            }
        }
        row
    }

    fn tuple_matches_simple_equality(tuple: &Tuple, eq: &SimpleEqualityFilter) -> bool {
        let col = eq.column.as_str();
        let cv = if col == "id" {
            // Tuple id is implicit; may or may not be stored in tuple.values.
            ColumnValue::new(DataType::BigInt(tuple.id as i64))
        } else {
            let Some(v) = tuple.values.get(col) else {
                return false;
            };
            v.clone()
        };

        match (&eq.literal, &cv.data_type) {
            (Literal::Null, DataType::Null) => true,
            (Literal::Boolean(a), DataType::Boolean(b)) => a == b,
            (Literal::Integer(a), DataType::TinyInt(b)) => *a == *b as i64,
            (Literal::Integer(a), DataType::SmallInt(b)) => *a == *b as i64,
            (Literal::Integer(a), DataType::Integer(b)) => *a == *b as i64,
            (Literal::Integer(a), DataType::BigInt(b)) => *a == *b,
            (Literal::Float(a), DataType::Float(b)) => (*a - *b as f64).abs() < f64::EPSILON,
            (Literal::Float(a), DataType::Double(b)) => (*a - *b).abs() < f64::EPSILON,
            (Literal::String(a), DataType::Char(b))
            | (Literal::String(a), DataType::Varchar(b))
            | (Literal::String(a), DataType::Text(b)) => a == b,
            // Allow comparing string literal to numeric stored as text (best-effort).
            (Literal::Integer(a), DataType::Text(b))
            | (Literal::Integer(a), DataType::Varchar(b))
            | (Literal::Integer(a), DataType::Char(b)) => b == &a.to_string(),
            (Literal::String(a), DataType::Integer(b)) => a == &b.to_string(),
            (Literal::String(a), DataType::BigInt(b)) => a == &b.to_string(),
            _ => false,
        }
    }

    fn apply_filter(&self, row: &Row) -> bool {
        if let Some(condition) = &self.filter_condition {
            let row_string = format!("{:?}", row);
            row_string.contains(condition)
        } else {
            true
        }
    }
}

impl Operator for TableScanOperator {
    fn next(&mut self) -> Result<Option<Row>> {
        let start_time = std::time::Instant::now();
        self.ensure_initialized()?;

        loop {
            // Need a page loaded?
            if self.record_pos >= self.page_records.len() {
                if !self.load_next_page()? {
                    self.statistics.execution_time_ms = start_time.elapsed().as_millis() as u64;
                    return Ok(None);
                }
            }

            while self.record_pos < self.page_records.len() {
                let (_off, data) = &self.page_records[self.record_pos];
                self.record_pos += 1;

                let tuple = match Tuple::from_bytes(data) {
                    Ok(t) => t,
                    Err(_) => continue,
                };
                if tuple.is_deleted {
                    continue;
                }

                if let Some(eq) = self.pushdown_equality.as_ref() {
                    if !Self::tuple_matches_simple_equality(&tuple, eq) {
                        continue;
                    }
                }

                let row = Self::tuple_to_row(&tuple, &self.schema);
                self.statistics.rows_processed += 1;
                if self.apply_filter(&row) {
                    self.statistics.rows_returned += 1;
                    self.statistics.execution_time_ms = start_time.elapsed().as_millis() as u64;
                    return Ok(Some(row));
                }
            }
        }
    }

    fn reset(&mut self) -> Result<()> {
        self.page_ids = None;
        self.page_pos = 0;
        self.page_records.clear();
        self.record_pos = 0;
        self.statistics = OperatorStatistics::default();
        Ok(())
    }

    fn get_schema(&self) -> Result<Vec<String>> {
        if self.schema.iter().any(|c| c == "*") {
            Ok(vec!["*".to_string()])
        } else if !self.schema.is_empty() {
            Ok(self.schema.clone())
        } else {
            Ok(vec!["*".to_string()])
        }
    }

    fn get_statistics(&self) -> OperatorStatistics {
        self.statistics.clone()
    }
}

/// Index scan operator
pub struct IndexScanOperator {
    /// Table name
    table_name: String,
    /// Index name
    index_name: String,
    /// Index for scanning: key = column value, value = list of record IDs
    index: Arc<Mutex<BPlusTree<String, Vec<RecordId>>>>,
    /// Page manager
    page_manager: Arc<Mutex<StoragePageManager>>,
    /// Search conditions
    search_conditions: Vec<IndexCondition>,
    /// Current position in index result
    current_position: usize,
    /// Index search result (record IDs)
    index_result: Vec<RecordId>,
    /// Table schema
    schema: Vec<String>,
    /// Statistics
    statistics: OperatorStatistics,
}

/// Index search condition
#[derive(Debug, Clone)]
pub struct IndexCondition {
    /// Column name
    pub column: String,
    /// Comparison operator
    pub operator: IndexOperator,
    /// Value for comparison
    pub value: String,
}

/// Index comparison operator
#[derive(Debug, Clone)]
pub enum IndexOperator {
    Equal,
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,
    Between,
    In,
}

impl IndexScanOperator {
    /// Create new index scan operator
    pub fn new(
        table_name: String,
        index_name: String,
        index: Arc<Mutex<BPlusTree<String, Vec<RecordId>>>>,
        page_manager: Arc<Mutex<StoragePageManager>>,
        search_conditions: Vec<IndexCondition>,
        schema: Vec<String>,
    ) -> Result<Self> {
        let mut operator = Self {
            table_name,
            index_name,
            index,
            page_manager,
            search_conditions,
            current_position: 0,
            index_result: Vec::new(),
            schema,
            statistics: OperatorStatistics::default(),
        };

        // Perform index search
        operator.perform_index_search()?;

        Ok(operator)
    }

    /// Perform index search using real B+ tree
    fn perform_index_search(&mut self) -> Result<()> {
        let index = self
            .index
            .lock()
            .map_err(|_| Error::internal("Lock poisoned"))?;

        if self.search_conditions.is_empty() {
            // No conditions: range search over all keys (use min/max string bounds)
            let results = index.range_search(&String::new(), &"\u{10FFFF}".to_string())?;
            self.index_result = results.into_iter().flat_map(|(_, ids)| ids).collect();
        } else {
            // Use first condition for index lookup
            let cond = &self.search_conditions[0];
            let key = cond.value.clone();

            match cond.operator {
                IndexOperator::Equal => {
                    if let Some(ids) = index.search(&key)? {
                        self.index_result = ids;
                    }
                }
                IndexOperator::LessThan
                | IndexOperator::LessThanOrEqual
                | IndexOperator::GreaterThan
                | IndexOperator::GreaterThanOrEqual
                | IndexOperator::Between => {
                    let (start, end) = self.range_bounds_from_conditions();
                    let results = index.range_search(&start, &end)?;
                    self.index_result = results.into_iter().flat_map(|(_, ids)| ids).collect();
                }
                IndexOperator::In => {
                    // IN: multiple equality lookups - for simplicity use range
                    let results = index.range_search(&key.clone(), &key)?;
                    self.index_result = results.into_iter().flat_map(|(_, ids)| ids).collect();
                }
            }
        }

        self.statistics.io_operations += 1;
        Ok(())
    }

    fn range_bounds_from_conditions(&self) -> (String, String) {
        let mut start = String::new();
        let mut end = "\u{10FFFF}".to_string();
        for cond in &self.search_conditions {
            match cond.operator {
                IndexOperator::GreaterThan | IndexOperator::GreaterThanOrEqual => {
                    start = cond.value.clone();
                }
                IndexOperator::LessThan | IndexOperator::LessThanOrEqual => {
                    end = cond.value.clone();
                }
                _ => {}
            }
        }
        (start, end)
    }

    /// Load record by ID from PageManager
    fn load_record(&mut self, record_id: RecordId) -> Result<Option<Row>> {
        let mut pm = self
            .page_manager
            .lock()
            .map_err(|_| Error::internal("Lock poisoned"))?;
        let data = pm.get_record(record_id)?;
        self.statistics.io_operations += 1;

        let row = match data {
            Some(bytes) => Self::bytes_to_row(&bytes, &self.schema),
            None => None,
        };
        Ok(row)
    }

    /// Convert record bytes to Row (tries bincode, falls back to simple row)
    fn bytes_to_row(bytes: &[u8], schema: &[String]) -> Option<Row> {
        if let Ok(row) = crate::common::bincode_io::deserialize::<Row>(bytes) {
            return Some(row);
        }
        // Fallback: create row with raw data as single column
        let mut row = Row::new();
        row.set_value(
            "data",
            ColumnValue::new(DataType::Varchar(
                String::from_utf8_lossy(bytes).to_string(),
            )),
        );
        Some(row)
    }

    /// Apply search conditions to row
    fn apply_search_conditions(&self, _row: &Row) -> bool {
        true
    }
}

impl Operator for IndexScanOperator {
    fn next(&mut self) -> Result<Option<Row>> {
        let start_time = std::time::Instant::now();

        while self.current_position < self.index_result.len() {
            let record_id = self.index_result[self.current_position];
            self.current_position += 1;

            if let Some(row) = self.load_record(record_id)? {
                self.statistics.rows_processed += 1;
                if self.apply_search_conditions(&row) {
                    self.statistics.rows_returned += 1;
                    self.statistics.execution_time_ms = start_time.elapsed().as_millis() as u64;
                    return Ok(Some(row));
                }
            }
        }

        self.statistics.execution_time_ms = start_time.elapsed().as_millis() as u64;
        Ok(None)
    }

    fn reset(&mut self) -> Result<()> {
        self.current_position = 0;
        self.statistics = OperatorStatistics::default();
        self.perform_index_search()?;
        Ok(())
    }

    fn get_schema(&self) -> Result<Vec<String>> {
        Ok(self.schema.clone())
    }

    fn get_statistics(&self) -> OperatorStatistics {
        self.statistics.clone()
    }
}

/// Range scan operator
pub struct RangeScanOperator {
    /// Base scan operator
    base_operator: Box<dyn Operator>,
    /// Range start value
    start_value: Option<String>,
    /// Range end value
    end_value: Option<String>,
    /// Statistics
    statistics: OperatorStatistics,
}

impl RangeScanOperator {
    /// Create new range scan operator
    pub fn new(
        base_operator: Box<dyn Operator>,
        start_value: Option<String>,
        end_value: Option<String>,
    ) -> Result<Self> {
        Ok(Self {
            base_operator,
            start_value,
            end_value,
            statistics: OperatorStatistics::default(),
        })
    }

    /// Check if value is in range
    fn is_in_range(&self, value: &str) -> bool {
        if let Some(start) = &self.start_value {
            if value < start.as_str() {
                return false;
            }
        }

        if let Some(end) = &self.end_value {
            if value > end.as_str() {
                return false;
            }
        }

        true
    }
}

impl Operator for RangeScanOperator {
    fn next(&mut self) -> Result<Option<Row>> {
        let start_time = std::time::Instant::now();

        while let Some(row) = self.base_operator.next()? {
            self.statistics.rows_processed += 1;

            // Check if first row value is in range
            // Simplified check - take row version as string value
            let row_value = row.version.to_string();
            if self.is_in_range(&row_value) {
                self.statistics.rows_returned += 1;
                self.statistics.execution_time_ms = start_time.elapsed().as_millis() as u64;
                return Ok(Some(row));
            }
        }

        self.statistics.execution_time_ms = start_time.elapsed().as_millis() as u64;
        Ok(None)
    }

    fn reset(&mut self) -> Result<()> {
        self.base_operator.reset()?;
        self.statistics = OperatorStatistics::default();
        Ok(())
    }

    fn get_schema(&self) -> Result<Vec<String>> {
        self.base_operator.get_schema()
    }

    fn get_statistics(&self) -> OperatorStatistics {
        self.statistics.clone()
    }
}

/// Conditional scan operator
pub struct ConditionalScanOperator {
    /// Base scan operator
    base_operator: Box<dyn Operator>,
    /// Filter condition
    condition: String,
    /// Full predicate expression (preferred over string matching).
    predicate: Option<Expression>,
    /// Structured equality when available (SELECT WHERE column = literal)
    equality: Option<SimpleEqualityFilter>,
    /// Statistics
    statistics: OperatorStatistics,
}

#[derive(Debug, Clone, PartialEq)]
enum EvalValue {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
}

fn column_value_to_eval(cv: &ColumnValue) -> EvalValue {
    if cv.is_null {
        return EvalValue::Null;
    }
    match &cv.data_type {
        DataType::Null => EvalValue::Null,
        DataType::Boolean(b) => EvalValue::Bool(*b),
        DataType::TinyInt(n) => EvalValue::Int(*n as i64),
        DataType::SmallInt(n) => EvalValue::Int(*n as i64),
        DataType::Integer(n) => EvalValue::Int(*n as i64),
        DataType::BigInt(n) => EvalValue::Int(*n),
        DataType::Float(f) => EvalValue::Float(*f as f64),
        DataType::Double(f) => EvalValue::Float(*f),
        DataType::Char(s)
        | DataType::Varchar(s)
        | DataType::Text(s)
        | DataType::Date(s)
        | DataType::Time(s)
        | DataType::Timestamp(s) => EvalValue::String(s.clone()),
        DataType::Blob(b) => EvalValue::String(format!("{:?}", b)),
    }
}

fn literal_to_eval(l: &Literal) -> EvalValue {
    match l {
        Literal::Null => EvalValue::Null,
        Literal::Boolean(b) => EvalValue::Bool(*b),
        Literal::Integer(n) => EvalValue::Int(*n),
        Literal::Float(f) => EvalValue::Float(*f),
        Literal::String(s) => EvalValue::String(s.clone()),
    }
}

fn eval_expression(row: &Row, expr: &Expression) -> EvalValue {
    match expr {
        Expression::Literal(l) => literal_to_eval(l),
        Expression::Identifier(name) => row
            .values
            .get(name)
            .map(column_value_to_eval)
            .unwrap_or(EvalValue::Null),
        Expression::QualifiedIdentifier { column, .. } => row
            .values
            .get(column)
            .map(column_value_to_eval)
            .unwrap_or(EvalValue::Null),
        Expression::Function { name, args } => {
            let n = name.to_uppercase();
            let arg = args
                .first()
                .map(|e| match e {
                    Expression::Identifier(s) => s.clone(),
                    Expression::QualifiedIdentifier { column, .. } => column.clone(),
                    _ => "*".to_string(),
                })
                .unwrap_or_else(|| "*".to_string());
            let key = format!("{n}({arg})");
            row.values
                .get(&key)
                .map(column_value_to_eval)
                .unwrap_or(EvalValue::Null)
        }
        Expression::UnaryOp {
            op: UnaryOperator::Not,
            expr,
        } => match eval_expression(row, expr) {
            EvalValue::Bool(b) => EvalValue::Bool(!b),
            _ => EvalValue::Null,
        },
        Expression::BinaryOp { left, op, right } => {
            let lv = eval_expression(row, left);
            let rv = eval_expression(row, right);
            match op {
                BinaryOperator::And => match (lv, rv) {
                    (EvalValue::Bool(a), EvalValue::Bool(b)) => EvalValue::Bool(a && b),
                    _ => EvalValue::Null,
                },
                BinaryOperator::Or => match (lv, rv) {
                    (EvalValue::Bool(a), EvalValue::Bool(b)) => EvalValue::Bool(a || b),
                    _ => EvalValue::Null,
                },
                BinaryOperator::Add
                | BinaryOperator::Subtract
                | BinaryOperator::Multiply
                | BinaryOperator::Divide
                | BinaryOperator::Modulo => {
                    let (a, b) = match (lv, rv) {
                        (EvalValue::Int(a), EvalValue::Int(b)) => (a as f64, b as f64),
                        (EvalValue::Int(a), EvalValue::Float(b)) => (a as f64, b),
                        (EvalValue::Float(a), EvalValue::Int(b)) => (a, b as f64),
                        (EvalValue::Float(a), EvalValue::Float(b)) => (a, b),
                        _ => return EvalValue::Null,
                    };
                    let out = match op {
                        BinaryOperator::Add => a + b,
                        BinaryOperator::Subtract => a - b,
                        BinaryOperator::Multiply => a * b,
                        BinaryOperator::Divide => a / b,
                        BinaryOperator::Modulo => a % b,
                        _ => unreachable!(),
                    };
                    EvalValue::Float(out)
                }
                BinaryOperator::Equal
                | BinaryOperator::NotEqual
                | BinaryOperator::LessThan
                | BinaryOperator::LessThanOrEqual
                | BinaryOperator::GreaterThan
                | BinaryOperator::GreaterThanOrEqual => {
                    use std::cmp::Ordering;
                    let ord = match (&lv, &rv) {
                        (EvalValue::Null, _) | (_, EvalValue::Null) => None,
                        (EvalValue::Bool(a), EvalValue::Bool(b)) => Some(a.cmp(b)),
                        (EvalValue::Int(a), EvalValue::Int(b)) => Some(a.cmp(b)),
                        (EvalValue::Float(a), EvalValue::Float(b)) => a.partial_cmp(b),
                        (EvalValue::Int(a), EvalValue::Float(b)) => (*a as f64).partial_cmp(b),
                        (EvalValue::Float(a), EvalValue::Int(b)) => a.partial_cmp(&(*b as f64)),
                        (EvalValue::String(a), EvalValue::String(b)) => Some(a.cmp(b)),
                        _ => None,
                    };
                    let Some(ord) = ord else {
                        return EvalValue::Null;
                    };
                    let res = match op {
                        BinaryOperator::Equal => ord == Ordering::Equal,
                        BinaryOperator::NotEqual => ord != Ordering::Equal,
                        BinaryOperator::LessThan => ord == Ordering::Less,
                        BinaryOperator::LessThanOrEqual => {
                            ord == Ordering::Less || ord == Ordering::Equal
                        }
                        BinaryOperator::GreaterThan => ord == Ordering::Greater,
                        BinaryOperator::GreaterThanOrEqual => {
                            ord == Ordering::Greater || ord == Ordering::Equal
                        }
                        _ => false,
                    };
                    EvalValue::Bool(res)
                }
                _ => EvalValue::Null,
            }
        }
        _ => EvalValue::Null,
    }
}

fn eval_predicate(row: &Row, expr: &Expression) -> bool {
    matches!(eval_expression(row, expr), EvalValue::Bool(true))
}

fn filter_literal_to_column_value(l: &Literal) -> ColumnValue {
    let dt = match l {
        Literal::Null => DataType::Null,
        Literal::Boolean(b) => DataType::Boolean(*b),
        Literal::Integer(n) => {
            if *n >= i32::MIN as i64 && *n <= i32::MAX as i64 {
                DataType::Integer(*n as i32)
            } else {
                DataType::BigInt(*n)
            }
        }
        Literal::Float(f) => DataType::Double(*f),
        Literal::String(s) => DataType::Varchar(s.clone()),
    };
    ColumnValue::new(dt)
}

impl ConditionalScanOperator {
    /// Create new conditional scan operator
    pub fn new(
        base_operator: Box<dyn Operator>,
        condition: String,
        predicate: Option<Expression>,
        equality: Option<SimpleEqualityFilter>,
    ) -> Result<Self> {
        Ok(Self {
            base_operator,
            condition,
            predicate,
            equality,
            statistics: OperatorStatistics::default(),
        })
    }

    /// Evaluate condition for row
    fn evaluate_condition(&self, row: &Row) -> bool {
        if let Some(ref p) = self.predicate {
            return eval_predicate(row, p);
        }
        if let Some(ref eq) = self.equality {
            let expected = filter_literal_to_column_value(&eq.literal);
            let Some(cv) = row.values.get(&eq.column) else {
                return false;
            };
            return cv.data_type == expected.data_type && cv.is_null == expected.is_null;
        }
        let row_string = format!("{:?}", row);
        row_string.contains(&self.condition)
    }
}

impl Operator for ConditionalScanOperator {
    fn next(&mut self) -> Result<Option<Row>> {
        let start_time = std::time::Instant::now();

        while let Some(row) = self.base_operator.next()? {
            self.statistics.rows_processed += 1;

            if self.evaluate_condition(&row) {
                self.statistics.rows_returned += 1;
                self.statistics.execution_time_ms = start_time.elapsed().as_millis() as u64;
                return Ok(Some(row));
            }
        }

        self.statistics.execution_time_ms = start_time.elapsed().as_millis() as u64;
        Ok(None)
    }

    fn reset(&mut self) -> Result<()> {
        self.base_operator.reset()?;
        self.statistics = OperatorStatistics::default();
        Ok(())
    }

    fn get_schema(&self) -> Result<Vec<String>> {
        self.base_operator.get_schema()
    }

    fn get_statistics(&self) -> OperatorStatistics {
        self.statistics.clone()
    }
}

/// Join type
#[derive(Debug, Clone)]
pub enum JoinType {
    /// INNER JOIN
    Inner,
    /// LEFT OUTER JOIN
    LeftOuter,
    /// RIGHT OUTER JOIN
    RightOuter,
    /// FULL OUTER JOIN
    FullOuter,
}

/// Join condition
#[derive(Debug, Clone)]
pub struct JoinCondition {
    /// Left column
    pub left_column: String,
    /// Right column
    pub right_column: String,
    /// Comparison operator
    pub operator: JoinOperator,
}

/// Join comparison operator
#[derive(Debug, Clone)]
pub enum JoinOperator {
    Equal,
    NotEqual,
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,
}

/// Nested Loop Join operator
pub struct NestedLoopJoinOperator {
    /// Left input operator
    left_input: Box<dyn Operator>,
    /// Right input operator
    right_input: Box<dyn Operator>,
    /// Join condition
    join_condition: JoinCondition,
    /// Join type
    join_type: JoinType,
    /// Current row from left input
    current_left_row: Option<Row>,
    /// Current position in right input
    current_right_position: usize,
    /// Buffer for right input
    right_buffer: Vec<Row>,
    /// Result schema
    schema: Vec<String>,
    /// Statistics
    statistics: OperatorStatistics,
    /// Block size for block nested loop
    block_size: usize,
}

impl NestedLoopJoinOperator {
    /// Create new Nested Loop Join operator
    pub fn new(
        left_input: Box<dyn Operator>,
        right_input: Box<dyn Operator>,
        join_condition: JoinCondition,
        join_type: JoinType,
        block_size: usize,
    ) -> Result<Self> {
        let mut left_schema = left_input.get_schema()?;
        let right_schema = right_input.get_schema()?;

        // Combine schemas
        left_schema.extend(right_schema);

        Ok(Self {
            left_input,
            right_input,
            join_condition,
            join_type,
            current_left_row: None,
            current_right_position: 0,
            right_buffer: Vec::new(),
            schema: left_schema,
            statistics: OperatorStatistics::default(),
            block_size,
        })
    }

    /// Load block of rows from right input
    fn load_right_block(&mut self) -> Result<()> {
        self.right_buffer.clear();
        self.current_right_position = 0;

        // Load block of rows
        for _ in 0..self.block_size {
            if let Some(row) = self.right_input.next()? {
                self.right_buffer.push(row);
            } else {
                break;
            }
        }

        self.statistics.memory_operations += 1;
        Ok(())
    }

    /// Check join condition
    fn check_join_condition(&self, left_row: &Row, right_row: &Row) -> bool {
        let left_value = left_row.get_value(&self.join_condition.left_column);
        let right_value = right_row.get_value(&self.join_condition.right_column);

        match (left_value, right_value) {
            (Some(left), Some(right)) => match self.join_condition.operator {
                JoinOperator::Equal => left == right,
                JoinOperator::NotEqual => left != right,
                JoinOperator::LessThan => {
                    self.compare_values(left, right) == std::cmp::Ordering::Less
                }
                JoinOperator::LessThanOrEqual => {
                    self.compare_values(left, right) != std::cmp::Ordering::Greater
                }
                JoinOperator::GreaterThan => {
                    self.compare_values(left, right) == std::cmp::Ordering::Greater
                }
                JoinOperator::GreaterThanOrEqual => {
                    self.compare_values(left, right) != std::cmp::Ordering::Less
                }
            },
            _ => false,
        }
    }

    /// Compare column values
    fn compare_values(&self, left: &ColumnValue, right: &ColumnValue) -> std::cmp::Ordering {
        // Simplified comparison - compare string representations
        let left_str = format!("{:?}", left);
        let right_str = format!("{:?}", right);
        left_str.cmp(&right_str)
    }

    /// Combine rows
    fn combine_rows(&self, left_row: &Row, right_row: &Row) -> Row {
        let mut combined_row = Row::new();

        // Copy values from left row
        for (column, value) in &left_row.values {
            combined_row.set_value(column, value.clone());
        }

        // Copy values from right row
        for (column, value) in &right_row.values {
            combined_row.set_value(column, value.clone());
        }

        combined_row
    }
}

impl Operator for NestedLoopJoinOperator {
    fn next(&mut self) -> Result<Option<Row>> {
        let start_time = std::time::Instant::now();

        loop {
            // If we don't have current row from left input, get next one
            if self.current_left_row.is_none() {
                self.current_left_row = self.left_input.next()?;
                if self.current_left_row.is_none() {
                    // No more rows in left input
                    break;
                }

                // Reset right input for new row from left input
                self.right_input.reset()?;
                self.load_right_block()?;
            }

            let left_row = self.current_left_row.as_ref().unwrap();
            self.statistics.rows_processed += 1;

            // Check rows in current block of right input
            while self.current_right_position < self.right_buffer.len() {
                let right_row = &self.right_buffer[self.current_right_position];
                self.current_right_position += 1;

                if self.check_join_condition(left_row, right_row) {
                    let combined_row = self.combine_rows(left_row, right_row);
                    self.statistics.rows_returned += 1;
                    self.statistics.execution_time_ms = start_time.elapsed().as_millis() as u64;
                    return Ok(Some(combined_row));
                }
            }

            // If block ended, load next one
            if self.current_right_position >= self.right_buffer.len() {
                self.load_right_block()?;

                // If no more rows in right input, move to next row from left
                if self.right_buffer.is_empty() {
                    self.current_left_row = None;
                }
            }
        }

        self.statistics.execution_time_ms = start_time.elapsed().as_millis() as u64;
        Ok(None)
    }

    fn reset(&mut self) -> Result<()> {
        self.left_input.reset()?;
        self.right_input.reset()?;
        self.current_left_row = None;
        self.current_right_position = 0;
        self.right_buffer.clear();
        self.statistics = OperatorStatistics::default();
        Ok(())
    }

    fn get_schema(&self) -> Result<Vec<String>> {
        Ok(self.schema.clone())
    }

    fn get_statistics(&self) -> OperatorStatistics {
        self.statistics.clone()
    }
}

/// Hash Join operator
pub struct HashJoinOperator {
    /// Left input operator
    left_input: Box<dyn Operator>,
    /// Right input operator
    right_input: Box<dyn Operator>,
    /// Join condition
    join_condition: JoinCondition,
    /// Join type
    join_type: JoinType,
    /// Hash table for right input
    hash_table: HashMap<String, Vec<Row>>,
    /// Current row from left input
    current_left_row: Option<Row>,
    /// Current position in match list
    current_match_position: usize,
    /// Current match list
    current_matches: Vec<Row>,
    /// Result schema
    schema: Vec<String>,
    /// Statistics
    statistics: OperatorStatistics,
    /// Hash table size
    hash_table_size: usize,
}

impl HashJoinOperator {
    /// Create new Hash Join operator
    pub fn new(
        left_input: Box<dyn Operator>,
        right_input: Box<dyn Operator>,
        join_condition: JoinCondition,
        join_type: JoinType,
        hash_table_size: usize,
    ) -> Result<Self> {
        let mut left_schema = left_input.get_schema()?;
        let right_schema = right_input.get_schema()?;

        // Combine schemas
        left_schema.extend(right_schema);

        let mut operator = Self {
            left_input,
            right_input,
            join_condition,
            join_type,
            hash_table: HashMap::new(),
            current_left_row: None,
            current_match_position: 0,
            current_matches: Vec::new(),
            schema: left_schema,
            statistics: OperatorStatistics::default(),
            hash_table_size,
        };

        // Build hash table
        operator.build_hash_table()?;

        Ok(operator)
    }

    /// Build hash table from right input
    fn build_hash_table(&mut self) -> Result<()> {
        self.hash_table.clear();

        // Scan right input and build hash table
        while let Some(row) = self.right_input.next()? {
            let key = self.get_join_key(&row, &self.join_condition.right_column);
            self.hash_table
                .entry(key)
                .or_insert_with(Vec::new)
                .push(row);
        }

        self.statistics.memory_operations += 1;
        Ok(())
    }

    /// Get join key
    fn get_join_key(&self, row: &Row, column: &str) -> String {
        if let Some(value) = row.get_value(column) {
            format!("{:?}", value)
        } else {
            "NULL".to_string()
        }
    }

    /// Check join condition
    fn check_join_condition(&self, left_row: &Row, right_row: &Row) -> bool {
        let left_value = left_row.get_value(&self.join_condition.left_column);
        let right_value = right_row.get_value(&self.join_condition.right_column);

        match (left_value, right_value) {
            (Some(left), Some(right)) => match self.join_condition.operator {
                JoinOperator::Equal => left == right,
                JoinOperator::NotEqual => left != right,
                JoinOperator::LessThan => {
                    self.compare_values(left, right) == std::cmp::Ordering::Less
                }
                JoinOperator::LessThanOrEqual => {
                    self.compare_values(left, right) != std::cmp::Ordering::Greater
                }
                JoinOperator::GreaterThan => {
                    self.compare_values(left, right) == std::cmp::Ordering::Greater
                }
                JoinOperator::GreaterThanOrEqual => {
                    self.compare_values(left, right) != std::cmp::Ordering::Less
                }
            },
            _ => false,
        }
    }

    /// Compare column values
    fn compare_values(&self, left: &ColumnValue, right: &ColumnValue) -> std::cmp::Ordering {
        // Simplified comparison - compare string representations
        let left_str = format!("{:?}", left);
        let right_str = format!("{:?}", right);
        left_str.cmp(&right_str)
    }

    /// Combine rows
    fn combine_rows(&self, left_row: &Row, right_row: &Row) -> Row {
        let mut combined_row = Row::new();

        // Copy values from left row
        for (column, value) in &left_row.values {
            combined_row.set_value(column, value.clone());
        }

        // Copy values from right row
        for (column, value) in &right_row.values {
            combined_row.set_value(column, value.clone());
        }

        combined_row
    }
}

impl Operator for HashJoinOperator {
    fn next(&mut self) -> Result<Option<Row>> {
        let start_time = std::time::Instant::now();

        loop {
            // If we don't have current row from left input, get next one
            if self.current_left_row.is_none() {
                self.current_left_row = self.left_input.next()?;
                if self.current_left_row.is_none() {
                    // No more rows in left input
                    break;
                }

                // Find matches in hash table
                let left_row = self.current_left_row.as_ref().unwrap();
                let key = self.get_join_key(left_row, &self.join_condition.left_column);

                self.current_matches = self.hash_table.get(&key).cloned().unwrap_or_default();
                self.current_match_position = 0;
            }

            let left_row = self.current_left_row.as_ref().unwrap();
            self.statistics.rows_processed += 1;

            // Check matches
            while self.current_match_position < self.current_matches.len() {
                let right_row = &self.current_matches[self.current_match_position];
                self.current_match_position += 1;

                if self.check_join_condition(left_row, right_row) {
                    let combined_row = self.combine_rows(left_row, right_row);
                    self.statistics.rows_returned += 1;
                    self.statistics.execution_time_ms = start_time.elapsed().as_millis() as u64;
                    return Ok(Some(combined_row));
                }
            }

            // If matches ended, move to next row from left input
            self.current_left_row = None;
        }

        self.statistics.execution_time_ms = start_time.elapsed().as_millis() as u64;
        Ok(None)
    }

    fn reset(&mut self) -> Result<()> {
        self.left_input.reset()?;
        self.right_input.reset()?;
        self.current_left_row = None;
        self.current_match_position = 0;
        self.current_matches.clear();
        self.statistics = OperatorStatistics::default();
        self.build_hash_table()?;
        Ok(())
    }

    fn get_schema(&self) -> Result<Vec<String>> {
        Ok(self.schema.clone())
    }

    fn get_statistics(&self) -> OperatorStatistics {
        self.statistics.clone()
    }
}

/// Merge Join operator
pub struct MergeJoinOperator {
    /// Left input operator
    left_input: Box<dyn Operator>,
    /// Right input operator
    right_input: Box<dyn Operator>,
    /// Join condition
    join_condition: JoinCondition,
    /// Join type
    join_type: JoinType,
    /// Current row from left input
    current_left_row: Option<Row>,
    /// Current row from right input
    current_right_row: Option<Row>,
    /// Buffer for rows with same keys
    left_buffer: Vec<Row>,
    right_buffer: Vec<Row>,
    /// Positions in buffers
    left_buffer_pos: usize,
    right_buffer_pos: usize,
    /// Result schema
    schema: Vec<String>,
    /// Statistics
    statistics: OperatorStatistics,
}

impl MergeJoinOperator {
    /// Create new Merge Join operator
    pub fn new(
        left_input: Box<dyn Operator>,
        right_input: Box<dyn Operator>,
        join_condition: JoinCondition,
        join_type: JoinType,
    ) -> Result<Self> {
        let mut left_schema = left_input.get_schema()?;
        let right_schema = right_input.get_schema()?;

        // Combine schemas
        left_schema.extend(right_schema);

        Ok(Self {
            left_input,
            right_input,
            join_condition,
            join_type,
            current_left_row: None,
            current_right_row: None,
            left_buffer: Vec::new(),
            right_buffer: Vec::new(),
            left_buffer_pos: 0,
            right_buffer_pos: 0,
            schema: left_schema,
            statistics: OperatorStatistics::default(),
        })
    }

    /// Get join key
    fn get_join_key(&self, row: &Row, column: &str) -> String {
        if let Some(value) = row.get_value(column) {
            format!("{:?}", value)
        } else {
            "NULL".to_string()
        }
    }

    /// Compare join keys
    fn compare_keys(&self, left_key: &str, right_key: &str) -> std::cmp::Ordering {
        left_key.cmp(right_key)
    }

    /// Load rows with same keys into buffers
    fn load_matching_keys(&mut self) -> Result<()> {
        self.left_buffer.clear();
        self.right_buffer.clear();
        self.left_buffer_pos = 0;
        self.right_buffer_pos = 0;

        // Get current rows
        if self.current_left_row.is_none() {
            self.current_left_row = self.left_input.next()?;
        }
        if self.current_right_row.is_none() {
            self.current_right_row = self.right_input.next()?;
        }

        if let (Some(left_row), Some(right_row)) = (&self.current_left_row, &self.current_right_row)
        {
            let left_key = self.get_join_key(left_row, &self.join_condition.left_column);
            let right_key = self.get_join_key(right_row, &self.join_condition.right_column);

            match self.compare_keys(&left_key, &right_key) {
                std::cmp::Ordering::Equal => {
                    // Load all rows with same keys
                    let target_key = left_key.clone();

                    // Load rows from left input
                    while let Some(row) = &self.current_left_row {
                        let key = self.get_join_key(row, &self.join_condition.left_column);
                        if key == target_key {
                            self.left_buffer.push(row.clone());
                            self.current_left_row = self.left_input.next()?;
                        } else {
                            break;
                        }
                    }

                    // Load rows from right input
                    while let Some(row) = &self.current_right_row {
                        let key = self.get_join_key(row, &self.join_condition.right_column);
                        if key == target_key {
                            self.right_buffer.push(row.clone());
                            self.current_right_row = self.right_input.next()?;
                        } else {
                            break;
                        }
                    }
                }
                std::cmp::Ordering::Less => {
                    // Left row is less, move to next left row
                    self.current_left_row = self.left_input.next()?;
                }
                std::cmp::Ordering::Greater => {
                    // Right row is less, move to next right row
                    self.current_right_row = self.right_input.next()?;
                }
            }
        }

        Ok(())
    }

    /// Combine rows
    fn combine_rows(&self, left_row: &Row, right_row: &Row) -> Row {
        let mut combined_row = Row::new();

        // Copy values from left row
        for (column, value) in &left_row.values {
            combined_row.set_value(column, value.clone());
        }

        // Copy values from right row
        for (column, value) in &right_row.values {
            combined_row.set_value(column, value.clone());
        }

        combined_row
    }
}

impl Operator for MergeJoinOperator {
    fn next(&mut self) -> Result<Option<Row>> {
        let start_time = std::time::Instant::now();

        loop {
            // If buffers are empty, load new matching keys
            if self.left_buffer_pos >= self.left_buffer.len()
                || self.right_buffer_pos >= self.right_buffer.len()
            {
                self.load_matching_keys()?;

                // If no more data, finish
                if self.left_buffer.is_empty() || self.right_buffer.is_empty() {
                    if self.current_left_row.is_none() && self.current_right_row.is_none() {
                        break;
                    }
                    continue;
                }
            }

            // Return next combination of rows
            let left_row = &self.left_buffer[self.left_buffer_pos];
            let right_row = &self.right_buffer[self.right_buffer_pos];

            self.statistics.rows_processed += 1;

            let combined_row = self.combine_rows(left_row, right_row);
            self.statistics.rows_returned += 1;
            self.statistics.execution_time_ms = start_time.elapsed().as_millis() as u64;

            // Move to next combination
            self.right_buffer_pos += 1;
            if self.right_buffer_pos >= self.right_buffer.len() {
                self.right_buffer_pos = 0;
                self.left_buffer_pos += 1;
            }

            return Ok(Some(combined_row));
        }

        self.statistics.execution_time_ms = start_time.elapsed().as_millis() as u64;
        Ok(None)
    }

    fn reset(&mut self) -> Result<()> {
        self.left_input.reset()?;
        self.right_input.reset()?;
        self.current_left_row = None;
        self.current_right_row = None;
        self.left_buffer.clear();
        self.right_buffer.clear();
        self.left_buffer_pos = 0;
        self.right_buffer_pos = 0;
        self.statistics = OperatorStatistics::default();
        Ok(())
    }

    fn get_schema(&self) -> Result<Vec<String>> {
        Ok(self.schema.clone())
    }

    fn get_statistics(&self) -> OperatorStatistics {
        self.statistics.clone()
    }
}

/// Limit operator - returns at most N rows
pub struct LimitOperator {
    input: Box<dyn Operator>,
    limit: usize,
    returned: usize,
    statistics: OperatorStatistics,
}

impl LimitOperator {
    pub fn new(input: Box<dyn Operator>, limit: usize) -> Result<Self> {
        Ok(Self {
            input,
            limit,
            returned: 0,
            statistics: OperatorStatistics::default(),
        })
    }
}

impl Operator for LimitOperator {
    fn next(&mut self) -> Result<Option<Row>> {
        if self.returned >= self.limit {
            return Ok(None);
        }
        let row = self.input.next()?;
        if row.is_some() {
            self.returned += 1;
            self.statistics.rows_returned += 1;
        }
        self.statistics.rows_processed += 1;
        Ok(row)
    }

    fn reset(&mut self) -> Result<()> {
        self.input.reset()?;
        self.returned = 0;
        self.statistics = OperatorStatistics::default();
        Ok(())
    }

    fn get_schema(&self) -> Result<Vec<String>> {
        self.input.get_schema()
    }

    fn get_statistics(&self) -> OperatorStatistics {
        self.statistics.clone()
    }
}

/// Offset operator - skips first N rows
pub struct OffsetOperator {
    input: Box<dyn Operator>,
    offset: usize,
    skipped: usize,
    statistics: OperatorStatistics,
}

impl OffsetOperator {
    pub fn new(input: Box<dyn Operator>, offset: usize) -> Result<Self> {
        Ok(Self {
            input,
            offset,
            skipped: 0,
            statistics: OperatorStatistics::default(),
        })
    }
}

impl Operator for OffsetOperator {
    fn next(&mut self) -> Result<Option<Row>> {
        while self.skipped < self.offset {
            if self.input.next()?.is_none() {
                return Ok(None);
            }
            self.skipped += 1;
            self.statistics.rows_processed += 1;
        }
        let row = self.input.next()?;
        if row.is_some() {
            self.statistics.rows_returned += 1;
        }
        self.statistics.rows_processed += 1;
        Ok(row)
    }

    fn reset(&mut self) -> Result<()> {
        self.input.reset()?;
        self.skipped = 0;
        self.statistics = OperatorStatistics::default();
        Ok(())
    }

    fn get_schema(&self) -> Result<Vec<String>> {
        self.input.get_schema()
    }

    fn get_statistics(&self) -> OperatorStatistics {
        self.statistics.clone()
    }
}

/// Factory for creating scan operators
pub struct ScanOperatorFactory {
    /// Page manager
    default_page_manager: Arc<Mutex<StoragePageManager>>,
    table_page_managers: Arc<Mutex<HashMap<String, Arc<Mutex<StoragePageManager>>>>>,
    /// Indexes: (table_name, index_name) -> B+ tree
    indexes: HashMap<(String, String), Arc<Mutex<BPlusTree<String, Vec<RecordId>>>>>,
    /// When set, a table name not yet in `table_page_managers` opens `<data_dir>/<table>.tbl`
    /// using the same per-table heap files as [`SqlEngine`](crate::network::SqlEngine) for DML. This
    /// lets `SELECT` in a new process see rows inserted into a named heap file earlier.
    data_dir: Option<PathBuf>,
    pm_config: PageManagerConfig,
}

impl ScanOperatorFactory {
    /// Create new scan operator factory
    pub fn new(page_manager: Arc<Mutex<StoragePageManager>>) -> Self {
        Self {
            default_page_manager: page_manager,
            table_page_managers: Arc::new(Mutex::new(HashMap::new())),
            indexes: HashMap::new(),
            data_dir: None,
            pm_config: PageManagerConfig::default(),
        }
    }

    pub fn with_tables(
        default_page_manager: Arc<Mutex<StoragePageManager>>,
        table_page_managers: Arc<Mutex<HashMap<String, Arc<Mutex<StoragePageManager>>>>>,
        data_dir: PathBuf,
    ) -> Self {
        Self {
            default_page_manager,
            table_page_managers,
            indexes: HashMap::new(),
            data_dir: Some(data_dir),
            pm_config: PageManagerConfig::default(),
        }
    }

    fn page_manager_for_table(&self, table_name: &str) -> Result<Arc<Mutex<StoragePageManager>>> {
        let mut g = self
            .table_page_managers
            .lock()
            .map_err(|_| Error::lock("table page managers poisoned"))?;
        if let Some(pm) = g.get(table_name) {
            return Ok(pm.clone());
        }
        if let Some(ref dir) = self.data_dir {
            let pm = match StoragePageManager::open(dir.clone(), table_name, self.pm_config.clone())
            {
                Ok(pm) => Arc::new(Mutex::new(pm)),
                Err(_) => Arc::new(Mutex::new(StoragePageManager::new(
                    dir.clone(),
                    table_name,
                    self.pm_config.clone(),
                )?)),
            };
            g.insert(table_name.to_string(), pm.clone());
            Ok(pm)
        } else {
            Ok(self.default_page_manager.clone())
        }
    }

    pub fn register_table_page_manager(
        &self,
        table_name: &str,
        pm: Arc<Mutex<StoragePageManager>>,
    ) -> Result<()> {
        let mut g = self
            .table_page_managers
            .lock()
            .map_err(|_| Error::lock("table page managers poisoned"))?;
        g.insert(table_name.to_string(), pm);
        Ok(())
    }

    /// Add index for table
    pub fn add_index(
        &mut self,
        table_name: &str,
        index_name: &str,
        index: Arc<Mutex<BPlusTree<String, Vec<RecordId>>>>,
    ) {
        self.indexes
            .insert((table_name.to_string(), index_name.to_string()), index);
    }

    /// Create table scan operator
    pub fn create_table_scan(
        &self,
        table_name: String,
        filter: Option<String>,
        pushdown_equality: Option<SimpleEqualityFilter>,
        schema: Vec<String>,
    ) -> Result<Box<dyn Operator>> {
        let pm = self.page_manager_for_table(&table_name)?;
        let operator = TableScanOperator::new(table_name, pm, filter, pushdown_equality, schema)?;
        Ok(Box::new(operator))
    }

    /// Create range scan operator
    pub fn create_range_scan(
        &self,
        base_operator: Box<dyn Operator>,
        start_value: Option<String>,
        end_value: Option<String>,
    ) -> Result<Box<dyn Operator>> {
        let operator = RangeScanOperator::new(base_operator, start_value, end_value)?;
        Ok(Box::new(operator))
    }

    /// Create conditional scan operator
    pub fn create_conditional_scan(
        &self,
        base_operator: Box<dyn Operator>,
        condition: String,
    ) -> Result<Box<dyn Operator>> {
        let operator = ConditionalScanOperator::new(base_operator, condition, None, None)?;
        Ok(Box::new(operator))
    }

    /// Create index scan operator
    pub fn create_index_scan(
        &self,
        table_name: String,
        index_name: String,
        search_conditions: Vec<IndexCondition>,
        schema: Vec<String>,
    ) -> Result<Box<dyn Operator>> {
        let key = (table_name.clone(), index_name.clone());
        if let Some(index) = self.indexes.get(&key) {
            let operator = IndexScanOperator::new(
                table_name,
                index_name,
                index.clone(),
                self.default_page_manager.clone(),
                search_conditions,
                schema,
            )?;
            Ok(Box::new(operator))
        } else {
            Err(Error::query_execution("Index not found for table"))
        }
    }
}

/// Aggregate function type
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum AggregateFunction {
    Count,
    Sum,
    Avg,
    Min,
    Max,
    CountDistinct,
}

/// Aggregate group
#[derive(Debug, Clone)]
pub struct AggregateGroup {
    /// Grouping keys
    pub keys: Vec<ColumnValue>,
    /// Aggregate function values
    pub aggregates: Vec<ColumnValue>,
    /// Number of rows in group
    pub count: usize,
}

/// Simplified grouping operator (demonstration version)
pub struct HashGroupByOperator {
    /// Input operator
    input: Box<dyn Operator>,
    /// Grouping keys (column indices)
    group_keys: Vec<usize>,
    /// Aggregate functions
    aggregate_functions: Vec<(AggregateFunction, usize)>,
    /// Result schema
    result_schema: Vec<String>,
    /// Statistics
    statistics: OperatorStatistics,
    /// Processed results
    results: Vec<Row>,
    /// Current index
    current_index: usize,
}

impl HashGroupByOperator {
    /// Create new grouping operator
    pub fn new(
        input: Box<dyn Operator>,
        group_keys: Vec<usize>,
        aggregate_functions: Vec<(AggregateFunction, usize)>,
        result_schema: Vec<String>,
    ) -> Result<Self> {
        Ok(Self {
            input,
            group_keys,
            aggregate_functions,
            result_schema,
            statistics: OperatorStatistics::default(),
            results: Vec::new(),
            current_index: 0,
        })
    }

    /// Process input data (simplified version)
    fn process_input(&mut self) -> Result<()> {
        // Read all rows from input operator
        while let Some(_row) = self.input.next()? {
            self.statistics.rows_processed += 1;

            // Create simplified grouping result
            let mut result_tuple = Tuple::new(self.results.len() as u64);

            // Add grouping keys (use first few columns)
            for (i, &key_index) in self.group_keys.iter().enumerate() {
                if key_index < 4 {
                    // Limit for demonstration
                    result_tuple.set_value(
                        &format!("key_{}", i),
                        ColumnValue::new(DataType::Integer(i as i32)),
                    );
                }
            }

            // Add aggregate functions (demonstration values)
            for (i, (function, _)) in self.aggregate_functions.iter().enumerate() {
                match function {
                    AggregateFunction::Count => {
                        result_tuple.set_value(
                            &format!("count_{}", i),
                            ColumnValue::new(DataType::BigInt(1)),
                        );
                    }
                    AggregateFunction::Sum => {
                        result_tuple.set_value(
                            &format!("sum_{}", i),
                            ColumnValue::new(DataType::Double(100.0)),
                        );
                    }
                    AggregateFunction::Avg => {
                        result_tuple.set_value(
                            &format!("avg_{}", i),
                            ColumnValue::new(DataType::Double(50.0)),
                        );
                    }
                    AggregateFunction::Min => {
                        result_tuple.set_value(
                            &format!("min_{}", i),
                            ColumnValue::new(DataType::Integer(10)),
                        );
                    }
                    AggregateFunction::Max => {
                        result_tuple.set_value(
                            &format!("max_{}", i),
                            ColumnValue::new(DataType::Integer(100)),
                        );
                    }
                    AggregateFunction::CountDistinct => {
                        result_tuple.set_value(
                            &format!("count_distinct_{}", i),
                            ColumnValue::new(DataType::BigInt(5)),
                        );
                    }
                }
            }

            let result_row = Row::new();
            self.results.push(result_row);
        }

        Ok(())
    }
}

impl Operator for HashGroupByOperator {
    fn next(&mut self) -> Result<Option<Row>> {
        // If this is first call, process input data
        if self.results.is_empty() && self.current_index == 0 {
            self.process_input()?;
        }

        // Return next group
        if self.current_index < self.results.len() {
            let row = self.results[self.current_index].clone();
            self.current_index += 1;
            self.statistics.rows_returned += 1;
            return Ok(Some(row));
        }

        Ok(None)
    }

    fn reset(&mut self) -> Result<()> {
        self.results.clear();
        self.current_index = 0;
        self.statistics = OperatorStatistics::default();
        self.input.reset()?;
        Ok(())
    }

    fn get_schema(&self) -> Result<Vec<String>> {
        Ok(self.result_schema.clone())
    }

    fn get_statistics(&self) -> OperatorStatistics {
        self.statistics.clone()
    }
}

/// Simplified sort operator (demonstration version)
pub struct SortOperator {
    /// Input operator
    input: Box<dyn Operator>,
    /// Sort keys: column name and ascending flag
    sort_keys: Vec<(String, bool)>,
    /// Result schema
    result_schema: Vec<String>,
    /// Statistics
    statistics: OperatorStatistics,
    /// Sorted rows
    sorted_rows: Vec<Row>,
    /// Current index
    current_index: usize,
}

impl SortOperator {
    /// Create new sort operator
    pub fn new(
        input: Box<dyn Operator>,
        sort_keys: Vec<(String, bool)>,
        result_schema: Vec<String>,
    ) -> Result<Self> {
        if sort_keys.is_empty() {
            return Err(Error::QueryExecution {
                message: "Sort requires at least one sort key".to_string(),
            });
        }

        Ok(Self {
            input,
            sort_keys,
            result_schema,
            statistics: OperatorStatistics::default(),
            sorted_rows: Vec::new(),
            current_index: 0,
        })
    }

    fn compare_by_keys(a: &Row, b: &Row, keys: &[(String, bool)]) -> std::cmp::Ordering {
        use std::cmp::Ordering;
        for (name, ascending) in keys {
            let va = a
                .values
                .get(name)
                .map(column_value_to_eval)
                .unwrap_or(EvalValue::Null);
            let vb = b
                .values
                .get(name)
                .map(column_value_to_eval)
                .unwrap_or(EvalValue::Null);
            let ord = match (&va, &vb) {
                (EvalValue::Null, EvalValue::Null) => Ordering::Equal,
                (EvalValue::Null, _) => Ordering::Greater,
                (_, EvalValue::Null) => Ordering::Less,
                (EvalValue::Bool(a), EvalValue::Bool(b)) => a.cmp(b),
                (EvalValue::Int(a), EvalValue::Int(b)) => a.cmp(b),
                (EvalValue::Float(a), EvalValue::Float(b)) => {
                    a.partial_cmp(b).unwrap_or(Ordering::Equal)
                }
                (EvalValue::Int(a), EvalValue::Float(b)) => {
                    (*a as f64).partial_cmp(b).unwrap_or(Ordering::Equal)
                }
                (EvalValue::Float(a), EvalValue::Int(b)) => {
                    a.partial_cmp(&(*b as f64)).unwrap_or(Ordering::Equal)
                }
                (EvalValue::String(a), EvalValue::String(b)) => a.cmp(b),
                // different types: order by type name to keep deterministic
                _ => format!("{:?}", va).cmp(&format!("{:?}", vb)),
            };
            if ord != Ordering::Equal {
                return if *ascending { ord } else { ord.reverse() };
            }
        }
        Ordering::Equal
    }

    /// Load and sort all rows (simplified version)
    fn load_and_sort(&mut self) -> Result<()> {
        let mut rows = Vec::new();

        // Read all rows from input operator
        while let Some(row) = self.input.next()? {
            rows.push(row);
            self.statistics.rows_processed += 1;
        }

        let keys = self.sort_keys.clone();
        rows.sort_by(|a, b| Self::compare_by_keys(a, b, &keys));

        self.sorted_rows = rows;
        self.current_index = 0;

        Ok(())
    }
}

impl Operator for SortOperator {
    fn next(&mut self) -> Result<Option<Row>> {
        // If this is first call, load and sort data
        if self.sorted_rows.is_empty() && self.current_index == 0 {
            self.load_and_sort()?;
        }

        // Return next row
        if self.current_index < self.sorted_rows.len() {
            let row = self.sorted_rows[self.current_index].clone();
            self.current_index += 1;
            self.statistics.rows_returned += 1;
            return Ok(Some(row));
        }

        Ok(None)
    }

    fn reset(&mut self) -> Result<()> {
        self.sorted_rows.clear();
        self.current_index = 0;
        self.statistics = OperatorStatistics::default();
        self.input.reset()?;
        Ok(())
    }

    fn get_schema(&self) -> Result<Vec<String>> {
        Ok(self.result_schema.clone())
    }

    fn get_statistics(&self) -> OperatorStatistics {
        self.statistics.clone()
    }
}

/// Simplified sort-grouping operator (demonstration version)
pub struct SortGroupByOperator {
    /// Input operator
    input: Box<dyn Operator>,
    /// Grouping keys (column indices)
    group_keys: Vec<usize>,
    /// Aggregate functions
    aggregate_functions: Vec<(AggregateFunction, usize)>,
    /// Result schema
    result_schema: Vec<String>,
    /// Statistics
    statistics: OperatorStatistics,
    /// Group results
    group_results: Vec<Row>,
    /// Result index
    result_index: usize,
}

impl SortGroupByOperator {
    /// Create new sort-grouping operator
    pub fn new(
        input: Box<dyn Operator>,
        group_keys: Vec<usize>,
        aggregate_functions: Vec<(AggregateFunction, usize)>,
        result_schema: Vec<String>,
    ) -> Result<Self> {
        Ok(Self {
            input,
            group_keys,
            aggregate_functions,
            result_schema,
            statistics: OperatorStatistics::default(),
            group_results: Vec::new(),
            result_index: 0,
        })
    }

    /// Load and process data (simplified version)
    fn load_and_process(&mut self) -> Result<()> {
        let mut rows = Vec::new();

        // Read all rows from input operator
        while let Some(row) = self.input.next()? {
            rows.push(row);
            self.statistics.rows_processed += 1;
        }

        // Create demonstration group results
        for (i, _) in rows.iter().enumerate().take(3) {
            // Limit for demonstration
            let mut result_tuple = Tuple::new(i as u64);

            // Add grouping keys
            for (j, &key_index) in self.group_keys.iter().enumerate() {
                if key_index < 4 {
                    result_tuple.set_value(
                        &format!("group_key_{}", j),
                        ColumnValue::new(DataType::Integer(i as i32)),
                    );
                }
            }

            // Add aggregate functions
            for (j, (function, _)) in self.aggregate_functions.iter().enumerate() {
                match function {
                    AggregateFunction::Count => {
                        result_tuple.set_value(
                            &format!("count_{}", j),
                            ColumnValue::new(DataType::BigInt((i + 1) as i64)),
                        );
                    }
                    AggregateFunction::Sum => {
                        result_tuple.set_value(
                            &format!("sum_{}", j),
                            ColumnValue::new(DataType::Double((i + 1) as f64 * 100.0)),
                        );
                    }
                    AggregateFunction::Avg => {
                        result_tuple.set_value(
                            &format!("avg_{}", j),
                            ColumnValue::new(DataType::Double((i + 1) as f64 * 50.0)),
                        );
                    }
                    AggregateFunction::Min => {
                        result_tuple.set_value(
                            &format!("min_{}", j),
                            ColumnValue::new(DataType::Integer(((i + 1) * 10) as i32)),
                        );
                    }
                    AggregateFunction::Max => {
                        result_tuple.set_value(
                            &format!("max_{}", j),
                            ColumnValue::new(DataType::Integer(((i + 1) * 100) as i32)),
                        );
                    }
                    AggregateFunction::CountDistinct => {
                        result_tuple.set_value(
                            &format!("count_distinct_{}", j),
                            ColumnValue::new(DataType::BigInt((i + 1) as i64)),
                        );
                    }
                }
            }

            let result_row = Row::new();
            self.group_results.push(result_row);
        }

        Ok(())
    }
}

impl Operator for SortGroupByOperator {
    fn next(&mut self) -> Result<Option<Row>> {
        // If this is first call, load and process data
        if self.group_results.is_empty() && self.result_index == 0 {
            self.load_and_process()?;
        }

        // Return next group result
        if self.result_index < self.group_results.len() {
            let row = self.group_results[self.result_index].clone();
            self.result_index += 1;
            self.statistics.rows_returned += 1;
            return Ok(Some(row));
        }

        Ok(None)
    }

    fn reset(&mut self) -> Result<()> {
        self.group_results.clear();
        self.result_index = 0;
        self.statistics = OperatorStatistics::default();
        self.input.reset()?;
        Ok(())
    }

    fn get_schema(&self) -> Result<Vec<String>> {
        Ok(self.result_schema.clone())
    }

    fn get_statistics(&self) -> OperatorStatistics {
        self.statistics.clone()
    }
}

/// Factory for creating aggregation and sort operators
pub struct AggregationSortOperatorFactory;

impl AggregationSortOperatorFactory {
    /// Create grouping operator
    pub fn create_group_by(
        input: Box<dyn Operator>,
        group_keys: Vec<usize>,
        aggregate_functions: Vec<(AggregateFunction, usize)>,
        result_schema: Vec<String>,
        use_hash: bool,
    ) -> Result<Box<dyn Operator>> {
        if use_hash {
            Ok(Box::new(HashGroupByOperator::new(
                input,
                group_keys,
                aggregate_functions,
                result_schema,
            )?))
        } else {
            Ok(Box::new(SortGroupByOperator::new(
                input,
                group_keys,
                aggregate_functions,
                result_schema,
            )?))
        }
    }

    /// Create sort operator
    pub fn create_sort(
        input: Box<dyn Operator>,
        sort_column_indices: Vec<usize>,
        sort_directions: Vec<bool>,
        result_schema: Vec<String>,
    ) -> Result<Box<dyn Operator>> {
        let sort_keys: Vec<(String, bool)> = sort_column_indices
            .iter()
            .zip(sort_directions.iter())
            .map(|(&i, &asc)| (result_schema.get(i).cloned().unwrap_or_default(), asc))
            .collect();
        Ok(Box::new(SortOperator::new(
            input,
            sort_keys,
            result_schema,
        )?))
    }
}

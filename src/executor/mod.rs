//! Query executor for rustdb

pub mod executor;
pub mod operators;
pub mod result;

pub use executor::{QueryExecutor, QueryExecutorConfig};
pub use operators::{
    // New aggregation and sorting operators
    AggregateFunction,
    AggregateGroup,
    AggregationSortOperatorFactory,
    ConditionalScanOperator,
    HashGroupByOperator,
    HashJoinOperator,
    IndexCondition,
    IndexOperator,
    IndexScanOperator,
    JoinCondition,
    JoinOperator,
    JoinType,
    MergeJoinOperator,
    NestedLoopJoinOperator,
    Operator,
    OperatorStatistics,
    RangeScanOperator,
    ScanOperatorFactory,
    SortGroupByOperator,
    SortOperator,
    TableScanOperator,
};

//! Исполнитель запросов rustdb

pub mod executor;
pub mod operators;
pub mod result;

pub use operators::{
    // Новые операторы агрегации и сортировки
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

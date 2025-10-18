//! Исполнитель запросов rustdb

pub mod executor;
pub mod operators;
pub mod result;

pub use operators::{
    Operator, OperatorStatistics, TableScanOperator, IndexScanOperator,
    RangeScanOperator, ConditionalScanOperator, ScanOperatorFactory,
    IndexCondition, IndexOperator, NestedLoopJoinOperator, HashJoinOperator,
    MergeJoinOperator, JoinType, JoinCondition, JoinOperator,
    // Новые операторы агрегации и сортировки
    AggregateFunction, AggregateGroup, HashGroupByOperator, SortOperator,
    SortGroupByOperator, AggregationSortOperatorFactory
};

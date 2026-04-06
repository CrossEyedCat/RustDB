//! Tests for aggregation and sorting operators

use crate::common::types::{ColumnValue, DataType};
use crate::common::Result;
use crate::executor::operators::{
    AggregateFunction, AggregationSortOperatorFactory, HashGroupByOperator, Operator,
    SortGroupByOperator, SortOperator,
};
use crate::Row;
use std::collections::HashMap;

// / A simple test operator that returns fixed data
struct TestOperator {
    data: Vec<Row>,
    current_index: usize,
}

impl TestOperator {
    fn new() -> Self {
        let mut data = Vec::new();

        // Creating test data
        for i in 0..10 {
            let mut row = Row::new();
            row.set_value("id", ColumnValue::new(DataType::Integer(i)));
            row.set_value(
                "name",
                ColumnValue::new(DataType::Varchar(format!("user_{}", i))),
            );
            row.set_value(
                "age",
                ColumnValue::new(DataType::Integer(20 + (i % 5) * 10)),
            );
            row.set_value(
                "salary",
                ColumnValue::new(DataType::Double(1000.0 + (i as f64) * 100.0)),
            );
            data.push(row);
        }

        Self {
            data,
            current_index: 0,
        }
    }
}

impl Operator for TestOperator {
    fn next(&mut self) -> Result<Option<Row>> {
        if self.current_index < self.data.len() {
            let row = self.data[self.current_index].clone();
            self.current_index += 1;
            Ok(Some(row))
        } else {
            Ok(None)
        }
    }

    fn reset(&mut self) -> Result<()> {
        self.current_index = 0;
        Ok(())
    }

    fn get_schema(&self) -> Result<Vec<String>> {
        Ok(vec![
            "id".to_string(),
            "name".to_string(),
            "age".to_string(),
            "salary".to_string(),
        ])
    }

    fn get_statistics(&self) -> crate::executor::operators::OperatorStatistics {
        crate::executor::operators::OperatorStatistics::default()
    }
}

#[test]
fn test_hash_group_by_operator() -> Result<()> {
    let input = Box::new(TestOperator::new());

    // Grouping by age, aggregation by salary
    let group_keys = vec![2];
    let aggregate_functions = vec![
        (AggregateFunction::Count, 0), // COUNT(id)
        (AggregateFunction::Sum, 3),   // SUM(salary)
        (AggregateFunction::Avg, 3),   // AVG(salary)
    ];
    let result_schema = vec![
        "age".to_string(),
        "count".to_string(),
        "sum".to_string(),
        "avg".to_string(),
    ];

    let mut operator =
        HashGroupByOperator::new(input, group_keys, aggregate_functions, result_schema)?;

    // Getting results
    let mut results = Vec::new();
    while let Some(row) = operator.next()? {
        results.push(row);
    }

    // Checking that we have received grouping results
    assert!(!results.is_empty(), "There should be grouping results");

    println!("Grouping results:");
    for row in &results {
        println!("  {:?}", row);
    }

    Ok(())
}

#[test]
fn test_sort_operator() -> Result<()> {
    let input = Box::new(TestOperator::new());

    let sort_keys = vec![("age".to_string(), true), ("salary".to_string(), false)];
    let result_schema = vec![
        "id".to_string(),
        "name".to_string(),
        "age".to_string(),
        "salary".to_string(),
    ];

    let mut operator = SortOperator::new(input, sort_keys, result_schema)?;

    // Getting sorted results
    let mut results = Vec::new();
    while let Some(row) = operator.next()? {
        results.push(row);
    }

    // Checking that we have received sorted results
    assert_eq!(results.len(), 10, "There should be 10 sorted rows");

    println!("Sorted results:");
    for row in &results {
        println!("  {:?}", row);
    }

    Ok(())
}

#[test]
fn test_sort_group_by_operator() -> Result<()> {
    let input = Box::new(TestOperator::new());

    // Grouping by age with sorting
    let group_keys = vec![2];
    let aggregate_functions = vec![
        (AggregateFunction::Count, 0), // COUNT(id)
        (AggregateFunction::Max, 3),   // MAX(salary)
    ];
    let result_schema = vec![
        "age".to_string(),
        "count".to_string(),
        "max_salary".to_string(),
    ];

    let mut operator =
        SortGroupByOperator::new(input, group_keys, aggregate_functions, result_schema)?;

    // Getting results
    let mut results = Vec::new();
    while let Some(row) = operator.next()? {
        results.push(row);
    }

    // Checking that we have received grouping results
    assert!(!results.is_empty(), "There should be grouping results");

    println!("Sorting-grouping results:");
    for row in &results {
        println!("  {:?}", row);
    }

    Ok(())
}

#[test]
fn test_aggregation_sort_factory() -> Result<()> {
    let input = Box::new(TestOperator::new());

    // Testing the factory to create a grouping operator
    let group_keys = vec![2];
    let aggregate_functions = vec![(AggregateFunction::Count, 0), (AggregateFunction::Sum, 3)];
    let result_schema = vec!["age".to_string(), "count".to_string(), "sum".to_string()];

    // Create a hash group
    let mut hash_operator = AggregationSortOperatorFactory::create_group_by(
        input,
        group_keys.clone(),
        aggregate_functions.clone(),
        result_schema.clone(),
        true, // use_hash
    )?;

    // Create a sorting grouping
    let input2 = Box::new(TestOperator::new());
    let mut sort_operator = AggregationSortOperatorFactory::create_group_by(
        input2,
        group_keys,
        aggregate_functions,
        result_schema,
        false, // use_sort
    )?;

    // We get results from both operators
    let mut hash_results = Vec::new();
    while let Some(row) = hash_operator.next()? {
        hash_results.push(row);
    }

    let mut sort_results = Vec::new();
    while let Some(row) = sort_operator.next()? {
        sort_results.push(row);
    }

    // We check that both operators gave results
    assert!(
        !hash_results.is_empty(),
        "Hash grouping should give results"
    );
    assert!(
        !sort_results.is_empty(),
        "Sorting-grouping should give results"
    );

    println!("Hash grouping results: {}", hash_results.len());
    println!("Sorting-grouping results: {}", sort_results.len());

    Ok(())
}

#[test]
fn test_aggregate_functions() -> Result<()> {
    let input = Box::new(TestOperator::new());

    // Testing various aggregate functions
    let group_keys = vec![2];
    let aggregate_functions = vec![
        (AggregateFunction::Count, 0),         // COUNT
        (AggregateFunction::Sum, 3),           // SUM
        (AggregateFunction::Avg, 3),           // AVG
        (AggregateFunction::Min, 3),           // MIN
        (AggregateFunction::Max, 3),           // MAX
        (AggregateFunction::CountDistinct, 3), // COUNT DISTINCT
    ];
    let result_schema = vec![
        "age".to_string(),
        "count".to_string(),
        "sum".to_string(),
        "avg".to_string(),
        "min".to_string(),
        "max".to_string(),
        "count_distinct".to_string(),
    ];

    let mut operator =
        HashGroupByOperator::new(input, group_keys, aggregate_functions, result_schema)?;

    // Getting results
    let mut results = Vec::new();
    while let Some(row) = operator.next()? {
        results.push(row);
    }

    // Checking that we have received the results
    assert!(
        !results.is_empty(),
        "There should be results with different aggregate functions"
    );

    println!("Results with various aggregate functions:");
    for row in &results {
        println!("  {:?}", row);
    }

    Ok(())
}

//! Тесты для операторов агрегации и сортировки

use crate::executor::operators::{
    Operator, AggregateFunction, HashGroupByOperator, SortOperator,
    SortGroupByOperator, AggregationSortOperatorFactory
};
use crate::storage::row::Row;
use crate::storage::tuple::Tuple;
use crate::common::types::{DataType, ColumnValue};
use crate::common::Result;
use std::collections::HashMap;

/// Простой оператор для тестирования, который возвращает фиксированные данные
struct TestOperator {
    data: Vec<Row>,
    current_index: usize,
}

impl TestOperator {
    fn new() -> Self {
        let mut data = Vec::new();
        
        // Создаем тестовые данные
        for i in 0..10 {
            let mut tuple = Tuple::new();
            tuple.set_value("id", ColumnValue::new(DataType::Integer(i)));
            tuple.set_value("name", ColumnValue::new(DataType::Varchar(format!("user_{}", i))));
            tuple.set_value("age", ColumnValue::new(DataType::Integer(20 + (i % 5) * 10)));
            tuple.set_value("salary", ColumnValue::new(DataType::Double(1000.0 + (i as f64) * 100.0)));
            
            let row = Row::new(i as u64, tuple);
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
        Ok(vec!["id".to_string(), "name".to_string(), "age".to_string(), "salary".to_string()])
    }

    fn get_statistics(&self) -> crate::executor::operators::OperatorStatistics {
        crate::executor::operators::OperatorStatistics::default()
    }
}

#[test]
fn test_hash_group_by_operator() -> Result<()> {
    let input = Box::new(TestOperator::new());
    
    // Группировка по возрасту, агрегация по зарплате
    let group_keys = vec![2]; // индекс колонки age
    let aggregate_functions = vec![
        (AggregateFunction::Count, 0), // COUNT(id)
        (AggregateFunction::Sum, 3),    // SUM(salary)
        (AggregateFunction::Avg, 3),   // AVG(salary)
    ];
    let result_schema = vec!["age".to_string(), "count".to_string(), "sum".to_string(), "avg".to_string()];
    
    let mut operator = HashGroupByOperator::new(
        input,
        group_keys,
        aggregate_functions,
        result_schema,
    )?;
    
    // Получаем результаты
    let mut results = Vec::new();
    while let Some(row) = operator.next()? {
        results.push(row);
    }
    
    // Проверяем, что получили результаты группировки
    assert!(!results.is_empty(), "Должны быть результаты группировки");
    
    println!("Результаты группировки:");
    for row in &results {
        println!("  {:?}", row);
    }
    
    Ok(())
}

#[test]
fn test_sort_operator() -> Result<()> {
    let input = Box::new(TestOperator::new());
    
    // Сортировка по возрасту (ASC), затем по зарплате (DESC)
    let sort_columns = vec![2, 3]; // age, salary
    let sort_directions = vec![true, false]; // ASC, DESC
    let result_schema = vec!["id".to_string(), "name".to_string(), "age".to_string(), "salary".to_string()];
    
    let mut operator = SortOperator::new(
        input,
        sort_columns,
        sort_directions,
        result_schema,
    )?;
    
    // Получаем отсортированные результаты
    let mut results = Vec::new();
    while let Some(row) = operator.next()? {
        results.push(row);
    }
    
    // Проверяем, что получили отсортированные результаты
    assert_eq!(results.len(), 10, "Должно быть 10 отсортированных строк");
    
    println!("Отсортированные результаты:");
    for row in &results {
        println!("  {:?}", row);
    }
    
    Ok(())
}

#[test]
fn test_sort_group_by_operator() -> Result<()> {
    let input = Box::new(TestOperator::new());
    
    // Группировка по возрасту с сортировкой
    let group_keys = vec![2]; // индекс колонки age
    let aggregate_functions = vec![
        (AggregateFunction::Count, 0), // COUNT(id)
        (AggregateFunction::Max, 3),   // MAX(salary)
    ];
    let result_schema = vec!["age".to_string(), "count".to_string(), "max_salary".to_string()];
    
    let mut operator = SortGroupByOperator::new(
        input,
        group_keys,
        aggregate_functions,
        result_schema,
    )?;
    
    // Получаем результаты
    let mut results = Vec::new();
    while let Some(row) = operator.next()? {
        results.push(row);
    }
    
    // Проверяем, что получили результаты группировки
    assert!(!results.is_empty(), "Должны быть результаты группировки");
    
    println!("Результаты сортировки-группировки:");
    for row in &results {
        println!("  {:?}", row);
    }
    
    Ok(())
}

#[test]
fn test_aggregation_sort_factory() -> Result<()> {
    let input = Box::new(TestOperator::new());
    
    // Тестируем фабрику для создания оператора группировки
    let group_keys = vec![2]; // возраст
    let aggregate_functions = vec![
        (AggregateFunction::Count, 0),
        (AggregateFunction::Sum, 3),
    ];
    let result_schema = vec!["age".to_string(), "count".to_string(), "sum".to_string()];
    
    // Создаем хеш-группировку
    let mut hash_operator = AggregationSortOperatorFactory::create_group_by(
        input,
        group_keys.clone(),
        aggregate_functions.clone(),
        result_schema.clone(),
        true, // use_hash
    )?;
    
    // Создаем сортировку-группировку
    let input2 = Box::new(TestOperator::new());
    let mut sort_operator = AggregationSortOperatorFactory::create_group_by(
        input2,
        group_keys,
        aggregate_functions,
        result_schema,
        false, // use_sort
    )?;
    
    // Получаем результаты от обоих операторов
    let mut hash_results = Vec::new();
    while let Some(row) = hash_operator.next()? {
        hash_results.push(row);
    }
    
    let mut sort_results = Vec::new();
    while let Some(row) = sort_operator.next()? {
        sort_results.push(row);
    }
    
    // Проверяем, что оба оператора дали результаты
    assert!(!hash_results.is_empty(), "Хеш-группировка должна дать результаты");
    assert!(!sort_results.is_empty(), "Сортировка-группировка должна дать результаты");
    
    println!("Результаты хеш-группировки: {}", hash_results.len());
    println!("Результаты сортировки-группировки: {}", sort_results.len());
    
    Ok(())
}

#[test]
fn test_aggregate_functions() -> Result<()> {
    let input = Box::new(TestOperator::new());
    
    // Тестируем различные агрегатные функции
    let group_keys = vec![2]; // возраст
    let aggregate_functions = vec![
        (AggregateFunction::Count, 0),        // COUNT
        (AggregateFunction::Sum, 3),          // SUM
        (AggregateFunction::Avg, 3),          // AVG
        (AggregateFunction::Min, 3),          // MIN
        (AggregateFunction::Max, 3),          // MAX
        (AggregateFunction::CountDistinct, 3), // COUNT DISTINCT
    ];
    let result_schema = vec![
        "age".to_string(), "count".to_string(), "sum".to_string(), 
        "avg".to_string(), "min".to_string(), "max".to_string(), "count_distinct".to_string()
    ];
    
    let mut operator = HashGroupByOperator::new(
        input,
        group_keys,
        aggregate_functions,
        result_schema,
    )?;
    
    // Получаем результаты
    let mut results = Vec::new();
    while let Some(row) = operator.next()? {
        results.push(row);
    }
    
    // Проверяем, что получили результаты
    assert!(!results.is_empty(), "Должны быть результаты с различными агрегатными функциями");
    
    println!("Результаты с различными агрегатными функциями:");
    for row in &results {
        println!("  {:?}", row);
    }
    
    Ok(())
}


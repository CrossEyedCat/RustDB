//! Пример использования операторов агрегации и сортировки

use rustdb::executor::{
    Operator, OperatorStatistics, AggregateFunction, HashGroupByOperator, 
    SortOperator, SortGroupByOperator, AggregationSortOperatorFactory
};
use rustdb::common::types::{Row, ColumnValue, DataType};

/// Демонстрационный оператор для предоставления тестовых данных
struct DemoOperator {
    data: Vec<Row>,
    current_index: usize,
}

impl DemoOperator {
    fn new() -> Self {
        let mut data = Vec::new();
        
        // Создаем тестовые данные
        for i in 0..10 {
            let mut row = Row::new();
            row.set_value("id", ColumnValue::new(DataType::Integer(i as i32)));
            row.set_value("name", ColumnValue::new(DataType::Varchar(format!("User{}", i))));
            row.set_value("age", ColumnValue::new(DataType::Integer(20 + (i % 30))));
            row.set_value("department", ColumnValue::new(DataType::Varchar(
                match i % 3 {
                    0 => "IT".to_string(),
                    1 => "HR".to_string(),
                    _ => "Sales".to_string(),
                }
            )));
            row.set_value("salary", ColumnValue::new(DataType::Double(50000.0 + (i as f64 * 1000.0))));
            data.push(row);
        }
        
        Self {
            data,
            current_index: 0,
        }
    }
}

impl Operator for DemoOperator {
    fn next(&mut self) -> rustdb::common::Result<Option<rustdb::common::types::Row>> {
        if self.current_index < self.data.len() {
            let row = self.data[self.current_index].clone();
            self.current_index += 1;
            return Ok(Some(row));
        }
        Ok(None)
    }

    fn reset(&mut self) -> rustdb::common::Result<()> {
        self.current_index = 0;
        Ok(())
    }

    fn get_schema(&self) -> rustdb::common::Result<Vec<String>> {
        Ok(vec![
            "id".to_string(),
            "name".to_string(), 
            "age".to_string(),
            "department".to_string(),
            "salary".to_string(),
        ])
    }

    fn get_statistics(&self) -> OperatorStatistics {
        OperatorStatistics::default()
    }
}

/// Демонстрация оператора сортировки
fn demo_sort_operator() -> rustdb::common::Result<()> {
    println!("=== Демонстрация оператора сортировки ===");
    
    let input = Box::new(DemoOperator::new());
    let sort_operator = SortOperator::new(
        input,
        vec![3], // Сортировка по department
        vec![true], // ASC
        vec![
            "id".to_string(),
            "name".to_string(),
            "age".to_string(), 
            "department".to_string(),
            "salary".to_string(),
        ],
    )?;

    let mut operator = sort_operator;
    let mut count = 0;
    
    while let Some(row) = operator.next()? {
        if count < 5 { // Показываем первые 5 результатов
            println!("Строка {}: ID={:?}, Name={:?}, Age={:?}, Department={:?}, Salary={:?}",
                count,
                row.get_value("id").map(|v| &v.data_type).unwrap_or(&DataType::Null),
                row.get_value("name").map(|v| &v.data_type).unwrap_or(&DataType::Null),
                row.get_value("age").map(|v| &v.data_type).unwrap_or(&DataType::Null),
                row.get_value("department").map(|v| &v.data_type).unwrap_or(&DataType::Null),
                row.get_value("salary").map(|v| &v.data_type).unwrap_or(&DataType::Null),
            );
        }
        count += 1;
    }
    
    println!("Всего отсортировано строк: {}", count);
    println!("Статистика: {:?}", operator.get_statistics());
    println!();
    
    Ok(())
}

/// Демонстрация оператора группировки
fn demo_group_by_operator() -> rustdb::common::Result<()> {
    println!("=== Демонстрация оператора группировки ===");
    
    let input = Box::new(DemoOperator::new());
    let group_by_operator = HashGroupByOperator::new(
        input,
        vec![3], // Группировка по department
        vec![
            (AggregateFunction::Count, 0),
            (AggregateFunction::Sum, 4), // salary
            (AggregateFunction::Avg, 4), // salary
            (AggregateFunction::Min, 4), // salary
            (AggregateFunction::Max, 4), // salary
        ],
        vec![
            "department".to_string(),
            "count".to_string(),
            "sum".to_string(),
            "avg".to_string(),
            "min".to_string(),
            "max".to_string(),
        ],
    )?;

    let mut operator = group_by_operator;
    
    while let Some(row) = operator.next()? {
        println!("Группа: Department={:?}, Count={:?}, Sum={:?}, Avg={:?}, Min={:?}, Max={:?}",
            row.get_value("department").map(|v| &v.data_type).unwrap_or(&DataType::Null),
            row.get_value("count").map(|v| &v.data_type).unwrap_or(&DataType::Null),
            row.get_value("sum").map(|v| &v.data_type).unwrap_or(&DataType::Null),
            row.get_value("avg").map(|v| &v.data_type).unwrap_or(&DataType::Null),
            row.get_value("min").map(|v| &v.data_type).unwrap_or(&DataType::Null),
            row.get_value("max").map(|v| &v.data_type).unwrap_or(&DataType::Null),
        );
    }
    
    println!("Статистика: {:?}", operator.get_statistics());
    println!();
    
    Ok(())
}

/// Демонстрация оператора сортировки-группировки
fn demo_sort_group_by_operator() -> rustdb::common::Result<()> {
    println!("=== Демонстрация оператора сортировки-группировки ===");
    
    let input = Box::new(DemoOperator::new());
    let sort_group_by_operator = SortGroupByOperator::new(
        input,
        vec![3], // Группировка по department
        vec![
            (AggregateFunction::Count, 0),
            (AggregateFunction::Sum, 4), // salary
        ],
        vec![
            "department".to_string(),
            "count".to_string(),
            "sum".to_string(),
        ],
    )?;

    let mut operator = sort_group_by_operator;
    
    while let Some(row) = operator.next()? {
        println!("Группа: Department={:?}, Count={:?}, Sum={:?}",
            row.get_value("department").map(|v| &v.data_type).unwrap_or(&DataType::Null),
            row.get_value("count").map(|v| &v.data_type).unwrap_or(&DataType::Null),
            row.get_value("sum").map(|v| &v.data_type).unwrap_or(&DataType::Null),
        );
    }
    
    println!("Статистика: {:?}", operator.get_statistics());
    println!();
    
    Ok(())
}

/// Демонстрация фабрики операторов
fn demo_operator_factory() -> rustdb::common::Result<()> {
    println!("=== Демонстрация фабрики операторов ===");
    
    let input = Box::new(DemoOperator::new());
    
    // Создаем оператор группировки через фабрику
    let group_by_operator = AggregationSortOperatorFactory::create_group_by(
        input,
        vec![3], // Группировка по department
        vec![
            (AggregateFunction::Count, 0),
            (AggregateFunction::Sum, 4), // salary
        ],
        vec![
            "department".to_string(),
            "count".to_string(),
            "sum".to_string(),
        ],
        true, // Используем хеш-группировку
    )?;

    let mut operator = group_by_operator;
    
    while let Some(row) = operator.next()? {
        println!("Группа: Department={:?}, Count={:?}, Sum={:?}",
            row.get_value("department").map(|v| &v.data_type).unwrap_or(&DataType::Null),
            row.get_value("count").map(|v| &v.data_type).unwrap_or(&DataType::Null),
            row.get_value("sum").map(|v| &v.data_type).unwrap_or(&DataType::Null),
        );
    }
    
    println!("Статистика: {:?}", operator.get_statistics());
    println!();
    
    Ok(())
}

fn main() -> rustdb::common::Result<()> {
    println!("Демонстрация операторов агрегации и сортировки в RustDB");
    println!("=====================================================");
    println!();

    // Запускаем демонстрации
    demo_sort_operator()?;
    demo_group_by_operator()?;
    demo_sort_group_by_operator()?;
    demo_operator_factory()?;

    println!("Демонстрация завершена успешно!");
    Ok(())
}

//! Example of using aggregation and sorting operators

use rustdb::common::types::{ColumnValue, DataType, Row};
use rustdb::executor::{
    AggregateFunction, AggregationSortOperatorFactory, HashGroupByOperator, Operator,
    OperatorStatistics, SortGroupByOperator, SortOperator,
};

// / Demo operator to provide test data
struct DemoOperator {
    data: Vec<Row>,
    current_index: usize,
}

impl DemoOperator {
    fn new() -> Self {
        let mut data = Vec::new();

        // Creating test data
        for i in 0..10 {
            let mut row = Row::new();
            row.set_value("id", ColumnValue::new(DataType::Integer(i)));
            row.set_value(
                "name",
                ColumnValue::new(DataType::Varchar(format!("User{}", i))),
            );
            row.set_value("age", ColumnValue::new(DataType::Integer(20 + (i % 30))));
            row.set_value(
                "department",
                ColumnValue::new(DataType::Varchar(match i % 3 {
                    0 => "IT".to_string(),
                    1 => "HR".to_string(),
                    _ => "Sales".to_string(),
                })),
            );
            row.set_value(
                "salary",
                ColumnValue::new(DataType::Double(50000.0 + (i as f64 * 1000.0))),
            );
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

// / Demonstration of the sort operator
fn demo_sort_operator() -> rustdb::common::Result<()> {
    println!("=== Demonstration of the sort operator ===");

    let input = Box::new(DemoOperator::new());
    let sort_operator = SortOperator::new(
        input,
        vec![("salary".to_string(), true)],
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
        if count < 5 {
            // Showing the first 5 results
            println!(
                "Line {}: ID={:?}, Name={:?}, Age={:?}, Department={:?}, Salary={:?}",
                count,
                row.get_value("id")
                    .map(|v| &v.data_type)
                    .unwrap_or(&DataType::Null),
                row.get_value("name")
                    .map(|v| &v.data_type)
                    .unwrap_or(&DataType::Null),
                row.get_value("age")
                    .map(|v| &v.data_type)
                    .unwrap_or(&DataType::Null),
                row.get_value("department")
                    .map(|v| &v.data_type)
                    .unwrap_or(&DataType::Null),
                row.get_value("salary")
                    .map(|v| &v.data_type)
                    .unwrap_or(&DataType::Null),
            );
        }
        count += 1;
    }

    println!("Total rows sorted: {}", count);
    println!("Statistics: {:?}", operator.get_statistics());
    println!();

    Ok(())
}

// / Demonstration of the grouping operator
fn demo_group_by_operator() -> rustdb::common::Result<()> {
    println!("=== Grouping operator demonstration ===");

    let input = Box::new(DemoOperator::new());
    let group_by_operator = HashGroupByOperator::new(
        input,
        vec![3],
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
        println!(
            "Group: Department={:?}, Count={:?}, Sum={:?}, Avg={:?}, Min={:?}, Max={:?}",
            row.get_value("department")
                .map(|v| &v.data_type)
                .unwrap_or(&DataType::Null),
            row.get_value("count")
                .map(|v| &v.data_type)
                .unwrap_or(&DataType::Null),
            row.get_value("sum")
                .map(|v| &v.data_type)
                .unwrap_or(&DataType::Null),
            row.get_value("avg")
                .map(|v| &v.data_type)
                .unwrap_or(&DataType::Null),
            row.get_value("min")
                .map(|v| &v.data_type)
                .unwrap_or(&DataType::Null),
            row.get_value("max")
                .map(|v| &v.data_type)
                .unwrap_or(&DataType::Null),
        );
    }

    println!("Statistics: {:?}", operator.get_statistics());
    println!();

    Ok(())
}

// / Demonstration of the sort-group operator
fn demo_sort_group_by_operator() -> rustdb::common::Result<()> {
    println!("=== Demonstration of the sort-group operator ===");

    let input = Box::new(DemoOperator::new());
    let sort_group_by_operator = SortGroupByOperator::new(
        input,
        vec![3],
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
        println!(
            "Group: Department={:?}, Count={:?}, Sum={:?}",
            row.get_value("department")
                .map(|v| &v.data_type)
                .unwrap_or(&DataType::Null),
            row.get_value("count")
                .map(|v| &v.data_type)
                .unwrap_or(&DataType::Null),
            row.get_value("sum")
                .map(|v| &v.data_type)
                .unwrap_or(&DataType::Null),
        );
    }

    println!("Statistics: {:?}", operator.get_statistics());
    println!();

    Ok(())
}

// / Demonstration of operator factory
fn demo_operator_factory() -> rustdb::common::Result<()> {
    println!("=== Operator Factory Demonstration ===");

    let input = Box::new(DemoOperator::new());

    // Creating a grouping operator through a factory
    let group_by_operator = AggregationSortOperatorFactory::create_group_by(
        input,
        vec![3],
        vec![
            (AggregateFunction::Count, 0),
            (AggregateFunction::Sum, 4), // salary
        ],
        vec![
            "department".to_string(),
            "count".to_string(),
            "sum".to_string(),
        ],
        true,
    )?;

    let mut operator = group_by_operator;

    while let Some(row) = operator.next()? {
        println!(
            "Group: Department={:?}, Count={:?}, Sum={:?}",
            row.get_value("department")
                .map(|v| &v.data_type)
                .unwrap_or(&DataType::Null),
            row.get_value("count")
                .map(|v| &v.data_type)
                .unwrap_or(&DataType::Null),
            row.get_value("sum")
                .map(|v| &v.data_type)
                .unwrap_or(&DataType::Null),
        );
    }

    println!("Statistics: {:?}", operator.get_statistics());
    println!();

    Ok(())
}

fn main() -> rustdb::common::Result<()> {
    println!("Demonstration of aggregation and sorting operators in RustDB");
    println!("=====================================================");
    println!();

    // Launching demos
    demo_sort_operator()?;
    demo_group_by_operator()?;
    demo_sort_group_by_operator()?;
    demo_operator_factory()?;

    println!("Demonstration completed successfully!");
    Ok(())
}

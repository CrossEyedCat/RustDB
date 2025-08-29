//! Тесты для структуры Tuple

use crate::storage::tuple::{Tuple, TupleHeader, Schema, Column, Constraint, TableOptions};
use crate::common::types::{DataType, ColumnValue};
use crate::common::Result;

#[test]
fn test_tuple_creation() {
    let tuple = Tuple::new(1);
    
    assert_eq!(tuple.get_id(), 1);
    assert_eq!(tuple.get_values().len(), 0);
    assert!(tuple.is_dirty());
}

#[test]
fn test_tuple_with_values() {
    let mut tuple = Tuple::new(1);
    
    tuple.add_value(ColumnValue::Integer(42));
    tuple.add_value(ColumnValue::Text("Hello".to_string()));
    tuple.add_value(ColumnValue::Boolean(true));
    
    assert_eq!(tuple.get_values().len(), 3);
    
    let values = tuple.get_values();
    assert_eq!(values[0], ColumnValue::Integer(42));
    assert_eq!(values[1], ColumnValue::Text("Hello".to_string()));
    assert_eq!(values[2], ColumnValue::Boolean(true));
}

#[test]
fn test_tuple_get_value() {
    let mut tuple = Tuple::new(1);
    
    tuple.add_value(ColumnValue::Integer(100));
    tuple.add_value(ColumnValue::Text("World".to_string()));
    
    assert_eq!(tuple.get_value(0), Some(&ColumnValue::Integer(100)));
    assert_eq!(tuple.get_value(1), Some(&ColumnValue::Text("World".to_string())));
    assert_eq!(tuple.get_value(2), None); // Несуществующий индекс
}

#[test]
fn test_tuple_set_value() {
    let mut tuple = Tuple::new(1);
    
    tuple.add_value(ColumnValue::Integer(10));
    tuple.add_value(ColumnValue::Text("Old".to_string()));
    
    // Изменяем значения
    tuple.set_value(0, ColumnValue::Integer(20));
    tuple.set_value(1, ColumnValue::Text("New".to_string()));
    
    assert_eq!(tuple.get_value(0), Some(&ColumnValue::Integer(20)));
    assert_eq!(tuple.get_value(1), Some(&ColumnValue::Text("New".to_string())));
}

#[test]
fn test_tuple_set_invalid_index() {
    let mut tuple = Tuple::new(1);
    tuple.add_value(ColumnValue::Integer(1));
    
    // Попытка установить значение по несуществующему индексу
    let result = tuple.set_value(5, ColumnValue::Integer(2));
    assert!(result.is_err());
}

#[test]
fn test_tuple_header() {
    let tuple = Tuple::new(42);
    let header = tuple.get_header();
    
    assert_eq!(header.tuple_id, 42);
    assert_eq!(header.field_count, 0);
}

#[test]
fn test_tuple_serialization() {
    let mut tuple = Tuple::new(1);
    tuple.add_value(ColumnValue::Integer(42));
    tuple.add_value(ColumnValue::Text("Test".to_string()));
    tuple.add_value(ColumnValue::Boolean(false));
    
    // Сериализуем кортеж
    let serialized = tuple.serialize();
    assert!(!serialized.is_empty());
    
    // Десериализуем кортеж
    let deserialized = Tuple::deserialize(&serialized);
    assert!(deserialized.is_ok());
    
    let new_tuple = deserialized.unwrap();
    assert_eq!(new_tuple.get_id(), 1);
    assert_eq!(new_tuple.get_values().len(), 3);
    assert_eq!(new_tuple.get_value(0), Some(&ColumnValue::Integer(42)));
    assert_eq!(new_tuple.get_value(1), Some(&ColumnValue::Text("Test".to_string())));
    assert_eq!(new_tuple.get_value(2), Some(&ColumnValue::Boolean(false)));
}

#[test]
fn test_tuple_different_data_types() {
    let mut tuple = Tuple::new(1);
    
    // Добавляем различные типы данных
    tuple.add_value(ColumnValue::Integer(i64::MAX));
    tuple.add_value(ColumnValue::Float(3.14159));
    tuple.add_value(ColumnValue::Text("Unicode: 🦀".to_string()));
    tuple.add_value(ColumnValue::Boolean(true));
    tuple.add_value(ColumnValue::Null);
    
    assert_eq!(tuple.get_values().len(), 5);
    
    // Проверяем каждое значение
    assert_eq!(tuple.get_value(0), Some(&ColumnValue::Integer(i64::MAX)));
    assert_eq!(tuple.get_value(1), Some(&ColumnValue::Float(3.14159)));
    assert_eq!(tuple.get_value(2), Some(&ColumnValue::Text("Unicode: 🦀".to_string())));
    assert_eq!(tuple.get_value(3), Some(&ColumnValue::Boolean(true)));
    assert_eq!(tuple.get_value(4), Some(&ColumnValue::Null));
}

#[test]
fn test_tuple_large_text() {
    let mut tuple = Tuple::new(1);
    let large_text = "A".repeat(10000); // 10KB текста
    
    tuple.add_value(ColumnValue::Text(large_text.clone()));
    
    assert_eq!(tuple.get_values().len(), 1);
    assert_eq!(tuple.get_value(0), Some(&ColumnValue::Text(large_text)));
}

#[test]
fn test_tuple_empty_values() {
    let mut tuple = Tuple::new(1);
    
    tuple.add_value(ColumnValue::Text("".to_string())); // Пустая строка
    tuple.add_value(ColumnValue::Null); // NULL значение
    
    assert_eq!(tuple.get_values().len(), 2);
    assert_eq!(tuple.get_value(0), Some(&ColumnValue::Text("".to_string())));
    assert_eq!(tuple.get_value(1), Some(&ColumnValue::Null));
}

#[test]
fn test_tuple_dirty_flag() {
    let mut tuple = Tuple::new(1);
    assert!(tuple.is_dirty()); // Новый кортеж помечен как грязный
    
    tuple.mark_clean();
    assert!(!tuple.is_dirty());
    
    // Любое изменение должно помечать кортеж как грязный
    tuple.add_value(ColumnValue::Integer(1));
    assert!(tuple.is_dirty());
    
    tuple.mark_clean();
    assert!(!tuple.is_dirty());
    
    tuple.set_value(0, ColumnValue::Integer(2)).unwrap();
    assert!(tuple.is_dirty());
}

#[test]
fn test_schema_creation() {
    let mut schema = Schema::new("test_table".to_string());
    
    assert_eq!(schema.get_table_name(), "test_table");
    assert_eq!(schema.get_columns().len(), 0);
}

#[test]
fn test_schema_add_column() {
    let mut schema = Schema::new("test_table".to_string());
    
    let column = Column {
        name: "id".to_string(),
        data_type: DataType::Integer,
        nullable: false,
        default_value: None,
    };
    
    schema.add_column(column);
    assert_eq!(schema.get_columns().len(), 1);
    assert_eq!(schema.get_columns()[0].name, "id");
    assert_eq!(schema.get_columns()[0].data_type, DataType::Integer);
}

#[test]
fn test_schema_multiple_columns() {
    let mut schema = Schema::new("users".to_string());
    
    schema.add_column(Column {
        name: "id".to_string(),
        data_type: DataType::Integer,
        nullable: false,
        default_value: None,
    });
    
    schema.add_column(Column {
        name: "name".to_string(),
        data_type: DataType::Text,
        nullable: false,
        default_value: None,
    });
    
    schema.add_column(Column {
        name: "email".to_string(),
        data_type: DataType::Text,
        nullable: true,
        default_value: Some(ColumnValue::Null),
    });
    
    assert_eq!(schema.get_columns().len(), 3);
    
    // Проверяем nullable колонки
    assert!(!schema.get_columns()[0].nullable);
    assert!(!schema.get_columns()[1].nullable);
    assert!(schema.get_columns()[2].nullable);
}

#[test]
fn test_schema_constraints() {
    let mut schema = Schema::new("products".to_string());
    
    schema.add_column(Column {
        name: "id".to_string(),
        data_type: DataType::Integer,
        nullable: false,
        default_value: None,
    });
    
    // Добавляем ограничения
    schema.add_constraint(Constraint::PrimaryKey(vec!["id".to_string()]));
    schema.add_constraint(Constraint::Unique(vec!["id".to_string()]));
    
    assert_eq!(schema.get_constraints().len(), 2);
}

#[test]
fn test_schema_validation() {
    let mut schema = Schema::new("test".to_string());
    
    schema.add_column(Column {
        name: "required_field".to_string(),
        data_type: DataType::Integer,
        nullable: false,
        default_value: None,
    });
    
    schema.add_column(Column {
        name: "optional_field".to_string(),
        data_type: DataType::Text,
        nullable: true,
        default_value: Some(ColumnValue::Text("default".to_string())),
    });
    
    // Создаем кортеж, соответствующий схеме
    let mut valid_tuple = Tuple::new(1);
    valid_tuple.add_value(ColumnValue::Integer(42));
    valid_tuple.add_value(ColumnValue::Text("test".to_string()));
    
    let result = schema.validate_tuple(&valid_tuple);
    assert!(result.is_ok());
    
    // Создаем кортеж с NULL в обязательном поле
    let mut invalid_tuple = Tuple::new(2);
    invalid_tuple.add_value(ColumnValue::Null); // Нарушение NOT NULL
    invalid_tuple.add_value(ColumnValue::Text("test".to_string()));
    
    let result = schema.validate_tuple(&invalid_tuple);
    assert!(result.is_err());
}

#[test]
fn test_tuple_clone() {
    let mut original = Tuple::new(1);
    original.add_value(ColumnValue::Integer(42));
    original.add_value(ColumnValue::Text("Original".to_string()));
    
    let cloned = original.clone();
    
    assert_eq!(cloned.get_id(), original.get_id());
    assert_eq!(cloned.get_values().len(), original.get_values().len());
    assert_eq!(cloned.get_value(0), original.get_value(0));
    assert_eq!(cloned.get_value(1), original.get_value(1));
}

#[test]
fn test_tuple_equality() {
    let mut tuple1 = Tuple::new(1);
    tuple1.add_value(ColumnValue::Integer(42));
    tuple1.add_value(ColumnValue::Text("Test".to_string()));
    
    let mut tuple2 = Tuple::new(1);
    tuple2.add_value(ColumnValue::Integer(42));
    tuple2.add_value(ColumnValue::Text("Test".to_string()));
    
    let mut tuple3 = Tuple::new(2); // Другой ID
    tuple3.add_value(ColumnValue::Integer(42));
    tuple3.add_value(ColumnValue::Text("Test".to_string()));
    
    assert_eq!(tuple1, tuple2);
    assert_ne!(tuple1, tuple3);
}

#[test]
fn test_tuple_size_calculation() {
    let mut tuple = Tuple::new(1);
    
    let initial_size = tuple.calculate_size();
    assert!(initial_size > 0); // Размер заголовка
    
    tuple.add_value(ColumnValue::Integer(42));
    let size_with_int = tuple.calculate_size();
    assert!(size_with_int > initial_size);
    
    tuple.add_value(ColumnValue::Text("Hello World".to_string()));
    let size_with_text = tuple.calculate_size();
    assert!(size_with_text > size_with_int);
}

#[test]
fn test_tuple_boundary_conditions() {
    // Тест с максимальным количеством полей
    let mut tuple = Tuple::new(u32::MAX);
    
    // Добавляем много значений
    for i in 0..1000 {
        tuple.add_value(ColumnValue::Integer(i));
    }
    
    assert_eq!(tuple.get_values().len(), 1000);
    assert_eq!(tuple.get_id(), u32::MAX);
}

#[test]
fn test_tuple_memory_efficiency() {
    use std::mem;
    
    let tuple = Tuple::new(1);
    let size = mem::size_of_val(&tuple);
    
    // Кортеж должен иметь разумный размер в памяти
    assert!(size < 1024); // Менее 1KB для пустого кортежа
}

#[test]
fn test_schema_serialization() {
    let mut schema = Schema::new("test_table".to_string());
    
    schema.add_column(Column {
        name: "id".to_string(),
        data_type: DataType::Integer,
        nullable: false,
        default_value: None,
    });
    
    schema.add_column(Column {
        name: "name".to_string(),
        data_type: DataType::Text,
        nullable: true,
        default_value: Some(ColumnValue::Text("Unknown".to_string())),
    });
    
    // Сериализуем схему
    let serialized = schema.serialize();
    assert!(!serialized.is_empty());
    
    // Десериализуем схему
    let deserialized = Schema::deserialize(&serialized);
    assert!(deserialized.is_ok());
    
    let new_schema = deserialized.unwrap();
    assert_eq!(new_schema.get_table_name(), "test_table");
    assert_eq!(new_schema.get_columns().len(), 2);
    assert_eq!(new_schema.get_columns()[0].name, "id");
    assert_eq!(new_schema.get_columns()[1].name, "name");
}

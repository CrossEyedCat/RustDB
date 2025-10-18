//! Примеры использования базовых структур данных RustDB

use rustdb::storage::{
    page::{Page, PageManager},
    block::{Block, BlockType, BlockManager},
    tuple::{Tuple, Schema},
    row::{Row, Table},
    schema_manager::{SchemaManager, SchemaOperation, BasicSchemaValidator},
};
use rustdb::core::buffer::{BufferManager, EvictionStrategy};
use rustdb::common::types::{DataType, Column, ColumnValue};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Примеры использования базовых структур данных RustDB ===\n");

    // Пример 1: Работа со страницами
    example_pages()?;
    
    // Пример 2: Работа с блоками
    example_blocks()?;
    
    // Пример 3: Работа с кортежами и схемами
    example_tuples_and_schemas()?;
    
    // Пример 4: Работа со строками и таблицами
    example_rows_and_tables()?;
    
    // Пример 5: Работа с менеджером буферов
    example_buffer_manager()?;
    
    // Пример 6: Работа с менеджером схем
    example_schema_manager()?;

    println!("Все примеры выполнены успешно!");
    Ok(())
}

/// Пример работы со страницами
fn example_pages() -> Result<(), Box<dyn std::error::Error>> {
    println!("1. Работа со страницами:");
    
    // Создаем новую страницу
    let mut page = Page::new(1);
    println!("  - Создана страница ID: {}, тип: {:?}", page.header.page_id, page.header.page_type);
    
    // Добавляем записи на страницу
    let record1 = b"Hello, World!";
    let record2 = b"RustDB is awesome!";
    
    let offset1 = page.add_record(record1, 1)?;
    let offset2 = page.add_record(record2, 2)?;
    
    println!("  - Добавлены записи: ID 1 (смещение {}), ID 2 (смещение {})", offset1, offset2);
    println!("  - Количество записей: {}", page.record_count());
    println!("  - Свободное место: {} байт", page.free_space());
    
    // Получаем записи
    let retrieved1 = page.get_record(1).unwrap();
    let retrieved2 = page.get_record(2).unwrap();
    
    println!("  - Получена запись 1: {}", String::from_utf8_lossy(retrieved1));
    println!("  - Получена запись 2: {}", String::from_utf8_lossy(retrieved2));
    
    // Удаляем запись
    page.delete_record(1)?;
    println!("  - Запись 1 удалена");
    
    // Проверяем, что запись удалена
    assert!(page.get_record(1).is_none());
    println!("  - Запись 1 больше не доступна");
    
    // Создаем менеджер страниц
    let mut page_manager = PageManager::new(10);
    page_manager.add_page(page);
    println!("  - Страница добавлена в менеджер");
    
    println!("  ✓ Страницы работают корректно\n");
    Ok(())
}

/// Пример работы с блоками
fn example_blocks() -> Result<(), Box<dyn std::error::Error>> {
    println!("2. Работа с блоками:");
    
    // Создаем новый блок
    let mut block = Block::new(1, BlockType::Data, 1024);
    println!("  - Создан блок ID: {}, тип: {:?}, размер: {} байт", 
             block.header.block_id, block.header.block_type, block.header.size);
    
    // Добавляем страницы в блок
    let page_data1 = vec![1, 2, 3, 4, 5];
    let page_data2 = vec![6, 7, 8, 9, 10];
    
    block.add_page(1, page_data1.clone())?;
    block.add_page(2, page_data2.clone())?;
    
    println!("  - Добавлены страницы: ID 1, ID 2");
    println!("  - Количество страниц в блоке: {}", block.page_count());
    
    // Получаем страницы
    let retrieved1 = block.get_page(1).unwrap();
    let retrieved2 = block.get_page(2).unwrap();
    
    println!("  - Получена страница 1: {:?}", retrieved1);
    println!("  - Получена страница 2: {:?}", retrieved2);
    
    // Устанавливаем связи между блоками
    block.links.set_next(2);
    block.links.set_prev(0);
    
    println!("  - Установлены связи: next={:?}, prev={:?}", 
             block.links.next_block, block.links.prev_block);
    
    // Создаем менеджер блоков
    let mut block_manager = BlockManager::new(5);
    block_manager.add_block(block);
    println!("  - Блок добавлен в менеджер");
    
    println!("  ✓ Блоки работают корректно\n");
    Ok(())
}

/// Пример работы с кортежами и схемами
fn example_tuples_and_schemas() -> Result<(), Box<dyn std::error::Error>> {
    println!("3. Работа с кортежами и схемами:");
    
    // Создаем схему таблицы пользователей
    let mut schema = Schema::new("users".to_string());
    
    // Добавляем колонки
    schema = schema
        .add_column(Column::new("id".to_string(), DataType::Integer(0)).not_null())
        .add_column(Column::new("name".to_string(), DataType::Varchar("".to_string())))
        .add_column(Column::new("age".to_string(), DataType::Integer(0)))
        .add_column(Column::new("email".to_string(), DataType::Varchar("".to_string())));
    
    // Устанавливаем первичный ключ
    schema = schema.primary_key(vec!["id".to_string()]);
    
    // Добавляем уникальное ограничение
    schema = schema.unique(vec!["email".to_string()]);
    
    println!("  - Создана схема таблицы 'users'");
    println!("  - Колонки: {:?}", schema.get_columns().iter().map(|c| &c.name).collect::<Vec<_>>());
    println!("  - Первичный ключ: {:?}", schema.base.primary_key);
    
    // Создаем кортеж
    let mut tuple = Tuple::new(1);
    tuple.set_value("id", ColumnValue::new(DataType::Integer(1)));
    tuple.set_value("name", ColumnValue::new(DataType::Varchar("John Doe".to_string())));
    tuple.set_value("age", ColumnValue::new(DataType::Integer(30)));
    tuple.set_value("email", ColumnValue::new(DataType::Varchar("john@example.com".to_string())));
    
    println!("  - Создан кортеж с ID: {}", tuple.id);
    println!("  - Значения: id={:?}, name={:?}, age={:?}, email={:?}", 
             tuple.get_value("id"), tuple.get_value("name"), 
             tuple.get_value("age"), tuple.get_value("email"));
    
    // Валидируем кортеж против схемы
    schema.validate_tuple(&tuple)?;
    println!("  - Кортеж прошел валидацию схемы");
    
    // Создаем новую версию кортежа
    let new_tuple = tuple.create_new_version();
    println!("  - Создана новая версия кортежа: {}", new_tuple.version);
    
    println!("  ✓ Кортежи и схемы работают корректно\n");
    Ok(())
}

/// Пример работы со строками и таблицами
fn example_rows_and_tables() -> Result<(), Box<dyn std::error::Error>> {
    println!("4. Работа со строками и таблицами:");
    
    // Создаем схему и таблицу
    let schema = Schema::new("products".to_string())
        .add_column(Column::new("id".to_string(), DataType::Integer(0)).not_null())
        .add_column(Column::new("name".to_string(), DataType::Varchar("".to_string())))
        .add_column(Column::new("price".to_string(), DataType::Double(0.0)))
        .primary_key(vec!["id".to_string()]);
    
    let mut table = Table::new("products".to_string(), schema);
    println!("  - Создана таблица 'products'");
    
    // Создаем и добавляем строки
    let mut tuple1 = Tuple::new(1);
    tuple1.set_value("id", ColumnValue::new(DataType::Integer(1)));
    tuple1.set_value("name", ColumnValue::new(DataType::Varchar("Laptop".to_string())));
    tuple1.set_value("price", ColumnValue::new(DataType::Double(999.99)));
    
    let mut tuple2 = Tuple::new(2);
    tuple2.set_value("id", ColumnValue::new(DataType::Integer(2)));
    tuple2.set_value("name", ColumnValue::new(DataType::Varchar("Mouse".to_string())));
    tuple2.set_value("price", ColumnValue::new(DataType::Double(29.99)));
    
    let row1 = Row::new(1, tuple1);
    let row2 = Row::new(2, tuple2);
    
    table.insert_row(row1)?;
    table.insert_row(row2)?;
    
    println!("  - Добавлены строки с ID: 1, 2");
    println!("  - Количество строк в таблице: {}", table.row_count());
    
    // Получаем строку
    let row = table.get_row(1).unwrap();
    println!("  - Получена строка 1: id={:?}, name={:?}, price={:?}", 
             row.get_value("id"), row.get_value("name"), row.get_value("price"));
    
    // Обновляем строку
    let mut new_values = std::collections::HashMap::new();
    new_values.insert("price".to_string(), ColumnValue::new(DataType::Double(899.99)));
    
    table.update_row(1, new_values)?;
    println!("  - Обновлена цена продукта 1");
    
    // Проверяем обновление
    let updated_row = table.get_row(1).unwrap();
    println!("  - Новая цена продукта 1: {:?}", updated_row.get_value("price"));
    
    // Удаляем строку
    table.delete_row(2)?;
    println!("  - Строка 2 удалена");
    println!("  - Количество строк в таблице: {}", table.row_count());
    
    println!("  ✓ Строки и таблицы работают корректно\n");
    Ok(())
}

/// Пример работы с менеджером буферов
fn example_buffer_manager() -> Result<(), Box<dyn std::error::Error>> {
    println!("5. Работа с менеджером буферов:");
    
    // Создаем менеджер буферов с LRU стратегией
    let mut buffer_manager = BufferManager::new(3, EvictionStrategy::LRU);
    println!("  - Создан менеджер буферов с максимальным размером: 3 страницы");
    
    // Создаем страницы
    let page1 = Page::new(1);
    let page2 = Page::new(2);
    let page3 = Page::new(3);
    let page4 = Page::new(4);
    
    // Добавляем страницы в буфер
    buffer_manager.add_page(page1)?;
    buffer_manager.add_page(page2)?;
    buffer_manager.add_page(page3)?;
    
    println!("  - Добавлены страницы: 1, 2, 3");
    println!("  - Количество страниц в буфере: {}", buffer_manager.page_count());
    
    // Добавляем четвертую страницу (должна вытеснить первую)
    buffer_manager.add_page(page4)?;
    println!("  - Добавлена страница 4");
    println!("  - Количество страниц в буфере: {}", buffer_manager.page_count());
    
    // Проверяем, что первая страница была вытеснена
    assert!(!buffer_manager.contains_page(1));
    assert!(buffer_manager.contains_page(2));
    assert!(buffer_manager.contains_page(3));
    assert!(buffer_manager.contains_page(4));
    
    println!("  - Страница 1 была вытеснена (LRU стратегия)");
    println!("  - Страницы 2, 3, 4 остались в буфере");
    
    // Получаем страницу (обновляем порядок LRU)
    buffer_manager.get_page(2);
    println!("  - Получена страница 2 (обновлен порядок LRU)");
    
    // Добавляем еще одну страницу (должна вытеснить страницу 3)
    let page5 = Page::new(5);
    buffer_manager.add_page(page5)?;
    
    assert!(!buffer_manager.contains_page(3));
    assert!(buffer_manager.contains_page(2));
    assert!(buffer_manager.contains_page(4));
    assert!(buffer_manager.contains_page(5));
    
    println!("  - Страница 3 была вытеснена");
    println!("  - Страницы 2, 4, 5 остались в буфере");
    
    // Получаем статистику
    let stats = buffer_manager.get_stats();
    println!("  - Статистика буфера: hit_ratio={:.2}, total_accesses={}", 
             stats.hit_ratio(), stats.total_accesses);
    
    // Меняем стратегию вытеснения
    buffer_manager.set_eviction_strategy(EvictionStrategy::Clock);
    println!("  - Стратегия вытеснения изменена на Clock");
    
    println!("  ✓ Менеджер буферов работает корректно\n");
    Ok(())
}

/// Пример работы с менеджером схем
fn example_schema_manager() -> Result<(), Box<dyn std::error::Error>> {
    println!("6. Работа с менеджером схем:");
    
    // Создаем менеджер схем
    let mut schema_manager = SchemaManager::new();
    
    // Регистрируем валидатор
    let validator = Box::new(BasicSchemaValidator);
    schema_manager.register_validator(validator);
    println!("  - Зарегистрирован валидатор схем");
    
    // Создаем схему таблицы
    let schema = Schema::new("employees".to_string())
        .add_column(Column::new("id".to_string(), DataType::Integer(0)).not_null())
        .add_column(Column::new("name".to_string(), DataType::Varchar("".to_string())))
        .add_column(Column::new("department".to_string(), DataType::Varchar("".to_string())))
        .primary_key(vec!["id".to_string()]);
    
    // Создаем схему в менеджере
    schema_manager.create_schema("employees".to_string(), schema)?;
    println!("  - Создана схема таблицы 'employees'");
    
    // Выполняем ALTER TABLE операции
    let new_column = Column::new("salary".to_string(), DataType::Double(0.0));
    let add_column_op = SchemaOperation::AddColumn {
        column: new_column,
        after: Some("department".to_string()),
    };
    
    schema_manager.alter_table("employees", add_column_op)?;
    println!("  - Добавлена колонка 'salary'");
    
    // Проверяем, что колонка добавлена
    let updated_schema = schema_manager.get_schema("employees").unwrap();
    assert!(updated_schema.has_column("salary"));
    println!("  - Колонка 'salary' успешно добавлена");
    
    // Добавляем индекс
    let add_index_op = SchemaOperation::AddIndex {
        index_name: "idx_department".to_string(),
        columns: vec!["department".to_string()],
        unique: false,
    };
    
    schema_manager.alter_table("employees", add_index_op)?;
    println!("  - Добавлен индекс 'idx_department'");
    
    // Проверяем, что индекс добавлен
    let final_schema = schema_manager.get_schema("employees").unwrap();
    let index_exists = final_schema.base.indexes.iter().any(|i| i.name == "idx_department");
    assert!(index_exists);
    println!("  - Индекс 'idx_department' успешно добавлен");
    
    // Получаем историю изменений
    let history = schema_manager.get_change_history();
    println!("  - История изменений: {} записей", history.len());
    
    for (i, change) in history.iter().enumerate() {
        println!("    {}. {}: {} ({})", i + 1, change.operation_type, change.description, change.table_name);
    }
    
    println!("  ✓ Менеджер схем работает корректно\n");
    Ok(())
}

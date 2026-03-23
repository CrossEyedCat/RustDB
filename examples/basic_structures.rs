//! Examples of using basic RustDB data structures

use rustdb::common::types::{Column, ColumnValue, DataType};
use rustdb::core::buffer::{BufferManager, EvictionStrategy};
use rustdb::storage::{
    block::{Block, BlockManager, BlockType},
    page::{Page, PageManager},
    row::{Row, Table},
    schema_manager::{BasicSchemaValidator, SchemaManager, SchemaOperation},
    tuple::{Schema, Tuple},
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Examples of using basic RustDB data structures ===\n");

    // Example 1: Working with pages
    example_pages()?;

    // Example 2: Working with blocks
    example_blocks()?;

    // Example 3: Working with Tuples and Schemas
    example_tuples_and_schemas()?;

    // Example 4: Working with Rows and Tables
    example_rows_and_tables()?;

    // Example 5: Working with the Buffer Manager
    example_buffer_manager()?;

    // Example 6: Working with the Schema Manager
    example_schema_manager()?;

    println!("All examples completed successfully!");
    Ok(())
}

// / Example of working with pages
fn example_pages() -> Result<(), Box<dyn std::error::Error>> {
    println!("1. Working with pages:");

    // Create a new page
    let mut page = Page::new(1);
    println!(
        "- Created page ID: {}, type: {:?}",
        page.header.page_id, page.header.page_type
    );

    // Adding posts to the page
    let record1 = b"Hello, World!";
    let record2 = b"RustDB is awesome!";

    let offset1 = page.add_record(record1, 1)?;
    let offset2 = page.add_record(record2, 2)?;

    println!(
        "- Added entries: ID 1 (offset {}), ID 2 (offset {})",
        offset1, offset2
    );
    println!("- Number of entries: {}", page.record_count());
    println!("- Free space: {} bytes", page.free_space());

    // Getting records
    let retrieved1 = page.get_record(1).unwrap();
    let retrieved2 = page.get_record(2).unwrap();

    println!(
        "- Record 1 received: {}",
        String::from_utf8_lossy(retrieved1)
    );
    println!(
        "- Record 2 received: {}",
        String::from_utf8_lossy(retrieved2)
    );

    // Deleting an entry
    page.delete_record(1)?;
    println!("- Entry 1 deleted");

    // Checking that the entry has been deleted
    assert!(page.get_record(1).is_none());
    println!("- Entry 1 is no longer available");

    // Creating a page manager
    let mut page_manager = PageManager::new(10);
    page_manager.add_page(page);
    println!("- The page has been added to the manager");

    println!("✓ Pages work correctly\n");
    Ok(())
}

// / Example of working with blocks
fn example_blocks() -> Result<(), Box<dyn std::error::Error>> {
    println!("2. Working with blocks:");

    // Create a new block
    let mut block = Block::new(1, BlockType::Data, 1024);
    println!(
        "- Created block ID: {}, type: {:?}, size: {} bytes",
        block.header.block_id, block.header.block_type, block.header.size
    );

    // Adding pages to a block
    let page_data1 = vec![1, 2, 3, 4, 5];
    let page_data2 = vec![6, 7, 8, 9, 10];

    block.add_page(1, page_data1.clone())?;
    block.add_page(2, page_data2.clone())?;

    println!("- Added pages: ID 1, ID 2");
    println!("- Number of pages in a block: {}", block.page_count());

    // Getting pages
    let retrieved1 = block.get_page(1).unwrap();
    let retrieved2 = block.get_page(2).unwrap();

    println!("- Received page 1: {:?}", retrieved1);
    println!("- Received page 2: {:?}", retrieved2);

    // Establishing connections between blocks
    block.links.set_next(2);
    block.links.set_prev(0);

    println!(
        "- Connections established: next={:?}, prev={:?}",
        block.links.next_block, block.links.prev_block
    );

    // Creating a block manager
    let mut block_manager = BlockManager::new(5);
    block_manager.add_block(block);
    println!("- Block added to manager");

    println!("✓ Blocks work correctly\n");
    Ok(())
}

// / Example of working with tuples and schemas
fn example_tuples_and_schemas() -> Result<(), Box<dyn std::error::Error>> {
    println!("3. Working with tuples and schemas:");

    // Creating a user table schema
    let mut schema = Schema::new("users".to_string());

    // Adding columns
    schema = schema
        .add_column(Column::new("id".to_string(), DataType::Integer(0)).not_null())
        .add_column(Column::new(
            "name".to_string(),
            DataType::Varchar("".to_string()),
        ))
        .add_column(Column::new("age".to_string(), DataType::Integer(0)))
        .add_column(Column::new(
            "email".to_string(),
            DataType::Varchar("".to_string()),
        ));

    // Setting the primary key
    schema = schema.primary_key(vec!["id".to_string()]);

    // Adding a unique constraint
    schema = schema.unique(vec!["email".to_string()]);

    println!("- Created table schema 'users'");
    println!(
        "- Columns: {:?}",
        schema
            .get_columns()
            .iter()
            .map(|c| &c.name)
            .collect::<Vec<_>>()
    );
    println!("- Primary key: {:?}", schema.base.primary_key);

    // Create a tuple
    let mut tuple = Tuple::new(1);
    tuple.set_value("id", ColumnValue::new(DataType::Integer(1)));
    tuple.set_value(
        "name",
        ColumnValue::new(DataType::Varchar("John Doe".to_string())),
    );
    tuple.set_value("age", ColumnValue::new(DataType::Integer(30)));
    tuple.set_value(
        "email",
        ColumnValue::new(DataType::Varchar("john@example.com".to_string())),
    );

    println!("- Created a tuple with ID: {}", tuple.id);
    println!(
        "- Values: id={:?}, name={:?}, age={:?}, email={:?}",
        tuple.get_value("id"),
        tuple.get_value("name"),
        tuple.get_value("age"),
        tuple.get_value("email")
    );

    // Validating a tuple against a schema
    schema.validate_tuple(&tuple)?;
    println!("- The tuple has passed schema validation");

    // Create a new version of the tuple
    let new_tuple = tuple.create_new_version();
    println!(
        "- A new version of the tuple has been created: {}",
        new_tuple.version
    );

    println!("✓ Tuples and schemas work correctly\n");
    Ok(())
}

// / Example of working with rows and tables
fn example_rows_and_tables() -> Result<(), Box<dyn std::error::Error>> {
    println!("4. Working with rows and tables:");

    // Create a diagram and table
    let schema = Schema::new("products".to_string())
        .add_column(Column::new("id".to_string(), DataType::Integer(0)).not_null())
        .add_column(Column::new(
            "name".to_string(),
            DataType::Varchar("".to_string()),
        ))
        .add_column(Column::new("price".to_string(), DataType::Double(0.0)))
        .primary_key(vec!["id".to_string()]);

    let mut table = Table::new("products".to_string(), schema);
    println!("- The 'products' table has been created");

    // Create and add lines
    let mut tuple1 = Tuple::new(1);
    tuple1.set_value("id", ColumnValue::new(DataType::Integer(1)));
    tuple1.set_value(
        "name",
        ColumnValue::new(DataType::Varchar("Laptop".to_string())),
    );
    tuple1.set_value("price", ColumnValue::new(DataType::Double(999.99)));

    let mut tuple2 = Tuple::new(2);
    tuple2.set_value("id", ColumnValue::new(DataType::Integer(2)));
    tuple2.set_value(
        "name",
        ColumnValue::new(DataType::Varchar("Mouse".to_string())),
    );
    tuple2.set_value("price", ColumnValue::new(DataType::Double(29.99)));

    let row1 = Row::new(1, tuple1);
    let row2 = Row::new(2, tuple2);

    table.insert_row(row1)?;
    table.insert_row(row2)?;

    println!("- Added lines with ID: 1, 2");
    println!("- Number of rows in the table: {}", table.row_count());

    // We get the string
    let row = table.get_row(1).unwrap();
    println!(
        "- Received line 1: id={:?}, name={:?}, price={:?}",
        row.get_value("id"),
        row.get_value("name"),
        row.get_value("price")
    );

    // Update the line
    let mut new_values = std::collections::HashMap::new();
    new_values.insert(
        "price".to_string(),
        ColumnValue::new(DataType::Double(899.99)),
    );

    table.update_row(1, new_values)?;
    println!("- Updated price of product 1");

    // Checking the update
    let updated_row = table.get_row(1).unwrap();
    println!(
        "- New price for product 1: {:?}",
        updated_row.get_value("price")
    );

    // Delete a line
    table.delete_row(2)?;
    println!("- Line 2 deleted");
    println!("- Number of rows in the table: {}", table.row_count());

    println!("✓ Rows and tables work correctly\n");
    Ok(())
}

// / Example of working with the buffer manager
fn example_buffer_manager() -> Result<(), Box<dyn std::error::Error>> {
    println!("5. Working with the buffer manager:");

    // Creating a buffer manager with an LRU strategy
    let mut buffer_manager = BufferManager::new(3, EvictionStrategy::LRU);
    println!("- Created a buffer manager with a maximum size of 3 pages");

    // Creating pages
    let page1 = Page::new(1);
    let page2 = Page::new(2);
    let page3 = Page::new(3);
    let page4 = Page::new(4);

    // Adding pages to the buffer
    buffer_manager.add_page(page1)?;
    buffer_manager.add_page(page2)?;
    buffer_manager.add_page(page3)?;

    println!("- Added pages: 1, 2, 3");
    println!(
        "- Number of pages in buffer: {}",
        buffer_manager.page_count()
    );

    // Add a fourth page (should displace the first)
    buffer_manager.add_page(page4)?;
    println!("- Added page 4");
    println!(
        "- Number of pages in buffer: {}",
        buffer_manager.page_count()
    );

    // Checking that the first page has been evicted
    assert!(!buffer_manager.contains_page(1));
    assert!(buffer_manager.contains_page(2));
    assert!(buffer_manager.contains_page(3));
    assert!(buffer_manager.contains_page(4));

    println!("- Page 1 was evicted (LRU strategy)");
    println!("- Pages 2, 3, 4 remained in the buffer");

    // Getting the page (updating the LRU order)
    buffer_manager.get_page(2);
    println!("- Page 2 received (LRU order updated)");

    // Add another page (should displace page 3)
    let page5 = Page::new(5);
    buffer_manager.add_page(page5)?;

    assert!(!buffer_manager.contains_page(3));
    assert!(buffer_manager.contains_page(2));
    assert!(buffer_manager.contains_page(4));
    assert!(buffer_manager.contains_page(5));

    println!("- Page 3 has been supplanted");
    println!("- Pages 2, 4, 5 remained in the buffer");

    // Getting statistics
    let stats = buffer_manager.get_stats();
    println!(
        "- Buffer statistics: hit_ratio={:.2}, total_accesses={}",
        stats.hit_ratio(),
        stats.total_accesses
    );

    // Changing the displacement strategy
    buffer_manager.set_eviction_strategy(EvictionStrategy::Clock);
    println!("- Preemption strategy changed to Clock");

    println!("✓ Buffer manager works correctly\n");
    Ok(())
}

// / Example of working with the schema manager
fn example_schema_manager() -> Result<(), Box<dyn std::error::Error>> {
    println!("6. Working with the schema manager:");

    // Creating a schema manager
    let mut schema_manager = SchemaManager::new();

    // Registering the validator
    let validator = Box::new(BasicSchemaValidator);
    schema_manager.register_validator(validator);
    println!("- Registered schema validator");

    // Creating a table schema
    let schema = Schema::new("employees".to_string())
        .add_column(Column::new("id".to_string(), DataType::Integer(0)).not_null())
        .add_column(Column::new(
            "name".to_string(),
            DataType::Varchar("".to_string()),
        ))
        .add_column(Column::new(
            "department".to_string(),
            DataType::Varchar("".to_string()),
        ))
        .primary_key(vec!["id".to_string()]);

    // Creating a schema in the manager
    schema_manager.create_schema("employees".to_string(), schema)?;
    println!("- Created table schema 'employees'");

    // Performing ALTER TABLE operations
    let new_column = Column::new("salary".to_string(), DataType::Double(0.0));
    let add_column_op = SchemaOperation::AddColumn {
        column: new_column,
        after: Some("department".to_string()),
    };

    schema_manager.alter_table("employees", add_column_op)?;
    println!("- Added 'salary' column");

    // Checking that the column has been added
    let updated_schema = schema_manager.get_schema("employees").unwrap();
    assert!(updated_schema.has_column("salary"));
    println!("- 'salary' column added successfully");

    // Adding an index
    let add_index_op = SchemaOperation::AddIndex {
        index_name: "idx_department".to_string(),
        columns: vec!["department".to_string()],
        unique: false,
    };

    schema_manager.alter_table("employees", add_index_op)?;
    println!("- Added index 'idx_department'");

    // Checking that the index has been added
    let final_schema = schema_manager.get_schema("employees").unwrap();
    let index_exists = final_schema
        .base
        .indexes
        .iter()
        .any(|i| i.name == "idx_department");
    assert!(index_exists);
    println!("- Index 'idx_department' added successfully");

    // Getting the history of changes
    let history = schema_manager.get_change_history();
    println!("- History of changes: {} entries", history.len());

    for (i, change) in history.iter().enumerate() {
        println!(
            "    {}. {}: {} ({})",
            i + 1,
            change.operation_type,
            change.description,
            change.table_name
        );
    }

    println!("✓ Scheme manager works correctly\n");
    Ok(())
}

//! Example of using rustdb page manager
//!
//! Demonstrates the main features of PageManager:
//! - Creating and opening a page manager
//! - CRUD operations (insert, read, update, delete)
//! - Batch operations
//! - Page defragmentation
//! - Statistics monitoring

use rustdb::storage::page_manager::{PageManager, PageManagerConfig};
use std::path::Path;
use tempfile::TempDir;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🗄️ Example of using PageManager rustdb");
    println!("{}", "=".repeat(50));

    // Create a temporary directory for example
    let temp_dir = TempDir::new()?;
    let data_dir = temp_dir.path().to_path_buf();
    println!("📁 Working directory: {:?}", data_dir);

    // Demonstrating the creation of PageManager
    demo_create_page_manager(&data_dir)?;

    // Demonstrating CRUD operations
    demo_crud_operations(&data_dir)?;

    // Demonstrating batch operations
    demo_batch_operations(&data_dir)?;

    // Demonstrating defragmentation
    demo_defragmentation(&data_dir)?;

    // Demonstrating the opening of an existing manager
    demo_open_existing_manager(&data_dir)?;

    println!("\n✅ All demos completed successfully!");

    Ok(())
}

// / Demonstrates creating a PageManager with various configurations
fn demo_create_page_manager(data_dir: &Path) -> Result<(), Box<dyn std::error::Error>> {
    println!("\n🔧 Demonstration of creating a PageManager");
    println!("{}", "-".repeat(30));

    // Create a manager with default configuration
    let default_config = PageManagerConfig::default();
    println!("📊 Default configuration:");
    println!("   - max_fill_factor: {}", default_config.max_fill_factor);
    println!("   - min_fill_factor: {}", default_config.min_fill_factor);
    println!(
        "   - preallocation_buffer_size: {}",
        default_config.preallocation_buffer_size
    );
    println!(
        "   - enable_compression: {}",
        default_config.enable_compression
    );
    println!("   - batch_size: {}", default_config.batch_size);

    let manager_result = PageManager::new(data_dir.to_path_buf(), "demo_table", default_config);
    match manager_result {
        Ok(_manager) => {
            println!("✅ PageManager created successfully");
        }
        Err(e) => {
            println!("❌ Error creating PageManager: {}", e);
            return Ok(());
        }
    }

    // Creating a manager with a custom configuration
    let custom_config = PageManagerConfig {
        max_fill_factor: 0.85,
        min_fill_factor: 0.25,
        preallocation_buffer_size: 20,
        enable_compression: true,
        batch_size: 200,
        buffer_pool_size: 1000,
        flush_on_commit: true,
        batch_flush_size: 10,
        use_async_flush: true,
        defer_data_flush: false,
        flush_interval_ms: 0,
    };

    println!("\n📊 Custom configuration:");
    println!("   - max_fill_factor: {}", custom_config.max_fill_factor);
    println!("   - min_fill_factor: {}", custom_config.min_fill_factor);
    println!(
        "   - preallocation_buffer_size: {}",
        custom_config.preallocation_buffer_size
    );
    println!(
        "   - enable_compression: {}",
        custom_config.enable_compression
    );
    println!("   - batch_size: {}", custom_config.batch_size);

    let custom_manager_result =
        PageManager::new(data_dir.to_path_buf(), "custom_table", custom_config);
    match custom_manager_result {
        Ok(_manager) => {
            println!("✅ PageManager with custom configuration created successfully");
        }
        Err(e) => {
            println!("❌ Error creating custom PageManager: {}", e);
        }
    }

    Ok(())
}

// / Demonstrates CRUD operations
fn demo_crud_operations(data_dir: &Path) -> Result<(), Box<dyn std::error::Error>> {
    println!("\n📝Demonstration of CRUD operations");
    println!("{}", "-".repeat(30));

    let config = PageManagerConfig::default();
    let manager_result = PageManager::new(data_dir.to_path_buf(), "crud_table", config);

    let mut manager = match manager_result {
        Ok(mgr) => mgr,
        Err(e) => {
            println!("❌ Failed to create PageManager: {}", e);
            return Ok(());
        }
    };

    // CREATE (INSERT) operations
    println!("📥 Inserting records:");
    let records = [
        "Alice Johnson - Software Engineer".as_bytes(),
        "Bob Smith - Data Analyst".as_bytes(),
        "Carol Davis - Project Manager".as_bytes(),
        "David Wilson - DevOps Engineer".as_bytes(),
    ];

    let mut record_ids = Vec::new();
    for (i, record) in records.iter().enumerate() {
        match manager.insert(record) {
            Ok(insert_result) => {
                println!(
                    "✅ Post {}: ID {}, Page {}",
                    i + 1,
                    insert_result.record_id,
                    insert_result.page_id
                );
                record_ids.push(insert_result.record_id);
            }
            Err(e) => {
                println!("❌ Error inserting record {}: {}", i + 1, e);
            }
        }
    }

    // READ (SELECT) operations
    println!("\n📤 Reading all entries:");
    match manager.select(None) {
        Ok(all_records) => {
            println!("📊 {} records found:", all_records.len());
            for (i, (record_id, data)) in all_records.iter().enumerate() {
                let data_str = String::from_utf8_lossy(data);
                println!("{} - ID: {}, Data: {}", i + 1, record_id, data_str);
            }
        }
        Err(e) => {
            println!("❌ Error reading records: {}", e);
        }
    }

    // Conditional Reading
    println!("\n🔍 Reading entries with filter (contain 'Engineer'):");
    let condition = Box::new(|data: &[u8]| String::from_utf8_lossy(data).contains("Engineer"));

    match manager.select(Some(condition)) {
        Ok(filtered_records) => {
            println!(
                "📊 Found {} records with 'Engineer':",
                filtered_records.len()
            );
            for (i, (record_id, data)) in filtered_records.iter().enumerate() {
                let data_str = String::from_utf8_lossy(data);
                println!("{} - ID: {}, Data: {}", i + 1, record_id, data_str);
            }
        }
        Err(e) => {
            println!("❌ Filtered read error: {}", e);
        }
    }

    // UPDATE operations
    if !record_ids.is_empty() {
        println!("\n✏️ Post update:");
        let record_to_update = record_ids[0];
        let new_data = "Alice Johnson - Senior Software Engineer (Updated)".as_bytes();

        match manager.update(record_to_update, new_data) {
            Ok(update_result) => {
                println!("✅ Entry {} updated", record_to_update);
                if update_result.in_place {
                    println!("📍 Upgrade done on site");
                } else {
                    println!("🔄 Post moved to page {:?}", update_result.new_page_id);
                }
            }
            Err(e) => {
                println!("❌ Update error: {}", e);
            }
        }
    }

    // DELETE operations
    if record_ids.len() > 1 {
        println!("\n🗑️ Deleting an entry:");
        let record_to_delete = record_ids[1];

        match manager.delete(record_to_delete) {
            Ok(delete_result) => {
                println!("✅ Entry {} deleted", record_to_delete);
                if delete_result.physical_delete {
                    println!("🗑️ Physical removal");
                } else {
                    println!("👻 Logical removal");
                }
                if delete_result.page_merge {
                    println!("🔄 Pages merged");
                }
            }
            Err(e) => {
                println!("❌ Deletion error: {}", e);
            }
        }
    }

    // Showing statistics
    let stats = manager.get_statistics();
    println!("\n📈 Operation statistics:");
    println!("- Inserts: {}", stats.insert_operations);
    println!("- Readings: {}", stats.select_operations);
    println!("- Updates: {}", stats.update_operations);
    println!("- Deletions: {}", stats.delete_operations);

    Ok(())
}

// / Demonstrates batch operations
fn demo_batch_operations(data_dir: &Path) -> Result<(), Box<dyn std::error::Error>> {
    println!("\n📦 Demonstration of batch operations");
    println!("{}", "-".repeat(30));

    let config = PageManagerConfig {
        batch_size: 10,
        ..PageManagerConfig::default()
    };

    let manager_result = PageManager::new(data_dir.to_path_buf(), "batch_table", config);
    let mut manager = match manager_result {
        Ok(mgr) => mgr,
        Err(e) => {
            println!("❌ Failed to create PageManager: {}", e);
            return Ok(());
        }
    };

    // Preparing data for batch insertion
    let batch_data: Vec<Vec<u8>> = (1..=25)
        .map(|i| format!("Batch Record #{:03} - Generated Data", i).into_bytes())
        .collect();

    println!("📥 Batch insert {} records:", batch_data.len());

    match manager.batch_insert(batch_data.clone()) {
        Ok(results) => {
            println!("✅ {} records successfully processed", results.len());

            let mut page_splits = 0;
            for result in &results {
                if result.page_split {
                    page_splits += 1;
                }
            }

            if page_splits > 0 {
                println!("🔄 {} page splits occurred", page_splits);
            }

            // Showing the first few entries
            println!("📋 First entries:");
            for (i, result) in results.iter().take(5).enumerate() {
                println!(
                    "{} - ID: {}, Page: {}",
                    i + 1,
                    result.record_id,
                    result.page_id
                );
            }

            if results.len() > 5 {
                println!("... and {} more entries", results.len() - 5);
            }
        }
        Err(e) => {
            println!("❌ Batch insert error: {}", e);
        }
    }

    // Checking the result
    match manager.select(None) {
        Ok(all_records) => {
            println!(
                "📊 Total number of records in the table: {}",
                all_records.len()
            );
        }
        Err(e) => {
            println!("❌ Error checking records: {}", e);
        }
    }

    let stats = manager.get_statistics();
    println!("📈 Statistics of batch operations:");
    println!("- Total insertions: {}", stats.insert_operations);
    println!("- Page divisions: {}", stats.page_splits);

    Ok(())
}

// / Demonstrates page defragmentation
fn demo_defragmentation(data_dir: &Path) -> Result<(), Box<dyn std::error::Error>> {
    println!("\n🔧Demonstration of defragmentation");
    println!("{}", "-".repeat(30));

    let config = PageManagerConfig::default();
    let manager_result = PageManager::new(data_dir.to_path_buf(), "defrag_table", config);
    let mut manager = match manager_result {
        Ok(mgr) => mgr,
        Err(e) => {
            println!("❌ Failed to create PageManager: {}", e);
            return Ok(());
        }
    };

    // Inserting records
    println!("📥Create entries to demonstrate fragmentation:");
    let mut record_ids = Vec::new();

    for i in 1..=15 {
        let data = format!("Fragmentation Test Record #{:02}", i).into_bytes();
        match manager.insert(&data) {
            Ok(result) => {
                record_ids.push(result.record_id);
            }
            Err(e) => {
                println!("❌ Error inserting record {}: {}", i, e);
            }
        }
    }

    println!("✅ {} records created", record_ids.len());

    // Deleting every second record to create fragmentation
    println!("\n🗑️ Deleting every second record (creating fragmentation):");
    let mut deleted_count = 0;

    for (i, &record_id) in record_ids.iter().enumerate() {
        if i % 2 == 1 {
            // Deleting records with odd indices
            match manager.delete(record_id) {
                Ok(_) => {
                    deleted_count += 1;
                }
                Err(e) => {
                    println!("❌ Error deleting entry {}: {}", record_id, e);
                }
            }
        }
    }

    println!("✅ {} entries removed", deleted_count);

    // Showing statistics before defragmentation
    let stats_before = manager.get_statistics();
    println!("\n📊 Statistics before defragmentation:");
    println!(
        "- Defragmentation operations: {}",
        stats_before.defragmentation_operations
    );

    // Performing defragmentation
    println!("\n🔧 Performing defragmentation:");
    match manager.defragment() {
        Ok(defragmented_count) => {
            println!("✅ Defragmented pages: {}", defragmented_count);
        }
        Err(e) => {
            println!("❌ Defragmentation error: {}", e);
        }
    }

    // Showing statistics after defragmentation
    let stats_after = manager.get_statistics();
    println!("\n📊 Statistics after defragmentation:");
    println!(
        "- Defragmentation operations: {}",
        stats_after.defragmentation_operations
    );
    println!("- Total insertions: {}", stats_after.insert_operations);
    println!("- Total deletions: {}", stats_after.delete_operations);

    Ok(())
}

// / Demonstrates opening an existing manager
fn demo_open_existing_manager(data_dir: &Path) -> Result<(), Box<dyn std::error::Error>> {
    println!("\n🔓 Demonstration of opening an existing manager");
    println!("{}", "-".repeat(30));

    let table_name = "persistent_table";
    let config = PageManagerConfig::default();

    // Create a manager and add data
    println!("📝 Creating a new manager and adding data:");
    {
        let manager_result = PageManager::new(data_dir.to_path_buf(), table_name, config.clone());
        match manager_result {
            Ok(mut manager) => {
                // Adding multiple entries
                let persistent_data = [
                    "Persistent Record 1 - Will survive restart".as_bytes(),
                    "Persistent Record 2 - Stored on disk".as_bytes(),
                    "Persistent Record 3 - Available after reopen".as_bytes(),
                ];

                for (i, data) in persistent_data.iter().enumerate() {
                    match manager.insert(data) {
                        Ok(result) => {
                            println!("✅ Record {}: ID {}", i + 1, result.record_id);
                        }
                        Err(e) => {
                            println!("❌ Write error {}: {}", i + 1, e);
                        }
                    }
                }

                let stats = manager.get_statistics();
                println!("📊 Inserted records: {}", stats.insert_operations);
            }
            Err(e) => {
                println!("❌ Failed to create manager: {}", e);
                return Ok(());
            }
        }
    }

    // Open an existing manager
    println!("\n🔓 Opening an existing manager:");
    match PageManager::open(data_dir.to_path_buf(), table_name, config) {
        Ok(mut manager) => {
            println!("✅ Manager successfully opened");

            // Checking that the data has been saved
            match manager.select(None) {
                Ok(records) => {
                    println!("📊 Found {} saved entries:", records.len());
                    for (i, (record_id, data)) in records.iter().enumerate() {
                        let data_str = String::from_utf8_lossy(data);
                        println!("{} - ID: {}, Data: {}", i + 1, record_id, data_str);
                    }
                }
                Err(e) => {
                    println!("❌ Error reading saved data: {}", e);
                }
            }

            // Add a new entry
            match manager.insert("New Record - Added after reopen".as_bytes()) {
                Ok(result) => {
                    println!("✅ New entry added: ID {}", result.record_id);
                }
                Err(e) => {
                    println!("❌ Error adding a new entry: {}", e);
                }
            }
        }
        Err(e) => {
            println!("❌ Failed to open existing manager: {}", e);
        }
    }

    Ok(())
}

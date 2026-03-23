//! Example of using rustdb file manager
//!
//! This example demonstrates:
//! - Creation of database files
//! - Write and read data blocks
//! - File size management
//! - Working with file headers

use rustdb::common::Result;
use rustdb::storage::file_manager::{FileManager, BLOCK_SIZE};
use tempfile::TempDir;

fn main() -> Result<()> {
    println!("=== Example of using the rustdb file manager ===\n");

    // Create a temporary directory for example
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path();

    println!(
        "📁 Create a file manager in the directory: {}",
        db_path.display()
    );
    let mut file_manager = FileManager::new(db_path)?;

    // Create a new database file
    println!("\n🗄️ Create a new database file 'example.db'");
    let file_id = file_manager.create_file("example.db")?;
    println!("✅ File created with ID: {}", file_id);

    // Getting information about the file
    if let Some(file_info) = file_manager.get_file_info(file_id) {
        println!("📊 File information:");
        println!("- Path: {}", file_info.path.display());
        println!("- Size in blocks: {}", file_info.size_in_blocks());
        println!("- Blocks used: {}", file_info.used_blocks());
        println!("- Free blocks: {}", file_info.free_blocks());
    }

    // Creating test data for recording
    println!("\n📝 We write test data into blocks:");

    // Block 0: String Data
    let mut block0_data = vec![0u8; BLOCK_SIZE];
    let text = "Hello World! This is a test of the rustdb file manager.";
    let text_bytes = text.as_bytes();
    block0_data[..text_bytes.len()].copy_from_slice(text_bytes);
    file_manager.write_block(file_id, 0, &block0_data)?;
    println!("✅ Block 0: written text '{}'", text);

    // Unit 1: Numerical Data
    let mut block1_data = vec![0u8; BLOCK_SIZE];
    for (i, byte) in block1_data.iter_mut().enumerate().take(256) {
        *byte = (i % 256) as u8;
    }
    file_manager.write_block(file_id, 1, &block1_data)?;
    println!("✅ Block 1: a sequence of bytes 0-255 is written");

    // Block 2: Random Data
    let block2_data: Vec<u8> = (0..BLOCK_SIZE)
        .map(|i| ((i * 7 + 13) % 256) as u8)
        .collect();
    file_manager.write_block(file_id, 2, &block2_data)?;
    println!("✅ Block 2: pseudo-random data recorded");

    // Synchronizing data to disk
    println!("\n💾 Synchronizing data to disk");
    file_manager.sync_file(file_id)?;
    println!("✅ Data is synchronized");

    // Getting updated information about the file
    if let Some(file_info) = file_manager.get_file_info(file_id) {
        println!("\n📊 Updated file information:");
        println!("- Size in blocks: {}", file_info.size_in_blocks());
        println!("- Blocks used: {}", file_info.used_blocks());
        println!("- Free blocks: {}", file_info.free_blocks());
    }

    // Reading the data back
    println!("\n📖 Reading data from blocks:");

    // Reading block 0
    let read_block0 = file_manager.read_block(file_id, 0)?;
    let text_end = read_block0
        .iter()
        .position(|&x| x == 0)
        .unwrap_or(text_bytes.len());
    let read_text = String::from_utf8_lossy(&read_block0[..text_end]);
    println!("📄 Block 0: '{}'", read_text);

    // Reading block 1
    let read_block1 = file_manager.read_block(file_id, 1)?;
    let first_10_bytes: Vec<u8> = read_block1[..10].to_vec();
    println!("🔢 Block 1: first 10 bytes: {:?}", first_10_bytes);

    // Reading block 2
    let read_block2 = file_manager.read_block(file_id, 2)?;
    let checksum: u32 = read_block2.iter().map(|&x| x as u32).sum();
    println!("🎲 Block 2: checksum: {}", checksum);

    // Checking data integrity
    println!("\n🔍 Checking the integrity of the data:");
    let block1_valid = read_block1[..256]
        .iter()
        .enumerate()
        .all(|(i, &byte)| byte == (i % 256) as u8);
    println!("✅ Block 1 is correct: {}", block1_valid);

    let block2_valid = read_block2
        .iter()
        .enumerate()
        .all(|(i, &byte)| byte == ((i * 7 + 13) % 256) as u8);
    println!("✅ Block 2 is correct: {}", block2_valid);

    // Closing the file
    println!("\n🚪Close the file");
    file_manager.close_file(file_id)?;
    println!("✅ File closed");

    // Re-open the file to check persistence
    println!("\n🔄 Re-open the file to check persistence");
    let reopened_file_id = file_manager.open_file("example.db", false)?;
    println!("✅ File opened with ID: {}", reopened_file_id);

    // Checking that the data has been saved
    let persistent_block0 = file_manager.read_block(reopened_file_id, 0)?;
    let persistent_text_end = persistent_block0
        .iter()
        .position(|&x| x == 0)
        .unwrap_or(text_bytes.len());
    let persistent_text = String::from_utf8_lossy(&persistent_block0[..persistent_text_end]);
    println!("📄 Persistent block 0 data: '{}'", persistent_text);

    // Demonstrating how to work with multiple files
    println!("\n📚 Create a second file to demonstrate multi-file work");
    let file2_id = file_manager.create_file("second.db")?;

    let file2_data = "This is the data in the second database file!".as_bytes();
    let mut block_data = vec![0u8; BLOCK_SIZE];
    block_data[..file2_data.len()].copy_from_slice(file2_data);
    file_manager.write_block(file2_id, 0, &block_data)?;

    let read_file2_data = file_manager.read_block(file2_id, 0)?;
    let file2_text_end = read_file2_data
        .iter()
        .position(|&x| x == 0)
        .unwrap_or(file2_data.len());
    let file2_text = String::from_utf8_lossy(&read_file2_data[..file2_text_end]);
    println!("📄 Second file data: '{}'", file2_text);

    // Showing a list of open files
    let open_files = file_manager.list_open_files();
    println!("\n📋 List of open files: {:?}", open_files);

    // Close all files
    println!("\n🚪 Close all files");
    file_manager.close_all()?;
    println!("✅ All files are closed");

    println!("\n🎉 Example completed successfully!");
    println!("\nThis example demonstrated:");
    println!("• Create and manage database files");
    println!("• Writing and reading data blocks of various types");
    println!("• Data integrity check");
    println!("• Data persistence between sessions");
    println!("• Work with several files simultaneously");
    println!("• Data synchronization to disk");

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_file_manager_example() -> Result<()> {
        // Run the main function as a test
        main()
    }

    #[test]
    fn test_multiple_files_operations() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let mut file_manager = FileManager::new(temp_dir.path())?;

        // Create several files
        let file1_id = file_manager.create_file("test1.db")?;
        let file2_id = file_manager.create_file("test2.db")?;
        let file3_id = file_manager.create_file("test3.db")?;

        // We write unique data to each file
        for (i, &file_id) in [file1_id, file2_id, file3_id].iter().enumerate() {
            let data = vec![(i + 1) as u8; BLOCK_SIZE];
            file_manager.write_block(file_id, 0, &data)?;
        }

        // We check that the data in each file is unique
        for (i, &file_id) in [file1_id, file2_id, file3_id].iter().enumerate() {
            let read_data = file_manager.read_block(file_id, 0)?;
            assert_eq!(read_data[0], (i + 1) as u8);
            assert!(read_data.iter().all(|&x| x == (i + 1) as u8));
        }

        file_manager.close_all()?;
        Ok(())
    }

    #[test]
    fn test_large_file_operations() -> Result<()> {
        let temp_dir = TempDir::new().unwrap();
        let mut file_manager = FileManager::new(temp_dir.path())?;

        let file_id = file_manager.create_file("large.db")?;

        // Write data to blocks with large indexes
        let block_indices = [0, 10, 100, 1000];

        for &block_id in &block_indices {
            let data = vec![(block_id % 256) as u8; BLOCK_SIZE];
            file_manager.write_block(file_id, block_id, &data)?;
        }

        // Checking that the file has automatically expanded
        if let Some(file_info) = file_manager.get_file_info(file_id) {
            assert!(file_info.size_in_blocks() > 1000);
        }

        // Checking the data
        for &block_id in &block_indices {
            let read_data = file_manager.read_block(file_id, block_id)?;
            assert!(read_data.iter().all(|&x| x == (block_id % 256) as u8));
        }

        file_manager.close_file(file_id)?;
        Ok(())
    }
}

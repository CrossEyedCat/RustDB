//! Example of using the RustDB database file structure
//!
//! This example demonstrates:
//! - Work with extended database file headers
//! - Management of the map of free pages
//! - Various file expansion strategies
//! - Monitoring and usage statistics

use rustdb::common::Result;
use rustdb::storage::database_file::{
    DatabaseFileHeader, DatabaseFileType, ExtensionReason, ExtensionStrategy, FileExtensionManager,
    FreePageMap,
};

fn main() -> Result<()> {
    println!("=== Example of RustDB database file structure ===\n");

    // Demonstration of working with database file header
    demonstrate_database_header()?;

    // Demonstration of the map of free pages
    demonstrate_free_page_map()?;

    // File extension manager demo
    demonstrate_extension_manager()?;

    println!("\n🎉 Example completed successfully!");
    Ok(())
}

fn demonstrate_database_header() -> Result<()> {
    println!("📋 === Demonstration of the database file header ===");

    // Creating a header for the data file
    let mut data_header = DatabaseFileHeader::new(DatabaseFileType::Data, 12345);
    data_header.file_sequence = 1;
    data_header.max_pages = 1000000;
    data_header.extension_size = 512;

    println!("🗄️ Data file header created:");
    println!("- File type: {}", data_header.type_description());
    println!("- State: {}", data_header.state_description());
    println!("- Database ID: {}", data_header.database_id);
    println!(
        "- Format version: {}.{}",
        data_header.version, data_header.subversion
    );
    println!("- Page size: {} bytes", data_header.page_size);
    println!("- Maximum pages: {}", data_header.max_pages);
    println!("- Extension size: {} pages", data_header.extension_size);

    // Demonstrating how to work with flags
    data_header.set_flag(DatabaseFileHeader::FLAG_COMPRESSED);
    data_header.set_flag(DatabaseFileHeader::FLAG_DEBUG_MODE);

    println!("- Flags:");
    println!(
        "* Compression: {}",
        data_header.has_flag(DatabaseFileHeader::FLAG_COMPRESSED)
    );
    println!(
        "* Encryption: {}",
        data_header.has_flag(DatabaseFileHeader::FLAG_ENCRYPTED)
    );
    println!(
        "* Checksums: {}",
        data_header.has_flag(DatabaseFileHeader::FLAG_CHECKSUM_ENABLED)
    );
    println!(
        "* Debug mode: {}",
        data_header.has_flag(DatabaseFileHeader::FLAG_DEBUG_MODE)
    );

    // Update statistics
    data_header.total_pages = 1000;
    data_header.used_pages = 750;
    data_header.free_pages = 250;
    data_header.increment_write_count();
    data_header.increment_read_count();

    // Checking the checksum
    data_header.update_checksum();
    println!("- Checksum: 0x{:08X}", data_header.checksum);
    println!("- The title is correct: {}", data_header.is_valid());

    // Creating a header for the index file
    let mut index_header = DatabaseFileHeader::new(DatabaseFileType::Index, 12345);
    index_header.file_sequence = 2;
    index_header.catalog_root_page = Some(1);
    index_header.update_checksum();

    println!("\n📊 Index file header created:");
    println!("- File type: {}", index_header.type_description());
    println!(
        "- Directory root page: {:?}",
        index_header.catalog_root_page
    );
    println!("- File sequence: {}", index_header.file_sequence);

    Ok(())
}

fn demonstrate_free_page_map() -> Result<()> {
    println!("\n🗺️ === Demonstration of the map of free pages ===");

    let mut free_map = FreePageMap::new();

    println!("📍 Add free blocks:");

    // Adding various blocks of free pages
    free_map.add_free_block(100, 50)?;
    println!("✅ Added block: pages 100-149 (50 pages)");

    free_map.add_free_block(200, 25)?;
    println!("✅ Added block: pages 200-224 (25 pages)");

    free_map.add_free_block(300, 75)?;
    println!("✅ Added block: pages 300-374 (75 pages)");

    // We are trying to add a neighboring block (must merge)
    free_map.add_free_block(150, 20)?;
    println!("✅ Added adjacent block: pages 150-169 (20 pages) - merged with the previous one");

    println!("\n📊 Free pages map statistics:");
    println!(
        "- Total number of entries: {}",
        free_map.header.total_entries
    );
    println!("- Active entries: {}", free_map.header.active_entries);
    println!("- Total free pages: {}", free_map.total_free_pages());
    println!(
        "- Largest free block: {} pages",
        free_map.find_largest_free_block()
    );

    println!("\n💾 Select pages:");

    // Selecting pages of different sizes
    if let Some(allocated) = free_map.allocate_pages(30) {
        println!("✅ 30 pages allocated, starting from page {}", allocated);
    }

    if let Some(allocated) = free_map.allocate_pages(10) {
        println!("✅ 10 pages allocated, starting from page {}", allocated);
    }

    if let Some(allocated) = free_map.allocate_pages(100) {
        println!("✅ 100 pages allocated, starting from page {}", allocated);
    } else {
        println!("❌ Failed to allocate 100 pages (not enough space)");
    }

    println!("\n📊 Updated statistics:");
    println!("- Total free pages: {}", free_map.total_free_pages());
    println!(
        "- Largest free block: {} pages",
        free_map.find_largest_free_block()
    );

    // Freeing up some pages
    println!("\n🔄 Freeing pages:");
    free_map.free_pages(50, 15)?;
    println!("✅ 15 pages freed, starting from page 50");

    // Defragment the map
    println!("\n🔧 Let's defragment the map...");
    free_map.defragment();
    println!("✅ Defragmentation completed");
    println!(
        "- Entries after defragmentation: {}",
        free_map.entries.len()
    );

    // Checking integrity
    match free_map.validate() {
        Ok(_) => println!("✅ The map of free pages is correct"),
        Err(e) => println!("❌ Card validation error: {}", e),
    }

    Ok(())
}

fn demonstrate_extension_manager() -> Result<()> {
    println!("\n📈 === File extension manager demonstration ===");

    // We demonstrate various expansion strategies
    let strategies = vec![
        ("Fixed", ExtensionStrategy::Fixed),
        ("Linear", ExtensionStrategy::Linear),
        ("Exponential", ExtensionStrategy::Exponential),
        ("Adaptive", ExtensionStrategy::Adaptive),
    ];

    for (name, strategy) in strategies {
        println!("\n🔧 Expansion strategy: {}", name);

        let mut manager = FileExtensionManager::new(strategy);
        manager.min_extension_size = 32; // 128KB
        manager.max_extension_size = 1024; // 4MB
        manager.growth_factor = 1.5;

        let current_size = 1000u64;

        // Calculating expansion sizes for various requirements
        let sizes = vec![10, 50, 100, 500];

        for required_size in sizes {
            let extension_size = manager.calculate_extension_size(current_size, required_size);
            println!(
                "- Requires {} pages → extension to {} pages",
                required_size, extension_size
            );
        }

        // Simulating several extensions
        let mut file_size = current_size;
        for i in 1..=3 {
            let old_size = file_size;
            let extension = manager.calculate_extension_size(file_size, 50);
            file_size += extension as u64;

            manager.record_extension(old_size, file_size, ExtensionReason::OutOfSpace);

            println!(
                "- Extension #{}: {} → {} pages (+{})",
                i, old_size, file_size, extension
            );
        }

        // Getting statistics
        let stats = manager.get_statistics();
        println!("- Total extensions: {}", stats.total_extensions);
        println!(
            "- Average extension size: {:.1} pages",
            stats.average_extension_size
        );

        // Checking recommendations for preliminary expansion
        let should_preextend = manager.should_preextend(file_size, 100, file_size);
        println!("- Pre-extension recommended: {}", should_preextend);
    }

    // Demonstrating an adaptive strategy with history
    println!("\n🧠Demonstration of adaptive strategy:");
    let mut adaptive_manager = FileExtensionManager::new(ExtensionStrategy::Adaptive);

    // Simulating active use (many extensions)
    let mut file_size = 500u64;
    for i in 1..=8 {
        let old_size = file_size;
        let extension = adaptive_manager.calculate_extension_size(file_size, 20);
        file_size += extension as u64;

        let reason = if i % 3 == 0 {
            ExtensionReason::Preallocation
        } else {
            ExtensionReason::OutOfSpace
        };

        adaptive_manager.record_extension(old_size, file_size, reason);

        println!(
            "- Adaptive expansion #{}: +{} pages (reason: {:?})",
            i, extension, reason
        );
    }

    let final_stats = adaptive_manager.get_statistics();
    println!("- Final statistics:");
    println!("* Total extensions: {}", final_stats.total_extensions);
    println!(
        "* Average size: {:.1} pages",
        final_stats.average_extension_size
    );

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_database_structure_example() -> Result<()> {
        // Run the main function as a test
        main()
    }

    #[test]
    fn test_header_operations() -> Result<()> {
        let mut header = DatabaseFileHeader::new(DatabaseFileType::Data, 999);

        // Testing basic operations
        assert_eq!(header.database_id, 999);
        assert_eq!(header.file_type, DatabaseFileType::Data);
        assert_eq!(header.file_state, DatabaseFileState::Creating);

        // Testing flags
        header.set_flag(DatabaseFileHeader::FLAG_COMPRESSED);
        assert!(header.has_flag(DatabaseFileHeader::FLAG_COMPRESSED));

        header.clear_flag(DatabaseFileHeader::FLAG_COMPRESSED);
        assert!(!header.has_flag(DatabaseFileHeader::FLAG_COMPRESSED));

        // Testing validation
        header.update_checksum();
        assert!(header.is_valid());

        Ok(())
    }

    #[test]
    fn test_free_page_map_operations() -> Result<()> {
        let mut map = FreePageMap::new();

        // Adding blocks
        map.add_free_block(10, 5)?;
        map.add_free_block(20, 10)?;

        assert_eq!(map.total_free_pages(), 15);
        assert_eq!(map.find_largest_free_block(), 10);

        // Selecting pages
        let allocated = map.allocate_pages(3);
        assert_eq!(allocated, Some(10));

        assert_eq!(map.total_free_pages(), 12);

        // Freeing up pages
        map.free_pages(50, 5)?;
        assert_eq!(map.total_free_pages(), 17);

        Ok(())
    }

    #[test]
    fn test_extension_strategies() {
        let fixed = FileExtensionManager::new(ExtensionStrategy::Fixed);
        let linear = FileExtensionManager::new(ExtensionStrategy::Linear);
        let exponential = FileExtensionManager::new(ExtensionStrategy::Exponential);

        let current_size = 1000;
        let required = 10;

        let fixed_ext = fixed.calculate_extension_size(current_size, required);
        let linear_ext = linear.calculate_extension_size(current_size, required);
        let exp_ext = exponential.calculate_extension_size(current_size, required);

        // A fixed strategy should give a minimum size
        assert!(fixed_ext >= required as u32);

        // Other strategies must take file size into account
        assert!(linear_ext >= fixed_ext);
        assert!(exp_ext >= fixed_ext);
    }
}

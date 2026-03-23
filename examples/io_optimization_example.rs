//! An example of using I/O optimization in rustdb
//!
//! This example demonstrates:
//! - Buffered writes
//! - Asynchronous read/write operations
//! - Caching pages with LRU policy
//! - Data prefetching
//! - Performance monitoring

use rustdb::common::Result;
use rustdb::storage::{
    database_file::{DatabaseFileType, ExtensionStrategy, BLOCK_SIZE},
    io_optimization::{BufferedIoManager, IoBufferConfig},
    optimized_file_manager::OptimizedFileManager,
};
use std::time::{Duration, Instant};
use tokio::time::sleep;
// use rand::prelude::*; // unused

#[tokio::main]
async fn main() -> Result<()> {
    println!("=== Example of optimization of I/O operations rustdb ===\n");

    // Demonstration of a buffered I/O manager
    demonstrate_buffered_io().await?;

    // Optimized file manager demo
    demonstrate_optimized_file_manager().await?;

    // Cache performance demo (lite version)
    demonstrate_cache_performance_simple().await?;

    // Demonstration of monitoring and statistics
    demonstrate_monitoring().await?;

    println!("\n🎉 Example completed successfully!");
    Ok(())
}

async fn demonstrate_buffered_io() -> Result<()> {
    println!("💾 === Demonstration of buffered I/O ===");

    // Creating a configuration with performance settings
    let config = IoBufferConfig {
        max_write_buffer_size: 500,
        max_buffer_time: Duration::from_millis(50),
        page_cache_size: 1000,
        enable_prefetch: true,
        prefetch_window_size: 5,
        ..Default::default()
    };

    println!("📋 I/O configuration:");
    println!(
        "- Write buffer size: {} operations",
        config.max_write_buffer_size
    );
    println!("- Buffering time: {:?}", config.max_buffer_time);
    println!("- Page cache size: {} pages", config.page_cache_size);
    println!(
        "- Prefetch: {} (window: {})",
        config.enable_prefetch, config.prefetch_window_size
    );

    let manager = BufferedIoManager::new(config);

    println!("\n📝 Performing write operations...");
    let start_time = Instant::now();

    // Performing a batch of write operations
    for i in 0..100 {
        let data = vec![(i % 256) as u8; BLOCK_SIZE];
        manager.write_page_async(1, i, data).await?;

        if i % 20 == 0 {
            print!(".");
        }
    }

    let write_time = start_time.elapsed();
    println!("\n ✅ 100 pages recorded in {:?}", write_time);

    println!("\n📖 Performing read operations...");
    let start_time = Instant::now();

    // Reading data (must go into cache)
    for i in 0..100 {
        let _data = manager.read_page_async(1, i).await?;

        if i % 20 == 0 {
            print!(".");
        }
    }

    let read_time = start_time.elapsed();
    println!("\n ✅ Read 100 pages in {:?}", read_time);

    // Getting statistics
    let stats = manager.get_statistics();
    println!("\n📊 I/O statistics:");
    println!("- Total operations: {}", stats.total_operations);
    println!("- Write operations: {}", stats.write_operations);
    println!("- Read operations: {}", stats.read_operations);
    println!("- Cache hits: {}", stats.cache_hits);
    println!("- Cache misses: {}", stats.cache_misses);
    println!("- Hit Rate: {:.2}%", stats.cache_hit_ratio * 100.0);

    let (buffer_used, buffer_max, cache_size) = manager.get_buffer_info();
    println!("- Buffer usage: {}/{}", buffer_used, buffer_max);
    println!("- Cache size: {} pages", cache_size);

    Ok(())
}

async fn demonstrate_optimized_file_manager() -> Result<()> {
    println!("\n🚀 === Demonstration of an optimized file manager ===");

    let temp_dir = tempfile::TempDir::new().unwrap();
    let manager = OptimizedFileManager::new(temp_dir.path())?;

    // Creating files with different expansion strategies
    let strategies = [
        ("Fixed", ExtensionStrategy::Fixed),
        ("Linear", ExtensionStrategy::Linear),
        ("Exponential", ExtensionStrategy::Exponential),
        ("Adaptive", ExtensionStrategy::Adaptive),
    ];

    for (name, strategy) in &strategies {
        println!("\n📁 Create a file with the strategy: {}", name);

        let file_id = manager
            .create_database_file(
                &format!("{}_test.db", name.to_lowercase()),
                DatabaseFileType::Data,
                123,
                *strategy,
            )
            .await?;

        // Selecting pages
        let start_page = manager.allocate_pages(file_id, 50).await?;
        println!("✅ 50 pages allocated, starting from {}", start_page);

        // Recording data
        let test_data = vec![42u8; BLOCK_SIZE];
        let start_time = Instant::now();

        for i in 0..50 {
            manager
                .write_page(file_id, start_page + i, &test_data)
                .await?;
        }

        let write_time = start_time.elapsed();
        println!("✅ 50 pages recorded in {:?}", write_time);

        // Reading the data
        let start_time = Instant::now();

        for i in 0..50 {
            let _data = manager.read_page(file_id, start_page + i).await?;
        }

        let read_time = start_time.elapsed();
        println!("✅ Read 50 pages in {:?}", read_time);

        // Getting information about the file
        if let Some(file_info) = manager.get_file_info(file_id).await {
            println!("📊 File information:");
            println!("- Total pages: {}", file_info.total_pages);
            println!("- Pages used: {}", file_info.used_pages);
            println!("- Free pages: {}", file_info.free_pages);
            println!("- Usage rate: {:.1}%", file_info.utilization_ratio * 100.0);
            println!(
                "- Fragmentation rate: {:.1}%",
                file_info.fragmentation_ratio * 100.0
            );
        }
    }

    Ok(())
}

async fn demonstrate_cache_performance_simple() -> Result<()> {
    println!("\n🔄 === Demonstration of cache performance ===");

    let temp_dir = tempfile::TempDir::new().unwrap();
    let manager = OptimizedFileManager::new(temp_dir.path())?;

    let file_id = manager
        .create_database_file(
            "cache_demo.db",
            DatabaseFileType::Data,
            456,
            ExtensionStrategy::Adaptive,
        )
        .await?;

    // Preparing test data
    let page_count = 200;
    let start_page = manager.allocate_pages(file_id, page_count).await?;

    println!("📝 We record {} pages...", page_count);
    for i in 0..page_count {
        let data = vec![(i % 256) as u8; BLOCK_SIZE];
        manager
            .write_page(file_id, start_page + i as u64, &data)
            .await?;
    }

    // Testing simple access patterns
    let access_patterns = [
        ("Consistent", (0..page_count).collect::<Vec<_>>()),
        ("Back", (0..page_count).rev().collect::<Vec<_>>()),
    ];

    for (pattern_name, pattern) in &access_patterns {
        println!("\n🔍 Testing the pattern: {}", pattern_name);

        // Clearing the cache for a clean test
        manager.clear_io_cache().await;

        let start_time = Instant::now();

        for &page_offset in pattern {
            let _data = manager
                .read_page(file_id, start_page + page_offset as u64)
                .await?;
        }

        let access_time = start_time.elapsed();
        let stats = manager.get_io_statistics();

        println!("⏱️ Access time: {:?}", access_time);
        println!(
            "📊 Cache hits: {} ({:.1}%)",
            stats.cache_hits,
            stats.cache_hit_ratio * 100.0
        );
        println!("📊 Cache misses: {}", stats.cache_misses);

        let avg_time_per_op = access_time.as_nanos() / pattern.len() as u128;
        println!("⚡ Average time per operation: {} ns", avg_time_per_op);
    }

    Ok(())
}

async fn demonstrate_monitoring() -> Result<()> {
    println!("\n📈 === Demonstration of monitoring and statistics ===");

    let temp_dir = tempfile::TempDir::new().unwrap();
    let manager = OptimizedFileManager::new(temp_dir.path())?;

    // Create several files
    let mut file_ids = Vec::new();
    for i in 0..3 {
        let file_id = manager
            .create_database_file(
                &format!("monitor_test_{}.db", i),
                DatabaseFileType::Data,
                100 + i,
                ExtensionStrategy::Adaptive,
            )
            .await?;
        file_ids.push(file_id);
    }

    println!("🏃 We perform intensive work...");

    // Simulating the workload
    let workload_start = Instant::now();

    for round in 0..5 {
        println!("Round {}/5", round + 1);

        for &file_id in &file_ids {
            // Selecting pages
            let start_page = manager.allocate_pages(file_id, 20).await?;

            // Recording data
            for i in 0..20 {
                let data = vec![(round * 20 + i) as u8; BLOCK_SIZE];
                manager
                    .write_page(file_id, start_page + i as u64, &data)
                    .await?;
            }

            // Reading data (mixture of new and old)
            for i in 0..30 {
                let page_id = if i < 20 {
                    start_page + i as u64
                } else {
                    // Reading old data
                    if start_page > 0 {
                        start_page / 2
                    } else {
                        0
                    }
                };
                let _data = manager.read_page(file_id, page_id).await?;
            }
        }

        // Periodic Maintenance
        if round % 2 == 0 {
            let extended_files = manager.maintenance_check().await?;
            if !extended_files.is_empty() {
                println!("🔧 File extension: {}", extended_files.len());
            }
        }

        sleep(Duration::from_millis(100)).await;
    }

    let workload_time = workload_start.elapsed();
    println!("✅ Load completed in {:?}", workload_time);

    // We get combined statistics
    let stats = manager.get_combined_statistics().await;

    println!("\n📊 Combined statistics:");
    println!("- Total files: {}", stats.total_files);
    println!("- Total pages: {}", stats.total_pages);
    println!("- Read operations: {}", stats.total_reads);
    println!("- Write operations: {}", stats.total_writes);
    println!("- Cache hit rate: {:.1}%", stats.cache_hit_ratio * 100.0);
    println!(
        "- Average utilization: {:.1}%",
        stats.average_utilization * 100.0
    );
    println!(
        "- Average fragmentation: {:.1}%",
        stats.average_fragmentation * 100.0
    );
    println!("- Buffer usage: {:.1}%", stats.buffer_usage * 100.0);
    println!("- Cache size: {} pages", stats.cache_usage);

    if stats.read_throughput > 0.0 {
        println!(
            "- Read throughput: {:.1} MB/s",
            stats.read_throughput / 1_000_000.0
        );
    }
    if stats.write_throughput > 0.0 {
        println!(
            "- Write Bandwidth: {:.1} MB/s",
            stats.write_throughput / 1_000_000.0
        );
    }

    // Performance Evaluation
    let performance_score = stats.performance_score();
    println!(
        "\n⭐ Performance Rating: {:.1}% ({})",
        performance_score * 100.0,
        match performance_score {
            s if s >= 0.9 => "Great",
            s if s >= 0.8 => "Fine",
            s if s >= 0.7 => "Satisfactorily",
            s if s >= 0.6 => "Needs attention",
            _ => "Needs optimization",
        }
    );

    // Optimization recommendations
    let recommendations = stats.get_recommendations();
    println!("\n💡 Recommendations for optimization:");
    for (i, recommendation) in recommendations.iter().enumerate() {
        println!("   {}. {}", i + 1, recommendation);
    }

    // Integrity check
    println!("\n🔍 Checking file integrity...");
    let validation_results = manager.validate_all().await?;

    let mut valid_files = 0;
    let mut invalid_files = 0;

    for (file_id, result) in validation_results {
        match result {
            Ok(_) => {
                valid_files += 1;
                println!("✅ File {} is correct", file_id);
            }
            Err(e) => {
                invalid_files += 1;
                println!("❌ File {} is corrupted: {}", file_id, e);
            }
        }
    }

    println!("\n📋 Check result:");
    println!("- Correct files: {}", valid_files);
    println!("- Damaged files: {}", invalid_files);

    if invalid_files == 0 {
        println!("🎉 All files have passed the integrity check!");
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_io_optimization_example() -> Result<()> {
        // Run the main function as a test
        main()
    }

    #[tokio::test]
    async fn test_performance_comparison() -> Result<()> {
        let temp_dir = tempfile::TempDir::new().unwrap();

        // Testing a regular manager vs an optimized one
        let optimized_manager = OptimizedFileManager::new(temp_dir.path())?;

        let file_id = optimized_manager
            .create_database_file(
                "perf_test.db",
                DatabaseFileType::Data,
                999,
                ExtensionStrategy::Adaptive,
            )
            .await?;

        let data = vec![123u8; BLOCK_SIZE];
        let operations = 100;

        // Testing the optimized manager
        let start_time = Instant::now();

        let start_page = optimized_manager
            .allocate_pages(file_id, operations)
            .await?;

        for i in 0..operations {
            optimized_manager
                .write_page(file_id, start_page + i as u64, &data)
                .await?;
        }

        for i in 0..operations {
            let _read_data = optimized_manager
                .read_page(file_id, start_page + i as u64)
                .await?;
        }

        let optimized_time = start_time.elapsed();

        // Getting statistics
        let stats = optimized_manager.get_combined_statistics().await;

        println!("Optimized manager:");
        println!("Time: {:?}", optimized_time);
        println!("Cache hit rate: {:.1}%", stats.cache_hit_ratio * 100.0);
        println!(
            "Performance Rating: {:.1}%",
            stats.performance_score() * 100.0
        );

        // Checking that performance is acceptable
        assert!(stats.performance_score() > 0.5, "Performance is too low");

        Ok(())
    }
}

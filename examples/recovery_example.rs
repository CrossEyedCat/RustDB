//! Example of database recovery after a failure

use rustdb::core::{AdvancedRecoveryManager, RecoveryConfig};
// removed unused LogRecord import
use std::path::Path;
use std::time::Duration;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Example of a database recovery system ===\n");

    // 1. Create a recovery manager
    println!("1. Create a recovery manager");
    let config = RecoveryConfig {
        max_recovery_time: Duration::from_secs(300),
        enable_parallel: true,
        num_threads: 4,
        create_backup: true,
        enable_validation: true,
    };

    let mut manager = AdvancedRecoveryManager::new(config);
    println!("✓ Recovery manager has been created\n");

    // 2. Checking the need for restoration
    println!("2. Checking the need for restoration");
    let log_dir = Path::new("./logs");
    let needs_recovery = manager.needs_recovery(log_dir);

    if needs_recovery {
        println!("⚠️ Incomplete transactions detected");
        println!("🔄Recovery required\n");
    } else {
        println!("✅ No recovery required");
        println!("(no log files or all transactions completed)\n");
    }

    // 3. Create a backup (if necessary)
    println!("3. Create a backup before restoring");
    let data_dir = Path::new("./data");
    let backup_dir = Path::new("./backup");

    match manager.create_backup(data_dir, backup_dir) {
        Ok(_) => println!("✓ The backup was created in ./backup\n"),
        Err(_) => println!("ℹ️ Backup not created (no data)\n"),
    }

    // 4. Simulation of recovery
    println!("4. Simulation of the recovery process");
    println!("The recovery process includes:");
    println!("📊 Stage 1: Log analysis");
    println!("- Read all log files");
    println!("- Determination of active transactions");
    println!("- Building a dependency graph");
    println!("- Search for control points");
    println!();
    println!("🔄 Stage 2: REDO operations");
    println!("- Repetition of recorded transactions");
    println!("- Restoring modified pages");
    println!("- Application in LSN order");
    println!();
    println!("↩️ Stage 3: UNDO operations");
    println!("- Rollback of pending transactions");
    println!("- Recover old data");
    println!("- Application in reverse order");
    println!();
    println!("🔍 Stage 4: Validation");
    println!("- Data integrity check");
    println!("- Transaction verification");
    println!();

    // 5. Perform recovery (if necessary)
    if needs_recovery {
        println!("5. Performing a restore");
        match manager.recover(log_dir) {
            Ok(stats) => {
                println!("✅ Recovery completed successfully!");
                println!("\n6. Recovery statistics:");
                println!("Log files processed: {}", stats.log_files_processed);
                println!("Total entries: {}", stats.total_records);
                println!("REDO operations: {}", stats.redo_operations);
                println!("UNDO operations: {}", stats.undo_operations);
                println!(
                    "Transactions recovered: {}",
                    stats.recovered_transactions
                );
                println!("Transactions rolled back: {}", stats.rolled_back_transactions);
                println!("Pages recovered: {}", stats.recovered_pages);
                println!("Recovery Time: {}ms", stats.recovery_time_ms);
                println!("Errors: {}", stats.recovery_errors);
            }
            Err(e) => {
                println!("⚠️ Restore error: {}", e);
            }
        }
    } else {
        println!("5. No recovery required");
        println!("Database in a consistent state");
    }

    // 7. Description of the algorithm
    println!("\n7. Recovery algorithm (ARIES)");
    println!("   ARIES = Algorithm for Recovery and Isolation Exploiting Semantics");
    println!();
    println!("Phases:");
    println!("1️⃣ Analysis - log analysis, state determination");
    println!("2️⃣ REDO - repeat all changes");
    println!("3️⃣ UNDO - rollback of unfinished transactions");
    println!();
    println!("Guarantees:");
    println!("✓ Atomicity - the transaction is either fully applied or rolled back");
    println!("✓ Durability - recorded data is not lost");
    println!("✓ Consistency - the database remains in a consistent state");

    // 8. Recommendations
    println!("\n8. Recommendations for use");
    println!("✓ Create checkpoints regularly");
    println!("✓ Set up automatic log archiving");
    println!("✓ Monitor the size of log files");
    println!("✓ Test the recovery process");
    println!("✓ Create backups before restoring");

    println!("\n=== Example completed successfully ===");

    Ok(())
}

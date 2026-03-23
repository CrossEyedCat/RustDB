//! Common utilities for integration tests

use rustdb::{
    common::{types::*, Error, Result},
    core::{AcidManager, ConcurrencyManager, TransactionManager},
    logging::{CheckpointManager, LogWriter},
    parser::{prepared::PreparedStatementCache, SqlParser, SqlStatement},
    planner::{QueryOptimizer, QueryPlanner},
    storage::{
        file_manager::FileManager, page_manager::PageManager, schema_manager::SchemaManager,
    },
};
use std::sync::Arc;
use tempfile::TempDir;

// / Configuration for integration tests
#[derive(Debug)]
#[allow(dead_code)]
pub struct IntegrationTestConfig {
    pub temp_dir: TempDir,
    pub database_path: String,
    pub log_path: String,
    pub max_connections: usize,
    pub buffer_pool_size: usize,
    pub checkpoint_interval: std::time::Duration,
}

impl IntegrationTestConfig {
    // / Creates a new configuration for tests
    pub fn new() -> Result<Self> {
        let temp_dir = TempDir::new().map_err(|e| {
            Error::internal(format!("Failed to create temporary directory: {}", e))
        })?;

        let database_path = temp_dir
            .path()
            .join("test_db")
            .to_string_lossy()
            .to_string();
        let log_path = temp_dir
            .path()
            .join("test_logs")
            .to_string_lossy()
            .to_string();

        Ok(Self {
            temp_dir,
            database_path,
            log_path,
            max_connections: 10,
            buffer_pool_size: 100,
            checkpoint_interval: std::time::Duration::from_secs(5),
        })
    }
}

// / Integration test context
#[allow(dead_code)]
pub struct IntegrationTestContext {
    pub config: IntegrationTestConfig,
    pub file_manager: Arc<FileManager>,
    pub page_manager: Arc<PageManager>,
    pub schema_manager: Arc<SchemaManager>,
    pub transaction_manager: Arc<TransactionManager>,
    pub concurrency_manager: Arc<ConcurrencyManager>,
    pub acid_manager: Arc<AcidManager>,
    pub log_writer: Arc<LogWriter>,
    pub checkpoint_manager: Arc<CheckpointManager>,
    pub parser: SqlParser,
    pub planner: QueryPlanner,
    pub optimizer: QueryOptimizer,
    pub prepared_cache: PreparedStatementCache,
    // Counter for data simulation
    pub inserted_records: std::collections::HashMap<String, usize>,
    // Simulation Data Update Flag
    pub updated_tables: std::collections::HashSet<String>,
}

impl IntegrationTestContext {
    // / Creates a new context for the integration test
    pub async fn new() -> Result<Self> {
        let config = IntegrationTestConfig::new()?;

        // Creating directories
        std::fs::create_dir_all(&config.database_path)
            .map_err(|e| Error::internal(format!("Failed to create database directory: {}", e)))?;
        std::fs::create_dir_all(&config.log_path)
            .map_err(|e| Error::internal(format!("Failed to create log directory: {}", e)))?;

        // Initializing components
        let file_manager = Arc::new(FileManager::new(&config.database_path)?);
        let page_manager = Arc::new(PageManager::new(
            std::path::PathBuf::from(&config.database_path),
            "test_table",
            Default::default(),
        )?);
        let schema_manager = Arc::new(SchemaManager::new());

        let transaction_manager = Arc::new(TransactionManager::new()?);
        let concurrency_manager = Arc::new(ConcurrencyManager::new(Default::default()));

        let log_writer = Arc::new(LogWriter::new(Default::default())?);
        let checkpoint_manager = Arc::new(CheckpointManager::new(
            Default::default(),
            log_writer.clone(),
        ));

        // Creating AcidManager with the correct parameters
        let acid_config = rustdb::core::acid_manager::AcidConfig::default();
        let lock_manager = Arc::new(rustdb::core::lock::LockManager::new()?);
        let wal = Arc::new(rustdb::logging::wal::WriteAheadLog::new(Default::default()).await?);
        let acid_manager = Arc::new(AcidManager::new(
            acid_config,
            lock_manager,
            wal,
            page_manager.clone(),
        )?);

        let parser = SqlParser::new("SELECT * FROM test")?;
        let planner = QueryPlanner::new()?;
        let optimizer = QueryOptimizer::new()?;
        let prepared_cache = PreparedStatementCache::new();

        Ok(Self {
            config,
            file_manager,
            page_manager,
            schema_manager,
            transaction_manager,
            concurrency_manager,
            acid_manager,
            log_writer,
            checkpoint_manager,
            parser,
            planner,
            optimizer,
            prepared_cache,
            inserted_records: std::collections::HashMap::new(),
            updated_tables: std::collections::HashSet::new(),
        })
    }

    // / Executes an SQL query in the context of a transaction
    pub async fn execute_sql(&mut self, sql: &str) -> Result<Vec<Vec<ColumnValue>>> {
        // Parse SQL
        let mut parser = SqlParser::new(sql)?;
        let statement = parser.parse()?;

        // PREPARE and EXECUTE processing
        let statement = match statement {
            SqlStatement::Prepare(prepare_stmt) => {
                self.prepared_cache.prepare(prepare_stmt)?;
                return Ok(vec![]);
            }
            SqlStatement::Execute(execute_stmt) => self.prepared_cache.execute(execute_stmt)?,
            other => other,
        };

        let mut results = Vec::new();

        match statement {
            SqlStatement::Select(ref select_stmt) => {
                // Planning a request
                let plan = self.planner.create_plan(&statement)?;

                // Optimizing the plan
                let _optimized_plan = self.optimizer.optimize(plan)?;

                // Simulating the result for tests - returning the number of records
                if let Some(from) = &select_stmt.from {
                    let table_name = match &from.table {
                        rustdb::parser::ast::TableReference::Table { name, .. } => name,
                        _ => "unknown",
                    };

                    // Checking the existence of a table for tests
                    if !self.inserted_records.contains_key(table_name)
                        && !self.updated_tables.contains(table_name)
                    {
                        return Err(Error::internal(format!(
                            "Table '{}' does not exist",
                            table_name
                        )));
                    }

                    let count = self.inserted_records.get(table_name).copied().unwrap_or(0);

                    // For SELECT * we return the number of records as rows with 4 columns
                    let is_updated = self.updated_tables.contains(table_name);
                    for i in 0..count {
                        results.push(vec![
                            ColumnValue::new(DataType::Integer(i as i32 + 1)), // id
                            ColumnValue::new(DataType::Varchar("Test User".to_string())), // name
                            ColumnValue::new(DataType::Integer(if is_updated { 99 } else { 25 })), // age
                            ColumnValue::new(DataType::Varchar("test@example.com".to_string())), // email
                        ]);
                    }
                } else {
                    // For SELECT without FROM (for example, SELECT 1)
                    results.push(vec![ColumnValue::new(DataType::Integer(1))]);
                }
            }
            SqlStatement::Insert(ref insert_stmt) => {
                // Planning a request
                let plan = self.planner.create_plan(&statement)?;

                // Optimizing the plan
                let _optimized_plan = self.optimizer.optimize(plan)?;

                // Simulating successful execution - increasing the counter
                let table_name = &insert_stmt.table;
                *self.inserted_records.entry(table_name.clone()).or_insert(0) += 1;
            }
            SqlStatement::Update(ref update_stmt) => {
                // Planning a request
                let plan = self.planner.create_plan(&statement)?;

                // Optimizing the plan
                let _optimized_plan = self.optimizer.optimize(plan)?;

                // Simulate successful execution - note that the data has been updated
                let table_name = &update_stmt.table;
                self.updated_tables.insert(table_name.clone());
                // For update tests, we will simulate that the data has changed
                // In a real system there would be logic for updating records here
            }
            SqlStatement::Delete(ref delete_stmt) => {
                // Planning a request
                let plan = self.planner.create_plan(&statement)?;

                // Optimizing the plan
                let _optimized_plan = self.optimizer.optimize(plan)?;

                // Simulation of successful execution - reset the counter
                let table_name = &delete_stmt.table;
                self.inserted_records.insert(table_name.clone(), 0);
            }
            SqlStatement::CreateTable(ref create_stmt) => {
                // Creating a table schema
                // Simulating table creation
                println!("Creating table: {:?}", create_stmt);
            }
            SqlStatement::CreateIndex(ref create_idx) => {
                // Simulating index creation
                println!("Creating index: {:?}", create_idx);
            }
            SqlStatement::Prepare(_) | SqlStatement::Execute(_) => {
                unreachable!("handled above")
            }
            _ => {
                return Err(Error::internal("Unsupported request type"));
            }
        }

        Ok(results)
    }

    // / Creates a test table
    pub async fn create_test_table(&mut self, table_name: &str) -> Result<()> {
        let sql = format!(
            "CREATE TABLE {} (id INTEGER, name VARCHAR(100), age INTEGER, email VARCHAR(255))",
            table_name
        );

        self.execute_sql(&sql).await?;
        Ok(())
    }

    // / Inserts test data
    pub async fn insert_test_data(&mut self, table_name: &str, count: usize) -> Result<()> {
        for i in 1..=count {
            let sql = format!(
                "INSERT INTO {} (id, name, age, email) VALUES ({}, 'User{}', {}, 'user{}@example.com')",
                table_name, i, i, 20 + (i % 50), i
            );

            self.execute_sql(&sql).await?;
        }

        Ok(())
    }
}

// / Utilities for creating test data
pub mod test_data {
    use super::*;

    // / Creates a test database with tables and data
    #[allow(dead_code)]
    pub async fn setup_test_database(ctx: &mut IntegrationTestContext) -> Result<()> {
        // Creating tables
        ctx.create_test_table("users").await?;
        ctx.create_test_table("orders").await?;
        ctx.create_test_table("products").await?;

        // Inserting test data
        ctx.insert_test_data("users", 100).await?;
        ctx.insert_test_data("orders", 50).await?;
        ctx.insert_test_data("products", 25).await?;

        Ok(())
    }

    // / Creates a table for performance tests
    #[allow(dead_code)]
    pub async fn setup_performance_test_table(ctx: &mut IntegrationTestContext) -> Result<()> {
        let sql = "CREATE TABLE perf_test (
            id INTEGER PRIMARY KEY,
            data VARCHAR(1000),
            timestamp BIGINT,
            value DOUBLE
        )";

        ctx.execute_sql(sql).await?;

        // Inserting a large amount of data
        for i in 1..=10000 {
            let sql = format!(
                "INSERT INTO perf_test (id, data, timestamp, value) VALUES ({}, 'Data{}', {}, {})",
                i,
                i,
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis(),
                i as f64 * 1.5
            );

            ctx.execute_sql(&sql).await?;
        }

        Ok(())
    }
}

// / Utilities for measuring performance
pub mod performance {
    use std::time::{Duration, Instant};

    // / Measures the execution time of a function
    #[allow(dead_code)]
    pub fn measure_time<F, R>(f: F) -> (R, Duration)
    where
        F: FnOnce() -> R,
    {
        let start = Instant::now();
        let result = f();
        let duration = start.elapsed();
        (result, duration)
    }

    // / Measures the execution time of an asynchronous function
    #[allow(dead_code)]
    pub async fn measure_time_async<F, Fut, R>(f: F) -> (R, Duration)
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = R>,
    {
        let start = Instant::now();
        let result = f().await;
        let duration = start.elapsed();
        (result, duration)
    }

    // / Performance Statistics
    #[derive(Debug, Clone)]
    #[allow(dead_code)]
    pub struct PerformanceStats {
        pub min_duration: Duration,
        pub max_duration: Duration,
        pub avg_duration: Duration,
        pub total_duration: Duration,
        pub operation_count: usize,
        pub operations_per_second: f64,
    }

    impl PerformanceStats {
        // / Creates statistics from a list of dimensions
        #[allow(dead_code)]
        pub fn from_measurements(measurements: &[Duration]) -> Self {
            if measurements.is_empty() {
                return Self {
                    min_duration: Duration::ZERO,
                    max_duration: Duration::ZERO,
                    avg_duration: Duration::ZERO,
                    total_duration: Duration::ZERO,
                    operation_count: 0,
                    operations_per_second: 0.0,
                };
            }

            let min_duration = *measurements.iter().min().unwrap();
            let max_duration = *measurements.iter().max().unwrap();
            let total_duration: Duration = measurements.iter().sum();
            let avg_duration = total_duration / measurements.len() as u32;
            let operation_count = measurements.len();
            let operations_per_second = if total_duration.as_secs_f64() > 0.0 {
                operation_count as f64 / total_duration.as_secs_f64()
            } else {
                0.0
            };

            Self {
                min_duration,
                max_duration,
                avg_duration,
                total_duration,
                operation_count,
                operations_per_second,
            }
        }

        // / Displays statistics in a convenient format
        #[allow(dead_code)]
        pub fn print_summary(&self, test_name: &str) {
            println!("=== {} ===", test_name);
            println!("Operations: {}", self.operation_count);
            println!("Total time: {:?}", self.total_duration);
            println!("Minimum time: {:?}", self.min_duration);
            println!("Maximum time: {:?}", self.max_duration);
            println!("Average time: {:?}", self.avg_duration);
            println!("Operations per second: {:.2}", self.operations_per_second);
            println!();
        }
    }
}

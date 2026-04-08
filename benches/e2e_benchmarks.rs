//! RustDB end-to-end benchmarks with real execution
//!
//! Measures full pipeline: parse → plan → execute → WAL → disk.

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use rustdb::{
    common::types::RecordId,
    logging::log_record::IsolationLevel,
    logging::wal::{WalConfig, WriteAheadLog},
    parser::SqlParser,
    planner::{PlanNode, QueryOptimizer, QueryPlanner},
    storage::page_manager::{PageManager, PageManagerConfig},
};
use std::hint::black_box;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use tempfile::TempDir;
use tokio::runtime::Runtime;

/// Parses RecordId into page_id and offset (PageManager-compatible)
fn parse_record_id(record_id: RecordId) -> (u64, u16) {
    let page_id = record_id >> 32;
    let offset = (record_id & 0xFFFFFFFF) as u16;
    (page_id, offset)
}

/// Serializes INSERT values to bytes for PageManager (compact format)
fn serialize_insert_values(values: &[String]) -> Vec<u8> {
    values.join("\t").into_bytes()
}

/// Runs INSERT plan via PageManager and optional WAL
async fn execute_insert_e2e(
    sql: &str,
    page_manager: &Mutex<PageManager>,
    wal: Option<&WriteAheadLog>,
) -> rustdb::Result<()> {
    let mut parser = SqlParser::new(sql)?;
    let stmt = parser.parse()?;

    let planner = QueryPlanner::new()?;
    let plan = planner.create_plan(&stmt)?;

    let optimizer = QueryOptimizer::new()?;
    let optimized = optimizer.optimize(plan)?;

    let PlanNode::Insert(insert_node) = &optimized.optimized_plan.root else {
        return Err(rustdb::Error::internal("Expected Insert plan"));
    };

    if let Some(wal) = wal {
        let tx_id = wal.begin_transaction(IsolationLevel::ReadCommitted).await?;

        for row_values in &insert_node.values {
            let data = serialize_insert_values(row_values);
            let (result, file_id) = {
                let mut pm = page_manager.lock().unwrap();
                let result = pm.insert(&data)?;
                let file_id = pm.file_id();
                (result, file_id)
            };
            let (page_id, record_offset) = parse_record_id(result.record_id);
            wal.log_insert(tx_id, file_id, page_id, record_offset, data)
                .await?;
        }

        wal.commit_transaction(tx_id).await?;
        // Flush dirty pages after commit (write-ahead + batch flush)
        let mut pm = page_manager.lock().unwrap();
        pm.flush_dirty_pages()?;
    } else {
        for row_values in &insert_node.values {
            let data = serialize_insert_values(row_values);
            let mut pm = page_manager.lock().unwrap();
            pm.insert(&data)?;
        }
        let mut pm = page_manager.lock().unwrap();
        pm.flush_dirty_pages()?;
    }
    Ok(())
}

/// Setup for e2e benchmarks. When enable_wal is false, WAL is None for pure I/O measurement.
fn setup_e2e_env(
    enable_wal: bool,
) -> (
    TempDir,
    Arc<Mutex<PageManager>>,
    Runtime,
    Option<Arc<WriteAheadLog>>,
) {
    setup_e2e_env_with_wal_config(enable_wal.then_some(WalConfig::default()))
}

/// Setup with custom WalConfig (for high_throughput, durable presets).
fn setup_e2e_env_with_wal_config(
    wal_config: Option<WalConfig>,
) -> (
    TempDir,
    Arc<Mutex<PageManager>>,
    Runtime,
    Option<Arc<WriteAheadLog>>,
) {
    let temp_dir = TempDir::new().unwrap();
    let data_path = temp_dir.path().join("data");
    let log_path = temp_dir.path().join("wal");
    std::fs::create_dir_all(&data_path).unwrap();
    std::fs::create_dir_all(&log_path).unwrap();

    let rt = Runtime::new().unwrap();
    let wal = if let Some(mut wal_config) = wal_config {
        wal_config.log_writer_config.log_directory = log_path.clone();
        wal_config.max_active_transactions = 50_000;
        let w = rt.block_on(WriteAheadLog::new(wal_config)).unwrap();
        Some(Arc::new(w))
    } else {
        None
    };

    let page_manager = PageManager::new(
        PathBuf::from(&data_path),
        "bench_table",
        PageManagerConfig::default(),
    )
    .unwrap();
    let page_manager = Arc::new(Mutex::new(page_manager));

    (temp_dir, page_manager, rt, wal)
}

fn bench_e2e_insert_single(c: &mut Criterion) {
    let mut group = c.benchmark_group("e2e_insert_single");

    let (temp_dir, page_manager, rt, wal) = setup_e2e_env(true);

    group.bench_function("insert_with_tx_wal", |b| {
        b.iter(|| {
            let sql = "INSERT INTO bench_table (id, name, age) VALUES (1, 'Alice', 30)";
            rt.block_on(execute_insert_e2e(
                black_box(sql),
                &page_manager,
                wal.as_deref(),
            ))
            .unwrap();
        });
    });

    // Group commit: default config now uses force_flush_immediately=false
    group.bench_function("insert_with_tx_wal_group_commit", |b| {
        let (temp_dir_gc, page_manager_gc, rt_gc, wal_gc) = setup_e2e_env(true);
        b.iter(|| {
            let sql = "INSERT INTO bench_table (id, name, age) VALUES (1, 'Alice', 30)";
            rt_gc
                .block_on(execute_insert_e2e(
                    black_box(sql),
                    &page_manager_gc,
                    wal_gc.as_deref(),
                ))
                .unwrap();
        });
        drop(temp_dir_gc);
    });

    // synchronous_commit=off: max throughput, lower durability (log dir overwritten in setup)
    let (temp_dir_async, page_manager_async, rt_async, wal_async) =
        setup_e2e_env_with_wal_config(Some(WalConfig::high_throughput(PathBuf::from("."))));
    group.bench_function("insert_with_tx_wal_async_commit", |b| {
        b.iter(|| {
            let sql = "INSERT INTO bench_table (id, name, age) VALUES (1, 'Alice', 30)";
            rt_async
                .block_on(execute_insert_e2e(
                    black_box(sql),
                    &page_manager_async,
                    wal_async.as_deref(),
                ))
                .unwrap();
        });
    });

    let (temp_dir_io, page_manager_io, rt_io, _) = setup_e2e_env(false);
    group.bench_function("insert_without_wal", |b| {
        b.iter(|| {
            let sql = "INSERT INTO bench_table (id, name, age) VALUES (1, 'Alice', 30)";
            rt_io
                .block_on(execute_insert_e2e(black_box(sql), &page_manager_io, None))
                .unwrap();
        });
    });

    // insert_batch_flush: N inserts in one tx + single flush at end (plan Phase 4)
    group.bench_with_input(
        BenchmarkId::new("insert_batch_flush", 50),
        &50,
        |b, &count| {
            b.iter(|| {
                let (temp_dir_bf, page_manager_bf, rt_bf, wal_bf) = setup_e2e_env(true);
                rt_bf.block_on(async {
                    let tx_id = wal_bf
                        .as_ref()
                        .unwrap()
                        .begin_transaction(IsolationLevel::ReadCommitted)
                        .await
                        .unwrap();
                    let planner = QueryPlanner::new().unwrap();
                    let optimizer = QueryOptimizer::new().unwrap();

                    for i in 1..=count {
                        let sql = format!(
                            "INSERT INTO bench_table (id, name, age) VALUES ({}, 'User{}', {})",
                            i,
                            i,
                            20 + (i % 50)
                        );
                        let mut parser = SqlParser::new(&sql).unwrap();
                        let stmt = parser.parse().unwrap();
                        let plan = planner.create_plan(&stmt).unwrap();
                        let optimized = optimizer.optimize(plan).unwrap();

                        if let PlanNode::Insert(insert_node) = &optimized.optimized_plan.root {
                            for row_values in &insert_node.values {
                                let data = serialize_insert_values(row_values);
                                let (result, file_id) = {
                                    let mut pm_guard = page_manager_bf.lock().unwrap();
                                    let result = pm_guard.insert(&data).unwrap();
                                    let file_id = pm_guard.file_id();
                                    (result, file_id)
                                };
                                let (page_id, record_offset) = parse_record_id(result.record_id);
                                wal_bf
                                    .as_ref()
                                    .unwrap()
                                    .log_insert(tx_id, file_id, page_id, record_offset, data)
                                    .await
                                    .unwrap();
                            }
                        }
                    }

                    wal_bf
                        .as_ref()
                        .unwrap()
                        .commit_transaction(tx_id)
                        .await
                        .unwrap();
                    let mut pm = page_manager_bf.lock().unwrap();
                    pm.flush_dirty_pages().unwrap();
                });
                drop(temp_dir_bf);
            });
        },
    );

    drop(temp_dir);
    drop(temp_dir_async);
    drop(temp_dir_io);
    group.finish();
}

fn bench_e2e_insert_batch(c: &mut Criterion) {
    let mut group = c.benchmark_group("e2e_insert_batch");

    for count in [5, 10, 25] {
        group.bench_with_input(
            BenchmarkId::new("inserts_with_tx_wal", count),
            &count,
            |b, &count| {
                b.iter(|| {
                    let (temp_dir, page_manager, rt, wal) = setup_e2e_env(true);
                    rt.block_on(async {
                        let tx_id = wal
                            .as_ref()
                            .unwrap()
                            .begin_transaction(IsolationLevel::ReadCommitted)
                            .await
                            .unwrap();
                        let planner = QueryPlanner::new().unwrap();
                        let optimizer = QueryOptimizer::new().unwrap();

                        for i in 1..=count {
                            let sql =
                                format!(
                                "INSERT INTO bench_table (id, name, age) VALUES ({}, 'User{}', {})",
                                i, i, 20 + (i % 50)
                            );
                            let mut parser = SqlParser::new(&sql).unwrap();
                            let stmt = parser.parse().unwrap();
                            let plan = planner.create_plan(&stmt).unwrap();
                            let optimized = optimizer.optimize(plan).unwrap();

                            if let PlanNode::Insert(insert_node) = &optimized.optimized_plan.root {
                                for row_values in &insert_node.values {
                                    let data = serialize_insert_values(row_values);
                                    let (result, file_id) = {
                                        let mut pm_guard = page_manager.lock().unwrap();
                                        let result = pm_guard.insert(&data).unwrap();
                                        let file_id = pm_guard.file_id();
                                        (result, file_id)
                                    };
                                    let (page_id, record_offset) =
                                        parse_record_id(result.record_id);
                                    wal.as_ref()
                                        .unwrap()
                                        .log_insert(tx_id, file_id, page_id, record_offset, data)
                                        .await
                                        .unwrap();
                                }
                            }
                        }

                        wal.as_ref()
                            .unwrap()
                            .commit_transaction(tx_id)
                            .await
                            .unwrap();
                        let mut pm = page_manager.lock().unwrap();
                        pm.flush_dirty_pages().unwrap();
                    });
                    drop(temp_dir);
                });
            },
        );
    }

    group.finish();
}

fn bench_e2e_transaction_cycle(c: &mut Criterion) {
    let mut group = c.benchmark_group("e2e_transaction_cycle");

    let (temp_dir, page_manager, rt, wal) = setup_e2e_env(true);

    group.bench_function("begin_insert_commit", |b| {
        b.iter(|| {
            rt.block_on(async {
                let tx_id = wal
                    .as_ref()
                    .unwrap()
                    .begin_transaction(IsolationLevel::ReadCommitted)
                    .await
                    .unwrap();
                let data = serialize_insert_values(&["1".into(), "Test".into(), "25".into()]);
                let (result, file_id) = {
                    let mut pm_guard = page_manager.lock().unwrap();
                    let result = pm_guard.insert(&data).unwrap();
                    let file_id = pm_guard.file_id();
                    (result, file_id)
                };
                let (page_id, record_offset) = parse_record_id(result.record_id);
                wal.as_ref()
                    .unwrap()
                    .log_insert(tx_id, file_id, page_id, record_offset, data)
                    .await
                    .unwrap();
                wal.as_ref()
                    .unwrap()
                    .commit_transaction(tx_id)
                    .await
                    .unwrap();
            })
        });
    });

    drop(temp_dir);
    group.finish();
}

fn bench_e2e_select_scan(c: &mut Criterion) {
    let mut group = c.benchmark_group("e2e_select_scan");

    let (temp_dir, page_manager, rt, wal) = setup_e2e_env(true);

    // Pre-fill with data
    rt.block_on(async {
        let tx_id = wal
            .as_ref()
            .unwrap()
            .begin_transaction(IsolationLevel::ReadCommitted)
            .await
            .unwrap();
        for i in 1..=100 {
            let data = serialize_insert_values(&[
                i.to_string(),
                format!("User{}", i),
                (20 + (i % 50)).to_string(),
            ]);
            let (result, file_id) = {
                let mut pm = page_manager.lock().unwrap();
                let result = pm.insert(&data).unwrap();
                let file_id = pm.file_id();
                (result, file_id)
            };
            let (page_id, record_offset) = parse_record_id(result.record_id);
            wal.as_ref()
                .unwrap()
                .log_insert(tx_id, file_id, page_id, record_offset, data)
                .await
                .unwrap();
        }
        wal.as_ref()
            .unwrap()
            .commit_transaction(tx_id)
            .await
            .unwrap();
        let mut pm = page_manager.lock().unwrap();
        pm.flush_dirty_pages().unwrap();
    });

    group.bench_function("select_full_scan_100_rows", |b| {
        b.iter(|| {
            let mut pm = page_manager.lock().unwrap();
            let results = pm.select(None).unwrap();
            black_box(results);
        });
    });

    drop(temp_dir);
    group.finish();
}

criterion_group!(
    benches,
    bench_e2e_insert_single,
    bench_e2e_insert_batch,
    bench_e2e_transaction_cycle,
    bench_e2e_select_scan
);
criterion_main!(benches);

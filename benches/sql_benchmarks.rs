//! SQL-бенчмарки для RustDB
//!
//! Измеряет производительность парсера, планировщика и оптимизатора запросов.

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use std::hint::black_box;
use rustdb::{
    parser::SqlParser,
    planner::{QueryOptimizer, QueryPlanner},
};

fn bench_sql_parse_only(c: &mut Criterion) {
    let mut group = c.benchmark_group("sql_parse_only");

    let queries = [
        ("select_simple", "SELECT * FROM users"),
        ("select_where", "SELECT id, name FROM users WHERE age > 25"),
        (
            "select_join",
            "SELECT u.name, o.amount FROM users u JOIN orders o ON u.id = o.user_id",
        ),
        (
            "insert",
            "INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)",
        ),
        ("update", "UPDATE users SET age = 31 WHERE id = 1"),
        ("delete", "DELETE FROM users WHERE id = 1"),
        (
            "create_table",
            "CREATE TABLE test (id INTEGER, name VARCHAR(100))",
        ),
    ];

    for (name, sql) in queries {
        group.bench_function(name, |b| {
            b.iter(|| {
                let mut parser = SqlParser::new(black_box(sql)).unwrap();
                let stmt = parser.parse().unwrap();
                black_box(stmt);
            });
        });
    }

    group.finish();
}

fn bench_sql_parse_and_plan(c: &mut Criterion) {
    let mut group = c.benchmark_group("sql_parse_and_plan");

    let queries = [
        ("select_simple", "SELECT * FROM users"),
        ("select_where", "SELECT id, name FROM users WHERE age > 25"),
        (
            "insert",
            "INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)",
        ),
        ("update", "UPDATE users SET age = 31"),
        ("delete", "DELETE FROM users"),
    ];

    let mut planner = QueryPlanner::new().unwrap();

    for (name, sql) in queries {
        group.bench_function(name, |b| {
            b.iter(|| {
                let mut parser = SqlParser::new(black_box(sql)).unwrap();
                let stmt = parser.parse().unwrap();
                let plan = planner.create_plan(&stmt).unwrap();
                black_box(plan);
            });
        });
    }

    group.finish();
}

fn bench_sql_full_pipeline(c: &mut Criterion) {
    let mut group = c.benchmark_group("sql_full_pipeline");

    let queries = [
        ("select_simple", "SELECT * FROM users"),
        ("select_where", "SELECT id, name FROM users WHERE age > 25"),
        (
            "insert",
            "INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)",
        ),
        ("update", "UPDATE users SET age = 31"),
        ("delete", "DELETE FROM users"),
    ];

    let mut planner = QueryPlanner::new().unwrap();
    let mut optimizer = QueryOptimizer::new().unwrap();

    for (name, sql) in queries {
        group.bench_function(name, |b| {
            b.iter(|| {
                let mut parser = SqlParser::new(black_box(sql)).unwrap();
                let stmt = parser.parse().unwrap();
                let plan = planner.create_plan(&stmt).unwrap();
                let optimized = optimizer.optimize(plan).unwrap();
                black_box(optimized);
            });
        });
    }

    group.finish();
}

fn bench_sql_insert_batch(c: &mut Criterion) {
    let mut group = c.benchmark_group("sql_insert_batch");

    for count in [100, 1000, 10000] {
        group.bench_with_input(BenchmarkId::new("inserts", count), &count, |b, &count| {
            let mut planner = QueryPlanner::new().unwrap();
            let mut optimizer = QueryOptimizer::new().unwrap();

            b.iter(|| {
                for i in 1..=count {
                    let sql = format!(
                        "INSERT INTO users (id, name, age, email) VALUES ({}, 'User{}', {}, 'user{}@example.com')",
                        i, i, 20 + (i % 50), i
                    );
                    let mut parser = SqlParser::new(black_box(&sql)).unwrap();
                    let stmt = parser.parse().unwrap();
                    let plan = planner.create_plan(&stmt).unwrap();
                    let _ = optimizer.optimize(plan).unwrap();
                }
            });
        });
    }

    group.finish();
}

fn bench_sql_select_batch(c: &mut Criterion) {
    let mut group = c.benchmark_group("sql_select_batch");

    let select_simple = "SELECT * FROM users";
    let select_where = "SELECT id, name FROM users WHERE age > 25";

    for count in [100, 1000, 10000] {
        group.bench_with_input(
            BenchmarkId::new("select_simple", count),
            &count,
            |b, &count| {
                let mut planner = QueryPlanner::new().unwrap();
                let mut optimizer = QueryOptimizer::new().unwrap();

                b.iter(|| {
                    for _ in 0..count {
                        let mut parser = SqlParser::new(black_box(select_simple)).unwrap();
                        let stmt = parser.parse().unwrap();
                        let plan = planner.create_plan(&stmt).unwrap();
                        let _ = optimizer.optimize(plan).unwrap();
                    }
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("select_where", count),
            &count,
            |b, &count| {
                let mut planner = QueryPlanner::new().unwrap();
                let mut optimizer = QueryOptimizer::new().unwrap();

                b.iter(|| {
                    for _ in 0..count {
                        let mut parser = SqlParser::new(black_box(select_where)).unwrap();
                        let stmt = parser.parse().unwrap();
                        let plan = planner.create_plan(&stmt).unwrap();
                        let _ = optimizer.optimize(plan).unwrap();
                    }
                });
            },
        );
    }

    group.finish();
}

fn bench_sql_mixed_oltp(c: &mut Criterion) {
    let mut group = c.benchmark_group("sql_mixed_oltp");

    let operations = [
        "SELECT * FROM users WHERE age > 25",
        "INSERT INTO users (id, name) VALUES (999, 'New')",
        "UPDATE users SET name = 'Updated'",
        "DELETE FROM users",
    ];

    for count in [100, 500, 1000] {
        group.bench_with_input(BenchmarkId::new("mixed", count), &count, |b, &count| {
            let mut planner = QueryPlanner::new().unwrap();
            let mut optimizer = QueryOptimizer::new().unwrap();
            let queries: Vec<_> = (0..count).map(|i| operations[i % 4]).collect();

            b.iter(|| {
                for sql in &queries {
                    let mut parser = SqlParser::new(black_box(sql)).unwrap();
                    let stmt = parser.parse().unwrap();
                    let plan = planner.create_plan(&stmt).unwrap();
                    let _ = optimizer.optimize(plan).unwrap();
                }
            });
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_sql_parse_only,
    bench_sql_parse_and_plan,
    bench_sql_full_pipeline,
    bench_sql_insert_batch,
    bench_sql_select_batch,
    bench_sql_mixed_oltp
);
criterion_main!(benches);

//! Benchmarks for rustdb

use criterion::{criterion_group, criterion_main, Criterion};
use rustdb::Database;

fn database_creation_benchmark(c: &mut Criterion) {
    c.bench_function("database_creation", |b| {
        b.iter(|| {
            let _db = Database::new().unwrap();
        });
    });
}

fn database_open_benchmark(c: &mut Criterion) {
    let temp = tempfile::tempdir().expect("tempdir");
    let path = temp.path().join("bench_db");
    c.bench_function("database_open", |b| {
        b.iter(|| {
            let p = path.to_str().expect("path utf8");
            let _db = Database::open(p).expect("open");
        });
    });
}

criterion_group!(
    benches,
    database_creation_benchmark,
    database_open_benchmark
);
criterion_main!(benches);

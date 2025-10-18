//! Бенчмарки для rustdb

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
    c.bench_function("database_open", |b| {
        b.iter(|| {
            // TODO: Создать временную БД для тестирования
            // let _db = Database::open("temp.db").unwrap();
        });
    });
}

criterion_group!(
    benches,
    database_creation_benchmark,
    database_open_benchmark
);
criterion_main!(benches);

#!/bin/bash
# Скрипт для запуска pgbench и сравнения с RustDB
# Требует: PostgreSQL, pgbench, созданную БД

set -e

DB_NAME="${PGBENCH_DB:-pgbench_rustdb_compare}"
SCALE="${PGBENCH_SCALE:-10}"
CLIENTS="${PGBENCH_CLIENTS:-16}"
JOBS="${PGBENCH_JOBS:-4}"
DURATION="${PGBENCH_DURATION:-60}"

echo "=== PostgreSQL pgbench ==="
echo "DB: $DB_NAME, scale: $SCALE, clients: $CLIENTS, jobs: $JOBS, duration: ${DURATION}s"
echo ""

# Инициализация (если БД не существует)
if ! psql -lqt | cut -d \| -f 1 | grep -qw "$DB_NAME"; then
    echo "Creating database and initializing pgbench..."
    createdb "$DB_NAME" 2>/dev/null || true
    pgbench -i -s "$SCALE" "$DB_NAME"
fi

echo "Running pgbench (TPC-B-like)..."
pgbench -c "$CLIENTS" -j "$JOBS" -T "$DURATION" "$DB_NAME"

echo ""
echo "=== RustDB ==="
echo "Run: cargo bench --bench e2e_benchmarks"
echo "Typical: insert_with_tx_wal ~26 ms/op => ~38 TPS"

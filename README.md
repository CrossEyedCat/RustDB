# RustDB

[![CI/CD](https://github.com/CrossEyedCat/RustDB/actions/workflows/ci-cd.yml/badge.svg)](https://github.com/CrossEyedCat/RustDB/actions/workflows/ci-cd.yml)
[![codecov](https://codecov.io/gh/CrossEyedCat/RustDB/branch/main/graph/badge.svg)](https://codecov.io/gh/CrossEyedCat/RustDB)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Rust 1.90+](https://img.shields.io/badge/rust-1.90%2B-orange.svg)](https://www.rust-lang.org/)
[![dependency status](https://deps.rs/repo/github/CrossEyedCat/RustDB/status.svg)](https://deps.rs/repo/github/CrossEyedCat/RustDB)

![RustDB Logo](assets/logo.png)

Relational database engine implemented in Rust. The project provides storage, SQL parsing, planning, execution, transactions, and supporting subsystems suitable for experimentation and controlled OLTP-style workloads.

## Requirements

- **Rust toolchain**: MSRV **1.90.0** (see `rust-version` in `Cargo.toml`), required by dependencies such as `unty-next` / `virtue-next` in the `bincode-next` stack.
- **Supported platform for production-style use**: **Linux**. Other operating systems are not a supported deployment target.

## Building

```bash
cargo build --release
```

## Testing

```bash
cargo test
cargo test --test integration_tests
```

## Project status

### Implemented

- **QUIC network (experimental):** `cargo run -- server` starts the UDP listener (ALPN `rustdb-v1`) with a stub engine; `rustdb_quic_client` can run queries over loopback. See [docs/network/README.md](docs/network/README.md). Full `EngineHandle` integration with the database is still future work.
- **Parser and semantics:** lexer, AST, SELECT/INSERT/UPDATE/DELETE, CREATE TABLE/INDEX, BEGIN/COMMIT/ROLLBACK, PREPARE/EXECUTE; analyzer with types and access checks.
- **Planning and execution:** DML plan construction, optimization (some heuristics still stub cost/selectivity); executor operators (scan, filter, join, aggregates, sort, limit/offset, and others).
- **Storage and catalog:** file/page managers, tuples, B-tree and hash indexes, `SchemaManager` with storage-level DDL operations.
- **Logging:** WAL, checkpoint, compaction.
- **Transactions and concurrency:** MVCC modules, lock managers, recovery/recovery manager (see **In progress** for end-to-end wiring).
- **Infrastructure:** configuration, i18n, debugging/profiling, CI, benchmarks.

### In progress

- **Public database API** (`Database` in `lib.rs`): open/close lifecycle, coherent initialization of catalog, buffer pool, and WAL.
- **End-to-end SQL:** a single path parser → plan → `QueryExecutor` → pages/tables/indexes; the `create` / `query` CLI subcommands and the QUIC server’s engine are still stubs—there is no full “create a database and run a query” workflow through the main binary yet (the separate `rustdb_quic_client` exercises the wire path only).
- **ACID and recovery:** finishing AcidManager integration, WAL writes on commit, UNDO, isolation levels, and log-based recovery.
- **DDL and storage:** ALTER/DROP in the parser; insertion into internal B-tree nodes; buffer (flush to disk), catalog/schema, concurrent access to storage, tuple constraint validation.

MSRV and the target OS for production-style use are documented under **Requirements** above.

### Test limitations

Integration tests exercise the **parse → plan → optimize** pipeline and **simulate** DML outcomes (e.g. with counters), not full SQL execution through the executor with real on-disk pages. Some executor and storage tests are marked `#[ignore]` due to known issues (including hangs) on full runs—they do not demonstrate a production-ready “SQL → disk” path end to end.

## Documentation

- **Cookbook (Docker GHCR, CLI examples):** [docs/cookbook.md](docs/cookbook.md)
- **Network protocol (QUIC, framing, engine boundary):** see [docs/network/README.md](docs/network/README.md).

API documentation is generated with:

```bash
cargo doc --no-deps --document-private-items
```

## License

This project is licensed under the MIT License. See the `LICENSE` file in the repository root when present.

## Repository

Source and issue tracking: [GitHub](https://github.com/CrossEyedCat/RustDB).

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

## Documentation

API documentation is generated with:

```bash
cargo doc --no-deps --document-private-items
```

## License

This project is licensed under the MIT License. See the `LICENSE` file in the repository root when present.

## Repository

Source and issue tracking: [GitHub](https://github.com/CrossEyedCat/RustDB).

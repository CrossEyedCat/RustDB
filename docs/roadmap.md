# Implementation roadmap

Short, prioritized plan for RustDB beyond the current **SQL engine** (`SqlEngine`: parse → plan → execute, shared by CLI and QUIC). Context on **what is done vs in flight** stays in [README.md](../README.md) (sections *SQL-92 compatibility*, *Implemented*, *What’s still evolving*).

## Priorities (ordered)

1. **Library surface:** optional wrapper API (e.g. `Database` + owned `SqlEngine` or `Connection`) so embedders do not depend on wiring details; keep `SqlEngine` as the low-level primitive.
2. **Durability:** define a commit point: append **WAL records** for DML/DDL (or checkpointed equivalents), **`fsync` policy**, and **replay** on open; align `COMMIT` with log sequence and page state.
3. **Recovery:** integrate existing **checkpoint/recovery** modules in `src/logging/` with the **SqlEngine** data directory lifecycle; tests for crash-after-append, crash-after-commit.
4. **Isolation (later):** stronger guarantees if needed (locking upgrades, snapshot isolation), building on current `RwLock` + MVCC direction in `src/core/`.
5. **DDL / catalog:** serialize **catalog** consistently with heap files; expand **`ALTER`** (column add/drop/rewrite) and document unsupported forms; stress tests for FK/PK under concurrency.
6. **Operational clarity:** extend docs and smoke tests as behavior stabilizes (Docker stateful SQL smoke already covers constraints and session transactions).

This is a living list; adjust order as durability and recovery become blocking for real workloads.

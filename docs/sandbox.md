# RustDB sandbox (GitHub Codespaces)

Try RustDB SQL in a browser without Docker, QUIC clients, or a local Rust install. The sandbox runs inside a **GitHub Codespace**: a Linux VM with RustDB built from this repository and a small TPC-C seed dataset.

[![Open in GitHub Codespaces](https://github.com/codespaces/badge.svg)](https://codespaces.new/CrossEyedCat/RustDB?quickstart=1)

## Quick start

1. Click **Open in GitHub Codespaces** (requires a free [GitHub](https://github.com) account).
2. Wait for the dev container to finish `postCreate` (~3–5 minutes on first build: `cargo build --release`).
3. In the integrated terminal:

```bash
./target/release/rustdb --config sandbox-data/.sandbox-config.toml query "SELECT w_id, w_name FROM warehouse"
```

The seed runs automatically during setup. To wipe your changes and restore the demo data:

```bash
./scripts/sandbox-reset.sh
```

## Example queries

All examples assume the effective config written by `sandbox-init.sh`:

```bash
RUSTDB="./target/release/rustdb --config sandbox-data/.sandbox-config.toml"
```

**Browse seeded TPC-C tables**

```bash
$RUSTDB query "SELECT d_id, d_name FROM district WHERE d_w_id = 1"
$RUSTDB query "SELECT c_id, c_first, c_last, c_balance FROM customer WHERE c_w_id = 1 AND c_d_id = 1 LIMIT 5"
$RUSTDB query "SELECT i_id, i_name, i_price FROM item ORDER BY i_id"
```

**Create your own table**

```bash
$RUSTDB query "CREATE TABLE demo (id INTEGER, note VARCHAR(32))"
$RUSTDB query "INSERT INTO demo (id, note) VALUES (1, 'hello')"
$RUSTDB query "SELECT * FROM demo"
```

**Transactions (one batch file = one session)**

```bash
$RUSTDB query --batch-file - <<'SQL'
BEGIN TRANSACTION
INSERT INTO demo (id, note) VALUES (2, 'tx')
COMMIT
SQL
```

**EXPLAIN** (when supported for your statement — see [sql-explain.md](sql-explain.md))

```bash
$RUSTDB query "EXPLAIN SELECT c_id FROM customer WHERE c_w_id = 1 AND c_d_id = 1"
```

## Commands

| Command | Purpose |
|---------|---------|
| `./scripts/sandbox-init.sh` | Wipe `sandbox-data/`, rebuild `rustdb`, re-seed |
| `./scripts/sandbox-reset.sh` | Same as init (documented reset for users) |
| `RUSTDB_SANDBOX_DATA=/tmp/my-test ./scripts/sandbox-init.sh` | Custom data directory (CI smoke uses this) |

Environment variables (optional):

- `RUSTDB_SANDBOX_DATA` — data directory (default: `./sandbox-data`)
- `RUSTDB_CONFIG` — base TOML template (default: `config/sandbox.toml`)

## What you get

- **CLI only** — `rustdb query` against a local `SqlEngine` on disk under `sandbox-data/`.
- **Seed schema** — minimal TPC-C tables from [`scripts/tpcc_seed.sql`](../scripts/tpcc_seed.sql) (1 warehouse, districts, customers, items, stock, orders).
- **Ephemeral storage** — data lives on the Codespace disk. Deleting the Codespace removes it. Use `sandbox-reset.sh` to restore the seed inside an active session.

## Limitations

- **GitHub account required** — there is no anonymous public SQL URL.
- **Not PostgreSQL** — syntax and semantics follow RustDB’s [SQL-92 subset](../README.md#sql-92-compatibility), not `psql`.
- **No browser SQL UI** — no HTTP API or web editor in this repo; use the terminal (or VS Code SQL extensions against files you run manually).
- **Linux VM** — matches CI; Windows/macOS native builds are not a supported deployment target.
- **Not for production** — no HA, no managed backups, query timeout 60s (see `config/sandbox.toml`).

## Local smoke (without Codespaces)

On Linux (or WSL):

```bash
RUSTDB_SANDBOX_DATA=/tmp/sandbox-test ./scripts/sandbox-init.sh
./target/release/rustdb --config /tmp/sandbox-test/.sandbox-config.toml query "SELECT w_id FROM warehouse"
```

## See also

- [Cookbook](cookbook.md) — Docker / GHCR workflows
- [Network (QUIC)](network/README.md) — wire protocol when you outgrow the sandbox
- [CONTRIBUTING.md](../CONTRIBUTING.md) — build and test the full project

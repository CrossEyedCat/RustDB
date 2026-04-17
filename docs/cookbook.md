# RustDB cookbook

Hands-on examples for the **GitHub Container Registry** image [`ghcr.io/crosseyedcat/rustdb`](https://github.com/CrossEyedCat/RustDB/pkgs/container/rustdb). Commands below match the current CLI and packaged `config.toml`; re-verify after upgrading images (see [Verification](#verification)).

This cookbook focuses on:

- Running RustDB **from Docker** with persistent data
- Using the **CLI** (`info`, `create`, `query`, `server`)
- Understanding configuration and the most useful **env vars**
- Common workflows (batch SQL, QUIC server, smoke scripts, benchmarks)

## Image and tags

CI publishes several tag styles (see [`.github/workflows/ci-cd.yml`](../.github/workflows/ci-cd.yml) and `docker/metadata-action`):

| Tag / pattern | Purpose |
|----------------|---------|
| `latest` | Default branch when enabled by metadata |
| `main`, `develop` | Branch builds |
| `sha-<short>` | Short Git SHA (reproducible alongside branch tags) |
| `pr-<n>` | Pull-request pipelines |

For bit-for-bit reproducibility, pin by **digest** from the package page:

```bash
export RUSTDB_IMAGE="ghcr.io/crosseyedcat/rustdb:main"
docker pull "$RUSTDB_IMAGE"
# Optional: export RUSTDB_IMAGE="ghcr.io/crosseyedcat/rustdb@sha256:<digest>"
```

**Note:** the binary in the image may lag behind `main` in Git. Always check `rustdb server --help` and `/app/config/config.toml` inside the container. For QUIC framing, TLS leaf export, and clients, see [docs/network/README.md](network/README.md).

---

## 1. Version

```bash
docker run --rm "$RUSTDB_IMAGE" rustdb --version
```

Expected: `rustdb 0.1.0` (or the version in the image’s `Cargo.toml`).

---

## 2. System information

```bash
docker run --rm "$RUSTDB_IMAGE" rustdb info
```

Prints version, default language, OS, and architecture inside the container (typically `linux` / `x86_64` or `aarch64`).

---

## 3. Interface language (i18n)

```bash
docker run --rm "$RUSTDB_IMAGE" rustdb language list
docker run --rm "$RUSTDB_IMAGE" rustdb language show
docker run --rm "$RUSTDB_IMAGE" rustdb language set en
```

---

## 4. SQL via CLI (`query`)

`rustdb query` runs SQL **locally** through `SqlEngine` (same parse → plan → execute path as the QUIC server), using the configured data directory.

```bash
docker run --rm "$RUSTDB_IMAGE" rustdb query "SELECT 1"
docker run --rm "$RUSTDB_IMAGE" rustdb query "SELECT 1" -d mydb
```

The second form uses `-d` / `--database` to resolve data under `data_directory` from config (default `./data`, i.e. `/app/data` when using the image layout).

### Batch mode (single session, multi-statement)

Use `--batch-file` to run **one statement per line** in a single process, using one `SessionContext`.
This is required if you want to test transactions (`BEGIN/COMMIT/ROLLBACK`) across multiple statements.

Example via stdin (`--batch-file -`):

```bash
docker run --rm -i \
  -v rustdb-data:/app/data \
  "$RUSTDB_IMAGE" \
  sh -c 'rustdb --config /app/config/config.toml query --batch-file -' <<'SQL'
BEGIN TRANSACTION
INSERT INTO demo_t (a) VALUES (1)
ROLLBACK
SQL
```

Coverage of SQL features is still growing; see [README.md](../README.md) for project status and test limitations.

---

## 5. Create a database directory (`create`)

`rustdb create` creates a **directory** for a named database and writes a small `.rustdb` marker file. It does not register a full catalog entry everywhere in the stack—treat it as a filesystem helper.

```bash
docker run --rm "$RUSTDB_IMAGE" rustdb create demo --data-dir /app/data
```

Ephemeral containers lose `/app/data` unless you mount a volume (next sections).

---

## 6. Server in the background (QUIC / UDP)

The network entrypoint is **QUIC over UDP** with ALPN `rustdb-v1`. Map ports with **`/udp`**.

The image ships [`config.toml`](../config.toml) at `/app/config/config.toml` with **`network.port = 5432`**. Recommended invocation:

```bash
docker rm -f rustdb-server 2>/dev/null || true
docker run -d --name rustdb-server \
  -p 15432:5432/udp \
  -v rustdb-data:/app/data \
  "$RUSTDB_IMAGE" \
  rustdb --config /app/config/config.toml server --host 0.0.0.0 --cert-out /tmp/server.der
```

- `--host 0.0.0.0` is required for connections from the host or other containers.
- `--cert-out` writes the dev TLS leaf (DER) for `rustdb_quic_client --cert` (copy out with `docker cp` if needed).

Stop:

```bash
docker stop rustdb-server && docker rm rustdb-server
```

`Dockerfile` still **EXPOSE**s `8080`/`8081` for historical layout; the **QUIC listener port** comes from config or `--port`. To use another port: `--port 8080` and `-p 18080:8080/udp`.

---

## 7. Data and config on disk

Example: named volume for data and a read-only host config (paths follow the [Dockerfile](../Dockerfile)):

```bash
docker run --rm \
  -v rustdb-data:/app/data \
  -v "$(pwd)/config.toml:/app/config/config.toml:ro" \
  "$RUSTDB_IMAGE" \
  rustdb --config /app/config/config.toml info
```

### Config keys (practical reference)

The container expects a config file like `/app/config/config.toml` (see repository `config.toml`).
The most used keys in the current image:

- **`data_directory`**: base directory for database storage (in the image: `/app/data`)
- **`language`**: interface language (e.g. `en`)
- **`network.host` / `network.port`**: QUIC/UDP listener bind address and port

### Environment variables (common)

RustDB reads a small set of env vars used in different parts of the stack:

- **`RUST_LOG`**: logging filter for `tracing-subscriber` (server)
- **`RUSTDB_TRACE_CHROME_PATH`**: when set, enables `tracing-chrome` JSON trace output (server)
- **`RUSTDB_NAME`**, **`RUSTDB_DATA_DIR`**, **`RUSTDB_MAX_CONNECTIONS`**, **`RUSTDB_LANGUAGE`**: config overrides used by `DatabaseConfig::from_env()` (CLI / config helpers)

Example:

```bash
docker run --rm \
  -e RUST_LOG=info \
  -e RUSTDB_TRACE_CHROME_PATH=/app/logs/trace.json \
  -v rustdb-data:/app/data \
  "$RUSTDB_IMAGE" \
  rustdb --config /app/config/config.toml info
```

---

## 8. QUIC client (build from this repository)

The GHCR image contains only the **`rustdb`** binary. Build **`rustdb_quic_client`** (and optionally **`rustdb_load`**) from source:

```bash
git clone https://github.com/CrossEyedCat/RustDB.git && cd RustDB
cargo build --release --bin rustdb_quic_client
```

Then follow [docs/network/README.md](network/README.md): run `rustdb server`, pass `--cert` / `--server-name`, and use `--addr` for `host:port`.

---

## Verification

The repository script re-runs the main checks against an image (default `ghcr.io/crosseyedcat/rustdb:main`):

```bash
./scripts/verify-cookbook-docker.sh
# or: RUSTDB_IMAGE=ghcr.io/crosseyedcat/rustdb:sha-abc1234 ./scripts/verify-cookbook-docker.sh
```

---

## Benchmark via GHCR (QUIC + SQLite comparison)

[`scripts/bench_via_ghcr_image.sh`](../scripts/bench_via_ghcr_image.sh) pulls the image, uses a data volume, warms up `bench_t`, starts `rustdb server` with `--cert-out`, copies the leaf cert to the host, and runs [`scripts/bench_sqlite_vs_rustdb.py`](../scripts/bench_sqlite_vs_rustdb.py).

```bash
export RUSTDB_IMAGE="ghcr.io/crosseyedcat/rustdb:main"
./scripts/bench_via_ghcr_image.sh
```

By default this script runs the server on **UDP port 8080** inside the container (`UDP_PORT` on the host maps to `8080/udp`). That differs from the packaged `/app/config/config.toml` default (**5432**) but matches the script’s expectations; set `UDP_PORT` if you need a different host port.

On **Windows** with Docker Desktop, you can use the PowerShell helper:

```powershell
$env:RUSTDB_IMAGE = "ghcr.io/crosseyedcat/rustdb:main"
.\scripts\bench_via_ghcr_image.ps1
```

Or Git Bash: `"C:\Program Files\Git\bin\bash.exe" -lc './scripts/bench_via_ghcr_image.sh'`.

Optional: set `POSTGRES_DSN` for Postgres rows in the report. Output defaults under `target/bench_docker_ghcr/` (or `OUT_DIR` if set).

---

## Profiling image (optional)

The [Dockerfile](../Dockerfile) defines a **`profiler`** stage (Linux `perf` + `cargo flamegraph`). Example:

```bash
docker build -t rustdb-prof --target profiler .
# See comments in Dockerfile for run invocation
```

For CI-style flame graphs and tracing, see [CONTRIBUTING.md](../CONTRIBUTING.md) (`workflow_dispatch` jobs).

---

## `docker-compose.yml` in this repo

[`docker-compose.yml`](../docker-compose.yml) is an **illustrative** stack (Redis, Prometheus, Grafana, etc.). Comments mentioning HTTP/gRPC ports do **not** match the current QUIC/UDP server. For a minimal RustDB setup, prefer the `docker run` examples above and override the service `command` to `rustdb --config /app/config/config.toml server ...` with **UDP** port mappings if you adapt Compose.

---

## See also

- [README.md](../README.md) — goals, status, and test limitations  
- [Network (QUIC)](network/README.md) — protocol and client/server boundary  
- [Dockerfile](../Dockerfile), [`docker-compose.yml`](../docker-compose.yml) — build and optional orchestration  
- [CONTRIBUTING.md](../CONTRIBUTING.md) — CI jobs and contribution workflow  

---

## CLI quick reference

Run `--help` inside the container to confirm flags for the image you pinned:

```bash
docker run --rm "$RUSTDB_IMAGE" rustdb --help
docker run --rm "$RUSTDB_IMAGE" rustdb query --help
docker run --rm "$RUSTDB_IMAGE" rustdb server --help
```

Common commands:

- **`rustdb info`**: prints system info
- **`rustdb create <name>`**: creates a database directory (filesystem helper)
- **`rustdb query <SQL>`**: runs one statement
- **`rustdb query --batch-file <path|->`**: runs one statement per line (transactions span lines)
- **`rustdb server`**: starts the QUIC/UDP server

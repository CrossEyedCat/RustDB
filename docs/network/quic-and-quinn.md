# QUIC and quinn

## Why QUIC

- **Encryption by default:** TLS 1.3 is integrated into the QUIC handshake; there is no separate “plain TCP then STARTTLS” path comparable to classic PostgreSQL.
- **Stream multiplexing:** multiple independent bidirectional streams per connection without head-of-line blocking across streams (unlike a single TCP byte stream).
- **Modern loss recovery and migration:** useful for long-lived clients; details are handled by the QUIC stack.

RustDB chooses QUIC as the **only** first-class transport for the custom application protocol described in [framing.md](framing.md).

## Role of quinn

[quinn](https://github.com/quinn-rs/quinn) is the Rust implementation of QUIC used to:

- Create **server** and **client** `Endpoint`s bound to UDP sockets.
- **Accept** or **connect** `Connection`s.
- **Open** bidirectional or unidirectional **streams** and read/write bytes.

The application protocol (frames, message types) is **not** defined by quinn; quinn only delivers reliable byte streams within each QUIC stream.

## ALPN

Application-Layer Protocol Negotiation identifies the upper protocol inside TLS.

- The RustDB QUIC server uses **`rustdb-v1`** (constant [`ALPN_RUSTDB_V1`](../../src/network/server.rs) in `network::server`).
- Server and client `crypto` / transport config in quinn must use the **same** ALPN list so the handshake succeeds.

## Certificates and keys

### Development

- Use **self-signed** certificates, commonly generated with **[rcgen](https://crates.io/crates/rcgen)** or `openssl`, loaded into quinn’s `ServerConfig` / `ClientConfig`.
- The QUIC server exposes the **leaf DER** via [`QuicServer::pinned_certificate`](../../src/network/server.rs). Add that single cert to the client [`rustls::RootCertStore`](https://docs.rs/rustls) (see [`network::client::build_quinn_client_config`](../../src/network/client.rs)).

#### Saving the dev leaf certificate to a file

After [`QuicServer::bind`](../../src/network/server.rs), write the same bytes the client will trust:

```rust
std::fs::write("server.der", srv.pinned_certificate().as_ref())?;
```

Use that path as **`--cert`** for [`rustdb_quic_client`](../../src/bin/rustdb_quic_client.rs).

#### Running the main binary server

From the repo root, with [`config.toml`](../../config.toml) (optional `[network]` section) or overrides:

```bash
cargo run -- server --cert-out server.der
# or e.g.
cargo run -- server --host 127.0.0.1 --port 5432 --cert-out server.der
```

This starts [`QuicServer`](../../src/network/server.rs) with a [`StubEngine`](../../src/network/engine.rs) until Ctrl+C. Listen address and `network.max_connections` come from **`[network]`** in the config file unless **`--host`** / **`--port`** are set.

#### Running the CLI client

With the server listening (see above), use the same port and the written DER:

```bash
cargo run --bin rustdb_quic_client -- --addr 127.0.0.1:5432 --cert server.der --server-name 127.0.0.1 "SELECT 1"
```

For programmatic bring-up without the binary, you can still call **`QuicServer::bind`**, **`std::fs::write`** for the cert, and **`QuicServer::run`** with an [`EngineHandle`](../../src/network/engine.rs). Automated loopback coverage: **`cargo test quic_loopback`** (see [`tests/quic_network.rs`](../../tests/quic_network.rs)).

Use `--server-name` that matches the certificate’s SAN (for the default dev server bound to `127.0.0.1`, the name is typically `127.0.0.1`).

#### Graceful shutdown

Clone the [`quinn::Endpoint`](https://docs.rs/quinn) from [`QuicServer::endpoint`](../../src/network/server.rs) **before** spawning the task that runs [`QuicServer::run`](../../src/network/server.rs) (since `run` takes `&self`, you can also keep an [`Arc<QuicServer>`](../../src/network/server.rs)). Then:

1. Call [`QuicServer::initiate_shutdown`](../../src/network/server.rs) on that endpoint clone to stop accepting and begin closing connections.
2. Await [`QuicServer::wait_idle`](../../src/network/server.rs) on the same endpoint so existing work drains.

### Production (outline)

- Use certificates from your PKI or **ACME** (e.g. Let’s Encrypt) where applicable; QUIC still uses TLS 1.3, so standard practices apply.
- Automate rotation and document minimum key sizes and allowed cipher suites as enforced by the TLS stack bundled with quinn.

## Timeouts and limits

These map conceptually to existing [`ServerConfig`](../../src/network/server.rs) fields and future extensions:

| Concern | QUIC / quinn concept | Notes |
|--------|----------------------|--------|
| Idle connection | `max_idle_timeout` in transport params | Close connection if no activity; align with `connection_timeout` in `ServerConfig`. |
| Handshake time | implicit in connect | Fail dial if handshake does not complete in time (application-level timer). |
| Max clients | accept loop + semaphore | Cap concurrent `Connection`s to `max_connections` in `ServerConfig`. |
| Max frame payload | application | `ServerConfig::max_frame_payload_bytes` (clamped to protocol max in `StreamPolicy`). |
| Per-query timeout | application | Close stream or cancel task if engine does not respond (not QUIC-specific). |
| Ops metrics | application | `QuicServer::metrics()` — handshakes, refuse, read-frame errors, `queries_ok` / `queries_error_response` / `queries_write_failed`, bytes, latency sum. |

## Shared transport configuration (server and client)

RustDB builds a single [`TransportConfig`](https://docs.rs/quinn) shape from [`build_rustdb_transport_config`](../../src/network/transport.rs) for both the listener and [`build_quinn_client_config`](../../src/network/client.rs) / [`build_quinn_client_config_with_limits`](../../src/network/client.rs), so benchmarks are not skewed by asymmetric flow-control defaults.

- **`max_concurrent_bidi_streams`** is set from the same cap as [`ServerConfig::max_concurrent_streams_per_connection`](../../src/network/server.rs), which also drives the Tokio [`Semaphore`](../../src/network/query_stream.rs) in `run_connection_streams`. The QUIC limit should never be *below* the app’s concurrent stream work (here they match).
- **`keep_alive_interval`** is derived from `connection_timeout` / idle settings so long-lived connections send periodic traffic when needed.
- **`send_fairness(false)`** matches quinn’s guidance for many small request/response streams.

## Profiling (tracing)

To separate **QUIC/framing** from **SQL execution** in traces or Chrome JSON output, filter on these `tracing` spans (see [`query_stream.rs`](../../src/network/query_stream.rs)):

| Span | Phase |
|------|--------|
| `network.read_frame` | Reading one application frame from the QUIC stream |
| `dispatch_client_frame` | Decode → engine → encode (parent of `sql.query` for queries) |
| `sql.query` | Parse/plan/execute inside `dispatch_client_frame` |
| `network.write_response` | Writing the response bytes to the send half |

### Working hypothesis (before measuring)

A simple first guess: for **small** queries (e.g. `SELECT 1` or one-row lookups), **most wall time per request is inside `dispatch_client_frame` / `sql.query`**, not in `network.read_frame` or `network.write_response`, unless concurrency is so high that scheduling/QUIC dominates. **Validate only with a trace** — open the Chrome JSON and compare inclusive durations of those spans.

### Chrome trace file (server)

The server writes a **Chrome tracing** JSON when the env var is set (see [`cli.rs`](../../src/cli.rs)):

```bash
export RUSTDB_TRACE_CHROME_PATH="$PWD/target/rustdb-chrome-trace.json"
export RUST_LOG=info
cargo run -- server --host 127.0.0.1 --port 5432 --cert-out server.der
# In another terminal (TLS name must match cert SAN — use 127.0.0.1, not localhost):
# cargo run --bin rustdb_load -- --addr 127.0.0.1:5432 --cert server.der --server-name 127.0.0.1 --queries 30 --concurrency 1 --sql "SELECT 1"
# Then Ctrl+C the server to flush the trace.
```

Open `target/rustdb-chrome-trace.json` in `chrome://tracing` (Load). Spans above are emitted at **info**, so the default `RUST_LOG=info` is enough.

**Reading totals:** the **Totals** row can be misleading if a parent span used to wrap the whole process (now avoided). For “where did one query spend time?”, use **average wall duration** of `dispatch_client_frame`, `sql.query`, `network.read_frame`, and `network.write_response`, and **zoom** the timeline to the short bars when `rustdb_load` was running — not the long idle gap while the server waited for Ctrl+C.

On Windows, use [`scripts/run_server_chrome_trace.ps1`](../../scripts/run_server_chrome_trace.ps1).

Suggested load (not for comparing systems, only for slicing time): a short `rustdb_load --queries 50 --concurrency 1` so the trace stays small.

Suggested load scenarios when comparing numbers: `rustdb_load --stream-batch 1` vs higher values; `shared` vs `per-worker` connection mode; sweep concurrency against server `max_concurrent_streams_per_connection`.

## UDP buffer sizes and connection limits

Operating systems often default to **small UDP receive (and sometimes send) buffers**. Under high QPS, increase **`SO_RCVBUF` / `SO_SNDBUF`** on the listening socket and on client endpoints where the platform allows (Linux: `sysctl net.core.rmem_max` / `wmem_max`; Windows: registry or socket APIs). Quinn may not expose every knob; tuning at the OS level is still the first fix when the UDP path drops datagrams before user space.

Also ensure the benchmark does not hit **`ServerConfig::max_connections`**: the accept loop refuses new QUIC handshakes when `endpoint.open_connections()` reaches that cap (see [`QuicServer::run`](../../src/network/server.rs)).

## Benchmark fairness (`rustdb_load` vs TCP baselines)

When publishing comparisons against PostgreSQL (TCP) or SQLite (in-process), record at least:

- `rustdb_load` **`--connection-mode`**, **`--stream-batch`**, **`--quic-max-streams`**, **`--quic-idle-secs`** (defaults and overrides).
- Server-side **`max_concurrent_streams_per_connection`** and **`max_connections`** relative to client concurrency.

The script [`scripts/bench_sqlite_vs_rustdb.py`](../../scripts/bench_sqlite_vs_rustdb.py) forwards QUIC-related flags into `rustdb_load` and writes them into the generated `bench.md` for reproducibility.

### Optimization matrix (stream batch vs latency)

For **`select_table`**, **`shared`**, **`concurrency 128`**, sweep **`--stream-batch`** `1` vs `8` vs `16` and record **QPS** and **p50 / p95 / p99** from `bench.csv` / `bench.md`:

- Windows: [`scripts/run_optimization_matrix.ps1`](../../scripts/run_optimization_matrix.ps1)
- Unix: [`scripts/run_optimization_matrix.sh`](../../scripts/run_optimization_matrix.sh)

Requires a running server at the chosen `--addr` and a matching `--cert`. Stock **`rustdb server`** defaults to **`max_concurrent_streams_per_connection = 256`**, aligned with `rustdb_load --quic-max-streams` (and with automatic raise in **`shared`** mode so the cap is at least **`--concurrency`**).

### Trace under load

For Chrome tracing with **more than one in-flight query** (or a long run), start the traced server ([`scripts/run_server_chrome_trace.ps1`](../../scripts/run_server_chrome_trace.ps1)), optionally with **`-LoadConcurrency 128`** to print a ready-made `rustdb_load` line, or run [`scripts/trace_under_load.ps1`](../../scripts/trace_under_load.ps1) / [`scripts/trace_under_load.sh`](../../scripts/trace_under_load.sh) in a second terminal. Compare relative time in **`network.read_frame`**, **`dispatch_client_frame`**, and **`sql.query`** spans (see § Profiling above).

## References

- quinn repository: [https://github.com/quinn-rs/quinn](https://github.com/quinn-rs/quinn)
- QUIC: IETF RFC 9000 series (invariants, transport, recovery).

# Network layer implementation checklist

Actionable steps to go from the current stubs (`src/network/server.rs`, `connection.rs`) to a working QUIC server that speaks the [framing](framing.md) protocol and calls an [engine handle](engine-boundary.md). Order is suggested; some items can run in parallel.

---

## Phase 0 — Dependencies and project wiring

_Progress: core Phase 0 items completed **2025-03-25**._

- [x] Add **`quinn`** (and **`rustls`** / **`rcgen`** as needed per quinn’s current docs) to `Cargo.toml`; align **tokio** features with quinn (already using `tokio` with `full`). — **Done:** `quinn` 0.11 in `[dependencies]`; **`rustls`** pulled transitively by quinn (no separate entry); **`rcgen`** 0.14 in `[dev-dependencies]` for tests/examples; tokio left at **`full`** (no change).
- [x] Choose **serde wire format** and add the crate: **[postcard](https://crates.io/crates/postcard)** (recommended in [README](README.md)) *or* **`bincode-next`** already in the repo—pick one, document it in [framing.md](framing.md) and use it consistently for frame payloads. — **Done:** **`postcard`** 1.x with `use-std` in `Cargo.toml`; [framing.md](framing.md) treats postcard as the on-wire codec; keep **`bincode-next`** for existing storage / on-disk formats only—do not mix codecs on the same wire message type.
- [ ] Add a **`network`** or **`quic`** feature flag if you want optional builds without UDP/TLS for minimal CI (optional). — _Not done; deferred until QUIC code lands or CI needs a lean default build._

---

## Phase 1 — Application framing (no network)

_Progress: completed **2025-03-25** — `src/network/framing/`._

- [x] Define Rust types for the [frame header](framing.md): magic `RDB1`, `u16` protocol version, `u16` message kind, `u32` LE payload length (12-byte fixed header). — **`FrameHeader`**, constants in `framing/header.rs`.
- [x] Define **`ClientMessage`** / **`ServerMessage`** (or a single **`WireMessage`**) enums with `serde` derives; assign numeric **message kind** values and reject unknown kinds. — **`MessageKind`** `1`…`6`; inner payloads use serde + postcard; unknown kinds → `ProtocolError::UnknownMessageKind`.
- [x] Implement **encode**: header + serde payload → `Vec<u8>` or write to `Write`. — `encode_client_message` / `encode_server_message` and `*_write` in `framing/codec.rs`.
- [x] Implement **decode**: read 12 bytes → validate magic/version → read `payload_len` bytes → deserialize; handle truncation and **protocol errors** (see [framing.md](framing.md)). — `decode_client_frame` / `decode_server_frame` + `ProtocolError` in `framing/error.rs`.
- [x] **Unit tests**: roundtrip per message variant; wrong magic; version mismatch; oversized payload (respect a max frame size constant). — `src/network/tests/framing_tests.rs`.

---

## Phase 2 — Engine boundary (stub first)

_Progress: stub API and tests **2025-03-25** — `src/network/engine.rs`._

- [x] Introduce trait **`EngineHandle`** (or equivalent name) as in [engine-boundary.md](engine-boundary.md): `execute_sql(&self, sql: &str, ctx: &mut SessionContext) -> Result<EngineOutput, EngineError>`.
- [x] Define minimal **`SessionContext`** (empty struct or session id placeholder).
- [x] Define **`EngineOutput`** (rows + column metadata vs ok-without-rows) and **`EngineError`** (stable code + message for wire mapping).
- [x] Provide **`StubEngine`** returning empty result set or fixed error—enables server tests without `Database` wired.
- [ ] Later: implement **`EngineHandle` for `Database`** (or a wrapper) when `Database` exposes real `execute_sql` ([`src/lib.rs`](../../src/lib.rs)).

---

## Phase 3 — QUIC server skeleton (quinn)

_Progress: skeleton **2025-03-25** — [`server.rs`](../../src/network/server.rs)._

- [x] Load **TLS credentials**: dev self-signed cert (e.g. **rcgen**) into `ServerConfig`; set **ALPN** to the token chosen in [quic-and-quinn.md](quic-and-quinn.md) (e.g. `rustdb-v1`). — **`build_quinn_server_config`**, ALPN **`rustdb-v1`** (`ALPN_RUSTDB_V1`), `rustls` + one-time `CryptoProvider` install.
- [x] Bind UDP **`Endpoint`** from `ServerConfig` in [`server.rs`](../../src/network/server.rs) (`host` / `port` from existing struct). — **`QuicServer::bind`** → `quinn::Endpoint::server`.
- [x] **Accept loop**: `incoming` connections; spawn task per connection; apply **`max_connections`** semaphore/counter. — **`QuicServer::run`**: `endpoint.open_connections() >= max_connections` → `refuse()`; else `tokio::spawn` per handshake.
- [x] Map **`connection_timeout`** / idle behavior to QUIC transport parameters (`max_idle_timeout`) per [quic-and-quinn.md](quic-and-quinn.md). — `TransportConfig::max_idle_timeout` from `ServerConfig::connection_timeout`.
- [x] Log accept errors and connection close reasons (**tracing** / `log` already in project). — `tracing::info!` / `warn!` on bind, handshake failure, close.

---

## Phase 4 — Stream model and request/response path

**Variant A** chosen — see [stream-models.md](stream-models.md) and `src/network/query_stream.rs`.

### Variant A (one connection, many bidirectional streams)

- [x] On each new **bidirectional stream**, read frames from recv half, write responses to send half. — **`run_connection_streams`** / **`handle_query_bidi_stream`**.
- [x] Limit concurrent streams per connection; handle stream reset on cancel. — **`Semaphore`** (`max_concurrent_streams_per_connection`); **`send.reset`** on hard failures.

### Variant B (single REPL stream)

- [ ] _Not implemented — Variant A only._

### Common (implemented for Variant A)

- [x] Read loop: **decode frame** → if `Query`, call **`EngineHandle::execute_sql`** → **encode** `ResultSet` / `ExecutionOk` / `Error` frames. — **`dispatch_client_frame`**.
- [x] **Per-query timeout** (tokio `timeout` around engine call); on expiry, send `Error` and reset stream if appropriate. — **`tokio::time::timeout`** + `Error` frame with `QUERY_TIMEOUT`; **`send.reset`** if error frame cannot be sent.
- [x] Enforce **max SQL length** / **max rows** at network layer if policy requires it. — **`ServerConfig.max_sql_bytes`**, **`max_result_rows`** (`StreamPolicy`).

---

## Phase 5 — QUIC client (minimal)

_Progress: **2025-03-25** — `src/network/client.rs`, `src/bin/rustdb_quic_client.rs`._

- [x] Client **`Endpoint`** with TLS (trust dev cert or pinned CA). — **`build_quinn_client_config`** + [`QuicServer::pinned_certificate`](../../src/network/server.rs) (DER leaf).
- [x] **Dial** server, **open stream(s)** per chosen stream model. — **`connect`**, Variant A **`query_once`** (`open_bi`).
- [x] Send one **`Query`** frame; read response frames until complete. — **`query_once`** (one response frame for v1).
- [x] Small **binary** under `src/bin/` or integration test helper—enough to validate the server manually. — **`rustdb_quic_client`** (`--cert` DER); unit test `quic_client_query_roundtrip_localhost`.

---

## Phase 6 — Hardening and operations

_Progress: completed **2025-03-25** — `network::metrics`, `ServerConfig::max_frame_payload_bytes`, [`QuicServer::initiate_shutdown`](../../src/network/server.rs) / [`wait_idle`](../../src/network/server.rs), [`debug::record_network_query_latency_ms`](../../src/debug/mod.rs), [quic-and-quinn.md](quic-and-quinn.md) runbook, [`tests/quic_network.rs`](../../tests/quic_network.rs)._

- [x] **Metrics** (optional): active connections, streams, bytes, query latency hooks (tie into existing [`debug`](../../src/debug/) if useful). — **`QuicNetworkMetrics`** (ok / error-response / write-fail, read-frame errors, handshake failures, connections refused) + **`record_network_query_latency_ms`** (trace target `rustdb::network::metrics`); tests in **`src/network/tests/metrics_tests.rs`**, **`tests/quic_network.rs`**.
- [x] **Limits**: max frame size, max concurrent connections, graceful shutdown (stop accept, drain). — **`max_frame_payload_bytes`** on [`ServerConfig`](../../src/network/server.rs); **`max_connections`** unchanged; **`QuicServer::initiate_shutdown`** + **`QuicServer::wait_idle`** (use a clone of [`QuicServer::endpoint`](../../src/network/server.rs) while [`run`](../../src/network/server.rs) is active).
- [x] **Documentation**: update [quic-and-quinn.md](quic-and-quinn.md) with exact commands to generate certs and run server/client.
- [x] **Integration tests**: loopback QUIC with **`StubEngine`**; no flaky timing (use short timeouts). — **`tests/quic_network.rs`** (+ existing `quic_client_query_roundtrip_localhost` in crate tests).

---

## Phase 7 — CLI and product integration

_Progress: completed **2025-03-25** — [`server`](../../src/cli.rs) subcommand, [`DatabaseConfig.network`](../../src/common/config.rs), root [**README**](../../README.md)._

- [x] Wire **`server` subcommand** in [`src/cli.rs`](../../src/cli.rs) to start the QUIC endpoint with config path / listen address. — **`rustdb server`** binds [`QuicServer`](../../src/network/server.rs); **`--host` / `--port`** override [`[network]`](../../config.toml) from `config.toml`; **`--cert-out`** writes the dev leaf DER; Ctrl+C triggers graceful shutdown.
- [x] Mention the server in root [**README.md**](../../README.md) **Project status** when it becomes usable.

---

## Explicit non-goals (keep out of v1 scope)

- PostgreSQL wire protocol over TCP.
- Replication / clustering on the wire.
- Full authN/authZ (TLS identity only unless you add a later phase).

---

## Reference map

| Topic | Document |
|-------|----------|
| Layers | [architecture.md](architecture.md) |
| QUIC / TLS / ALPN | [quic-and-quinn.md](quic-and-quinn.md) |
| Streams A vs B | [stream-models.md](stream-models.md) |
| Frame bytes | [framing.md](framing.md) |
| Engine trait | [engine-boundary.md](engine-boundary.md) |
| Diagrams | [diagrams.md](diagrams.md) |

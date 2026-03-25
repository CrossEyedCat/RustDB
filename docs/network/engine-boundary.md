# Engine boundary

This document defines the **contract** between the QUIC network server and the **database engine** (SQL parsing, planning, execution, storage). The network layer must remain a **thin adapter**: decode frames, call the engine, encode responses.

## Responsibilities

### Network layer (`src/network`)

- Accept QUIC connections and manage streams per [stream-models.md](stream-models.md).
- Read/write [framing.md](framing.md) frames; enforce **per-stream** and **per-connection** timeouts where configured.
- Map decoded **Query** messages to engine calls; map engine output to **ResultSet**, **ExecutionOk**, or **Error** frames.
- Optionally enforce **policy** limits: max SQL length, max rows returned per query (soft caps before the engine runs).

### Engine layer (`Database`, executor, storage)

- **Parse** SQL, **analyze**, **plan**, **optimize**, and **execute** against real or stub storage.
- Manage **transactions**, **sessions**, and **catalog** state (future work as `Database` in [`src/lib.rs`](../../src/lib.rs) grows).
- Return **structured results** (columns, rows) or **typed errors** (syntax, semantic, runtime) independent of QUIC.

The network layer **must not** duplicate SQL parsing for correctness; it may only inspect SQL for policy (length, denylist) if needed.

## v1 abstract API (implemented)

The **engine handle** trait and types live in **`src/network/engine.rs`** (`crate::network::engine`):

```rust
pub trait EngineHandle: Send + Sync {
    fn execute_sql(
        &self,
        sql: &str,
        ctx: &mut SessionContext,
    ) -> Result<EngineOutput, EngineError>;
}
```

- **`SessionContext`:** placeholder (`session_id: Option<u64>`); extend with default schema, transaction state, and principal later.
- **`EngineOutput`:** `ResultSet { columns, rows }` or `ExecutionOk { rows_affected }`; maps to framing via `into_server_message()`.
- **`EngineError`:** `{ code: u32, message: String }` with `engine_error_code` constants; maps to `Error` frames via `Into<ErrorPayload>`.

**`StubEngine`** returns a fixed `EngineOutput` or `EngineError` for tests until `Database` implements `EngineHandle`.

## Mapping to `Database`

Today [`Database`](../../src/lib.rs) is a stub (`new` / `open` / `close` TODO). The intended evolution:

1. **`Database`** owns or references catalog, buffer pool, WAL, and exposes `execute_sql` (or equivalent) internally.
2. **`EngineHandle`** is implemented **for** a type wrapping `Database`, or `Database` implements the trait directly once the API is stable.
3. The **network server** holds `Arc<dyn EngineHandle>` (or a generic parameter) and does not depend on QUIC types inside the engine.

```mermaid
flowchart LR
  NET[Network_server]
  EH[EngineHandle_trait]
  DB[Database]
  NET --> EH
  EH --> DB
```

## Threading and async

- quinn is **async** (`tokio` or `async-std` depending on project choice). The engine handle should be **`Send + Sync`** if work is dispatched to blocking threads or if `Database` is behind `Arc<Mutex<...>>`; alternatively use a single-threaded engine with a channel—document the choice when implementing.

## Errors across the boundary

| Source | Example | Mapped to |
|--------|---------|-----------|
| Frame decode | Truncated header | Protocol `Error` frame; may reset stream |
| Engine | Syntax error | `Error` frame with SQL error code |
| Engine | Internal failure | `Error` frame; log server-side; avoid leaking paths |

## Testing strategy

- **Unit tests:** frame roundtrip (encode/decode) without UDP.
- **Integration tests:** in-memory or loopback QUIC (localhost) with a stub `EngineHandle` returning fixed rows.
- Engine tests remain independent of QUIC in `executor` / `storage` crates.

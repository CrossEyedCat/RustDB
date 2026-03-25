# Application framing protocol

This document specifies the **byte layout** of messages on top of QUIC streams. QUIC provides reliable ordered bytes per stream; RustDB adds **frames** so both peers can parse messages without ambiguity.

## Design principles

- **Length-prefixed frames:** each frame is self-contained and easy to parse without scanning for delimiters.
- **Serde for payloads:** Rust types derive `Serialize` / `Deserialize`; the wire format is defined by the serde backend (see below).
- **Explicit versioning:** clients and servers reject incompatible protocol versions early.

## Frame layout (logical)

Every frame has a **fixed-size header** followed by a **variable payload**.

| Field | Size | Description |
|-------|------|-------------|
| Magic | 4 bytes | ASCII **`RDB1`** (0x52 0x44 0x42 0x31) — identifies RustDB application frames (distinct from QUIC packet headers). |
| Protocol version | `u16` LE | Major wire version; bump when header or semantics change incompatibly. Start at **1**. |
| Message kind | `u16` LE | Discriminant for the application message enum (query, result chunk, error, etc.). |
| Payload length | `u32` LE | Byte length of the following payload **only** (not including the header). |
| Payload | N bytes | Serde-encoded body for this `Message kind`. |

Total fixed header size: **12 bytes** (4 + 2 + 2 + 4).

Endianness: **little-endian** for all multi-byte integers for consistency with common Rust conventions on little-endian hosts.

## Serde format

- **Recommended:** **[postcard](https://crates.io/crates/postcard)** — compact, no `std::fmt` dependency for floats in the same way as JSON, suitable for embedded-style protocols.
- **Alternative:** **bincode** with a fixed configuration — document the exact `Options` if used (endianness, int encoding).

The project uses **postcard** for v1 frame payloads (see `Cargo.toml` and [`network::framing`](../../src/network/framing/mod.rs)).

## Message kinds (v1)

The `u16` message kind in the header is the stable wire discriminant. Values **1–6** are defined; any other value is a **protocol error** (`unknown message kind`).

| `u16` | Kind | Direction | Purpose |
|-------|------|-----------|---------|
| `1` | `Query` | C → S | SQL text (`QueryPayload`). |
| `2` | `ResultSet` | S → C | Column names and row batches (`ResultSetPayload`). |
| `3` | `ExecutionOk` | S → C | Statement completed with no row set (`ExecutionOkPayload`). |
| `4` | `Error` | S → C | Stable error code + UTF-8 message (`ErrorPayload`). |
| `5` | `ClientHello` | C → S | Optional client/version probe (`ClientHelloPayload`). |
| `6` | `ServerReady` | S → C | Server ready / version string (`ServerReadyPayload`). |

The fixed **12-byte header** is followed by a **postcard** body for the payload only (the header carries the discriminant; bodies are not a second outer enum on the wire).

## Errors in-band

- **Protocol errors:** malformed frames, unknown version, unknown message kind → close stream or connection with an application `Error` frame where possible, then QUIC reset if unrecoverable.
- **SQL / engine errors:** returned as a normal **`Error`** frame with a stable **error code** (`u32` or enum discriminant) and human-readable message.

## Versioning rules

- **Patch-level** fixes: same `Protocol version`, same serde layout.
- **Breaking** changes: increment `Protocol version`; old clients must receive a clear failure (e.g. refuse handshake or send `unsupported version`).

## Rust types (implementation)

Types and encode/decode live in **`src/network/framing/`**:

- **`FrameHeader`** — magic `RDB1`, `protocol_version`, `message_kind`, `payload_len` (see `header.rs`).
- **`MessageKind`** — maps wire `u16` values **1–6**; unknown kinds are rejected on decode.
- **`ClientMessage`** / **`ServerMessage`** — logical enums; postcard serializes **only the inner payload** for the kind in the header.
- **`encode_*` / `decode_*`** — build or parse a full frame (header + postcard bytes); see `codec.rs`.

## Relationship to QUIC

- Frames are **opaque bytes** to QUIC: send with `write_all` on a `SendStream`, read with `read_exact` for the header then payload on `RecvStream`.
- **Variant A** ([stream-models.md](stream-models.md)): typically one request sequence and one response sequence per stream (possibly multiple frames each way).
- **Variant B:** same frame format on a single long-lived stream.

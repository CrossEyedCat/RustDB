# Network layer (QUIC)

This folder describes the intended **client/server network model** for RustDB: transport over **QUIC** using **[quinn](https://github.com/quinn-rs/quinn)**, an application-level **length-prefixed binary framing** protocol serialized with **serde**, and the **boundary** between the network layer and the database engine.

Implementation in `src/network/` is still a stub; these documents are the specification to build against.

## Contents

| Document | Description |
|----------|-------------|
| [architecture.md](architecture.md) | Goals, scope, and layering (TLS/QUIC → framing → session/query → engine). |
| [quic-and-quinn.md](quic-and-quinn.md) | Why QUIC, quinn’s role, ALPN, certificates, timeouts and connection limits. |
| [stream-models.md](stream-models.md) | Two candidate QUIC stream topologies (multi-stream vs single REPL stream); open decision. |
| [framing.md](framing.md) | Application frame format: magic, version, length, message kind, serde payload. |
| [engine-boundary.md](engine-boundary.md) | Contract between the network server and `Database` / query execution. |
| [diagrams.md](diagrams.md) | Mermaid sequence and topology diagrams. |
| [implementation-checklist.md](implementation-checklist.md) | Phased checklist for implementing QUIC, framing, and engine integration. |

## Quick summary

- **Transport:** QUIC (encrypted by design, TLS 1.3, stream multiplexing).
- **Library:** `quinn` for endpoints, connections, and streams.
- **Application protocol:** discrete **frames** on byte streams; each frame has a header and a serde-encoded body (see [framing.md](framing.md)). **postcard** is the recommended serde format for a compact, stable on-wire representation (final choice documented in [framing.md](framing.md)).
- **Engine:** the server delegates SQL execution to an **engine handle** trait; the network layer does not parse SQL (see [engine-boundary.md](engine-boundary.md)).

## Non-goals (for this protocol)

- PostgreSQL wire protocol over TCP (different transport and message set).
- JDBC/ODBC compatibility (would require a separate gateway or a different protocol).

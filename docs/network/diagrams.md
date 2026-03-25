# Diagrams

Mermaid diagrams for the QUIC network layer. GitHub and many Markdown viewers render these inline.

## Sequence: connect, query, response

Typical flow after stream model is chosen (one bidirectional stream shown for simplicity).

```mermaid
sequenceDiagram
  participant Client
  participant QUIC as QUIC_TLS
  participant Server
  participant Codec as Frame_codec
  participant Engine as EngineHandle

  Client->>QUIC: connect_and_TLS_handshake
  QUIC->>Server: accept_connection
  Client->>Server: open_bidirectional_stream
  Client->>Codec: write_Query_frame
  Codec->>Server: decoded_SQL
  Server->>Engine: execute_sql
  Engine-->>Server: EngineOutput_or_Error
  Server->>Codec: encode_Result_or_Error_frame
  Codec-->>Client: read_response_frames
  Client->>Server: close_stream_or_connection
```

## Topology: Variant A (multi-stream)

```mermaid
flowchart TB
  subgraph clientSide [Client]
    CC[QUIC_connection]
    S1[Stream_query1]
    S2[Stream_query2]
    CC --> S1
    CC --> S2
  end
  subgraph serverSide [Server]
    EP[Endpoint]
    T1[Task_stream1]
    T2[Task_stream2]
    EP --> T1
    EP --> T2
  end
  S1 <--> T1
  S2 <--> T2
```

## Topology: Variant B (single REPL stream)

```mermaid
flowchart LR
  subgraph clientSide [Client]
    CC[QUIC_connection]
    RS[Single_bidi_stream]
    CC --> RS
  end
  subgraph serverSide [Server]
    EP[Endpoint]
    LOOP[Sequential_read_write_loop]
    EP --> LOOP
  end
  RS <--> LOOP
```

## Session-oriented connection state (conceptual)

Optional state machine for a **logical** session; actual storage may live in `Connection` or task-local data.

```mermaid
stateDiagram-v2
  [*] --> Idle: QUIC_accepted
  Idle --> Active: first_frame_received
  Active --> Active: query_processed
  Active --> Idle: optional_reset_session
  Active --> Closed: error_or_shutdown
  Idle --> Closed: idle_timeout
  Closed --> [*]
```

## Layer stack (compact)

```mermaid
flowchart TB
  L1[QUIC_and_TLS]
  L2[quinn_streams]
  L3[Length_prefixed_frames]
  L4[Serde_messages]
  L5[EngineHandle]
  L1 --> L2 --> L3 --> L4 --> L5
```

See also [architecture.md](architecture.md) for the full layered description.

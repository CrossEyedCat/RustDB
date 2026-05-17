//! Encode and decode full frames: 12-byte header + postcard payload.

use std::io::Write;

use super::error::FrameDirection;
use super::header::{FrameHeader, FRAME_HEADER_LEN, MAX_FRAME_PAYLOAD_BYTES, PROTOCOL_VERSION_V1};
use super::messages::{
    ClientHelloPayload, ClientMessage, ErrorPayload, ExecuteScriptPayload, ExecuteTpccPayload,
    ExecutionOkPayload, MessageKind, QueryPayload, ResultSetPayload, ServerMessage,
    ServerReadyPayload,
};
use super::{EncodeError, ProtocolError};

/// Wire discriminant for native TPC-C `order_status` ([`crate::tpcc_workload::TxnKind::OrderStatus`]).
pub const TPCC_WIRE_KIND_ORDER_STATUS: u8 = 2;

/// Cached server frames for `ExecutionOk { rows_affected: n }` when `n <= 8` (hot microbench path).
static EXECUTION_OK_WIRE_CACHE_LE8: std::sync::LazyLock<
    std::sync::RwLock<[Option<std::sync::Arc<[u8]>>; 9]>,
> = std::sync::LazyLock::new(|| {
    let mut slots = std::array::from_fn(|_| None);
    for n in 0u64..=8 {
        if let Ok(bytes) = encode_execution_ok_frame(PROTOCOL_VERSION_V1, n) {
            slots[n as usize] = Some(std::sync::Arc::from(bytes.into_boxed_slice()));
        }
    }
    std::sync::RwLock::new(slots)
});

fn check_payload_len(len: usize) -> Result<(), EncodeError> {
    let max = MAX_FRAME_PAYLOAD_BYTES as usize;
    if len > max {
        return Err(EncodeError::PayloadTooLarge { len, max });
    }
    Ok(())
}

fn payload_slice<'a>(frame: &'a [u8], header: &FrameHeader) -> Result<&'a [u8], ProtocolError> {
    if header.payload_len > MAX_FRAME_PAYLOAD_BYTES {
        return Err(ProtocolError::PayloadTooLarge {
            len: header.payload_len,
            max: MAX_FRAME_PAYLOAD_BYTES,
        });
    }
    let need = FRAME_HEADER_LEN
        .checked_add(header.payload_len as usize)
        .ok_or(ProtocolError::PayloadTooLarge {
            len: header.payload_len,
            max: MAX_FRAME_PAYLOAD_BYTES,
        })?;
    if frame.len() < need {
        return Err(ProtocolError::TruncatedFrame {
            need,
            got: frame.len(),
        });
    }
    Ok(&frame[FRAME_HEADER_LEN..need])
}

/// Serialize a client message to a full frame (header + postcard body).
pub fn encode_client_message(
    protocol_version: u16,
    msg: &ClientMessage,
) -> Result<Vec<u8>, EncodeError> {
    let mut v = Vec::new();
    encode_client_message_write(protocol_version, msg, &mut v)?;
    Ok(v)
}

/// Write a client frame to `w` (typically a `Vec<u8>` or `TcpStream` in tests).
pub fn encode_client_message_write<W: Write>(
    protocol_version: u16,
    msg: &ClientMessage,
    w: &mut W,
) -> Result<(), EncodeError> {
    let (kind, payload_bytes): (MessageKind, Vec<u8>) = match msg {
        ClientMessage::Query(p) => (
            MessageKind::Query,
            postcard::to_allocvec(p).map_err(EncodeError::Postcard)?,
        ),
        ClientMessage::ClientHello(p) => (
            MessageKind::ClientHello,
            postcard::to_allocvec(p).map_err(EncodeError::Postcard)?,
        ),
        ClientMessage::ExecuteScript(p) => (
            MessageKind::ExecuteScript,
            postcard::to_allocvec(p).map_err(EncodeError::Postcard)?,
        ),
        ClientMessage::ExecuteTpcc(p) => (
            MessageKind::ExecuteTpcc,
            postcard::to_allocvec(p).map_err(EncodeError::Postcard)?,
        ),
    };
    check_payload_len(payload_bytes.len())?;
    let header = FrameHeader {
        protocol_version,
        message_kind: kind.as_u16(),
        payload_len: payload_bytes.len() as u32,
    };
    let mut hdr = [0u8; FRAME_HEADER_LEN];
    header.encode_into(&mut hdr);
    w.write_all(&hdr)?;
    w.write_all(&payload_bytes)?;
    Ok(())
}

/// Serialize a server message to a full frame.
pub fn encode_server_message(
    protocol_version: u16,
    msg: &ServerMessage,
) -> Result<Vec<u8>, EncodeError> {
    let mut v = Vec::new();
    encode_server_message_write(protocol_version, msg, &mut v)?;
    Ok(v)
}

/// Write a server frame to `w`.
pub fn encode_server_message_write<W: Write>(
    protocol_version: u16,
    msg: &ServerMessage,
    w: &mut W,
) -> Result<(), EncodeError> {
    let (kind, payload_bytes): (MessageKind, Vec<u8>) = match msg {
        ServerMessage::ResultSet(p) => (
            MessageKind::ResultSet,
            postcard::to_allocvec(p).map_err(EncodeError::Postcard)?,
        ),
        ServerMessage::ExecutionOk(p) => (
            MessageKind::ExecutionOk,
            postcard::to_allocvec(p).map_err(EncodeError::Postcard)?,
        ),
        ServerMessage::Error(p) => (
            MessageKind::Error,
            postcard::to_allocvec(p).map_err(EncodeError::Postcard)?,
        ),
        ServerMessage::ServerReady(p) => (
            MessageKind::ServerReady,
            postcard::to_allocvec(p).map_err(EncodeError::Postcard)?,
        ),
    };
    check_payload_len(payload_bytes.len())?;
    let header = FrameHeader {
        protocol_version,
        message_kind: kind.as_u16(),
        payload_len: payload_bytes.len() as u32,
    };
    let mut hdr = [0u8; FRAME_HEADER_LEN];
    header.encode_into(&mut hdr);
    w.write_all(&hdr)?;
    w.write_all(&payload_bytes)?;
    Ok(())
}

/// Decode a client-originated frame. `expected_protocol_version` must match the header field.
pub fn decode_client_frame(
    expected_protocol_version: u16,
    frame: &[u8],
) -> Result<ClientMessage, ProtocolError> {
    let header = FrameHeader::decode(frame)?;
    if header.protocol_version != expected_protocol_version {
        return Err(ProtocolError::UnsupportedVersion {
            expected: expected_protocol_version,
            got: header.protocol_version,
        });
    }
    let kind = MessageKind::try_from(header.message_kind)
        .map_err(|_| ProtocolError::UnknownMessageKind(header.message_kind))?;
    match kind {
        MessageKind::Query
        | MessageKind::ClientHello
        | MessageKind::ExecuteScript
        | MessageKind::ExecuteTpcc => {}
        _ => {
            return Err(ProtocolError::WrongDirection {
                kind,
                direction: FrameDirection::Client,
            });
        }
    }
    let body = payload_slice(frame, &header)?;
    match kind {
        MessageKind::Query => {
            let p: QueryPayload = postcard::from_bytes(body)
                .map_err(|e| ProtocolError::PostcardDecode(format!("{e:?}")))?;
            Ok(ClientMessage::Query(p))
        }
        MessageKind::ClientHello => {
            let p: ClientHelloPayload = postcard::from_bytes(body)
                .map_err(|e| ProtocolError::PostcardDecode(format!("{e:?}")))?;
            Ok(ClientMessage::ClientHello(p))
        }
        MessageKind::ExecuteScript => {
            let p: ExecuteScriptPayload = postcard::from_bytes(body)
                .map_err(|e| ProtocolError::PostcardDecode(format!("{e:?}")))?;
            Ok(ClientMessage::ExecuteScript(p))
        }
        MessageKind::ExecuteTpcc => {
            let p: ExecuteTpccPayload = postcard::from_bytes(body)
                .map_err(|e| ProtocolError::PostcardDecode(format!("{e:?}")))?;
            Ok(ClientMessage::ExecuteTpcc(p))
        }
        _ => unreachable!(),
    }
}

/// Decode a server-originated frame.
pub fn decode_server_frame(
    expected_protocol_version: u16,
    frame: &[u8],
) -> Result<ServerMessage, ProtocolError> {
    let header = FrameHeader::decode(frame)?;
    if header.protocol_version != expected_protocol_version {
        return Err(ProtocolError::UnsupportedVersion {
            expected: expected_protocol_version,
            got: header.protocol_version,
        });
    }
    let kind = MessageKind::try_from(header.message_kind)
        .map_err(|_| ProtocolError::UnknownMessageKind(header.message_kind))?;
    match kind {
        MessageKind::ResultSet
        | MessageKind::ExecutionOk
        | MessageKind::Error
        | MessageKind::ServerReady => {}
        _ => {
            return Err(ProtocolError::WrongDirection {
                kind,
                direction: FrameDirection::Server,
            });
        }
    }
    let body = payload_slice(frame, &header)?;
    match kind {
        MessageKind::ResultSet => {
            let p: ResultSetPayload = postcard::from_bytes(body)
                .map_err(|e| ProtocolError::PostcardDecode(format!("{e:?}")))?;
            Ok(ServerMessage::ResultSet(p))
        }
        MessageKind::ExecutionOk => {
            let p: ExecutionOkPayload = postcard::from_bytes(body)
                .map_err(|e| ProtocolError::PostcardDecode(format!("{e:?}")))?;
            Ok(ServerMessage::ExecutionOk(p))
        }
        MessageKind::Error => {
            let p: ErrorPayload = postcard::from_bytes(body)
                .map_err(|e| ProtocolError::PostcardDecode(format!("{e:?}")))?;
            Ok(ServerMessage::Error(p))
        }
        MessageKind::ServerReady => {
            let p: ServerReadyPayload = postcard::from_bytes(body)
                .map_err(|e| ProtocolError::PostcardDecode(format!("{e:?}")))?;
            Ok(ServerMessage::ServerReady(p))
        }
        _ => unreachable!(),
    }
}

/// Default protocol version for new encode/decode helpers in tests and future wire code.
pub fn encode_client_message_v1(msg: &ClientMessage) -> Result<Vec<u8>, EncodeError> {
    encode_client_message(PROTOCOL_VERSION_V1, msg)
}

pub fn encode_server_message_v1(msg: &ServerMessage) -> Result<Vec<u8>, EncodeError> {
    encode_server_message(PROTOCOL_VERSION_V1, msg)
}

pub fn decode_client_frame_v1(frame: &[u8]) -> Result<ClientMessage, ProtocolError> {
    decode_client_frame(PROTOCOL_VERSION_V1, frame)
}

pub fn decode_server_frame_v1(frame: &[u8]) -> Result<ServerMessage, ProtocolError> {
    decode_server_frame(PROTOCOL_VERSION_V1, frame)
}

/// Read the server [`MessageKind`] from a full frame without deserializing the payload.
pub fn server_frame_message_kind(frame: &[u8]) -> Result<MessageKind, ProtocolError> {
    let header = FrameHeader::decode(frame)?;
    MessageKind::try_from(header.message_kind)
        .map_err(|_| ProtocolError::UnknownMessageKind(header.message_kind))
}

/// Classify a server frame for the TPC-C client fast path (skips `ExecutionOk` body decode).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ServerFrameClass {
    ExecutionOk,
    Error(ErrorPayload),
    Other(ServerMessage),
}

pub fn classify_server_frame_v1(frame: &[u8]) -> Result<ServerFrameClass, ProtocolError> {
    let header = FrameHeader::decode(frame)?;
    let kind = MessageKind::try_from(header.message_kind)
        .map_err(|_| ProtocolError::UnknownMessageKind(header.message_kind))?;
    match kind {
        MessageKind::ExecutionOk => Ok(ServerFrameClass::ExecutionOk),
        MessageKind::Error => {
            let body = payload_slice(frame, &header)?;
            let p: ErrorPayload = postcard::from_bytes(body)
                .map_err(|e| ProtocolError::PostcardDecode(format!("{e:?}")))?;
            Ok(ServerFrameClass::Error(p))
        }
        _ => Ok(ServerFrameClass::Other(decode_server_frame_v1(frame)?)),
    }
}

/// Encode `ExecuteTpcc` into a single frame buffer (no intermediate payload `Vec`).
pub fn encode_execute_tpcc_frame_write<W: Write>(
    protocol_version: u16,
    p: &ExecuteTpccPayload,
    w: &mut W,
) -> Result<(), EncodeError> {
    let payload_bytes = postcard::to_allocvec(p).map_err(EncodeError::Postcard)?;
    check_payload_len(payload_bytes.len())?;
    let header = FrameHeader {
        protocol_version,
        message_kind: MessageKind::ExecuteTpcc.as_u16(),
        payload_len: payload_bytes.len() as u32,
    };
    let mut hdr = [0u8; FRAME_HEADER_LEN];
    header.encode_into(&mut hdr);
    w.write_all(&hdr)?;
    w.write_all(&payload_bytes)?;
    Ok(())
}

pub fn encode_execute_tpcc_frame_v1(p: &ExecuteTpccPayload) -> Result<Vec<u8>, EncodeError> {
    let mut v = Vec::new();
    encode_execute_tpcc_frame_write(PROTOCOL_VERSION_V1, p, &mut v)?;
    Ok(v)
}

/// Encode `ExecutionOk { rows_affected }` as a full wire frame.
pub fn encode_execution_ok_frame(
    protocol_version: u16,
    rows_affected: u64,
) -> Result<Vec<u8>, EncodeError> {
    let mut v = Vec::new();
    encode_execution_ok_frame_write(protocol_version, rows_affected, &mut v)?;
    Ok(v)
}

/// Write `ExecutionOk { rows_affected }` as a full wire frame into `w`.
pub fn encode_execution_ok_frame_write<W: Write>(
    protocol_version: u16,
    rows_affected: u64,
    w: &mut W,
) -> Result<(), EncodeError> {
    let payload_bytes = postcard::to_allocvec(&ExecutionOkPayload { rows_affected })
        .map_err(EncodeError::Postcard)?;
    check_payload_len(payload_bytes.len())?;
    let header = FrameHeader {
        protocol_version,
        message_kind: MessageKind::ExecutionOk.as_u16(),
        payload_len: payload_bytes.len() as u32,
    };
    let mut hdr = [0u8; FRAME_HEADER_LEN];
    header.encode_into(&mut hdr);
    w.write_all(&hdr)?;
    w.write_all(&payload_bytes)?;
    Ok(())
}

/// Pre-encoded `ExecutionOk` for small `rows_affected` values (order_status microbench).
pub fn cached_execution_ok_frame_v1(rows_affected: u64) -> Option<std::sync::Arc<[u8]>> {
    if rows_affected > 8 {
        return None;
    }
    let cache = EXECUTION_OK_WIRE_CACHE_LE8.read().ok()?;
    cache
        .get(rows_affected as usize)?
        .as_ref()
        .map(std::sync::Arc::clone)
}

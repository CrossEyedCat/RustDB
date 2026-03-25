//! Encode and decode full frames: 12-byte header + postcard payload.

use std::io::Write;

use super::error::FrameDirection;
use super::header::{FrameHeader, FRAME_HEADER_LEN, MAX_FRAME_PAYLOAD_BYTES, PROTOCOL_VERSION_V1};
use super::messages::{
    ClientHelloPayload, ClientMessage, ErrorPayload, ExecutionOkPayload, MessageKind, QueryPayload,
    ResultSetPayload, ServerMessage, ServerReadyPayload,
};
use super::{EncodeError, ProtocolError};

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
        MessageKind::Query | MessageKind::ClientHello => {}
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

//! Tests for `network::framing` (Phase 1 — no QUIC).

use std::io::Write;

use crate::network::framing::{
    decode_client_frame_v1, decode_server_frame_v1, encode_client_message_v1,
    encode_client_message_write, encode_server_message_v1, ClientHelloPayload, ClientMessage,
    EncodeError, ErrorPayload, ExecutionOkPayload, FrameDirection, FrameHeader, MessageKind,
    ProtocolError, QueryPayload, ResultSetPayload, ServerMessage, ServerReadyPayload,
    FRAME_HEADER_LEN, FRAME_MAGIC, MAX_FRAME_PAYLOAD_BYTES, PROTOCOL_VERSION_V1,
};

#[test]
fn roundtrip_client_query() {
    let msg = ClientMessage::Query(QueryPayload {
        sql: "SELECT 1".into(),
    });
    let wire = encode_client_message_v1(&msg).unwrap();
    let out = decode_client_frame_v1(&wire).unwrap();
    assert_eq!(out, msg);
}

#[test]
fn roundtrip_client_hello() {
    let msg = ClientMessage::ClientHello(ClientHelloPayload {
        client_version: "0.1.0".into(),
    });
    let wire = encode_client_message_v1(&msg).unwrap();
    let out = decode_client_frame_v1(&wire).unwrap();
    assert_eq!(out, msg);
}

#[test]
fn roundtrip_server_all_variants() {
    let cases = vec![
        ServerMessage::ResultSet(ResultSetPayload {
            columns: vec!["a".into()],
            rows: vec![vec!["1".into()]],
        }),
        ServerMessage::ExecutionOk(ExecutionOkPayload { rows_affected: 42 }),
        ServerMessage::Error(ErrorPayload {
            code: 7,
            message: "oops".into(),
        }),
        ServerMessage::ServerReady(ServerReadyPayload {
            server_version: "0.1.0".into(),
        }),
    ];
    for msg in cases {
        let wire = encode_server_message_v1(&msg).unwrap();
        let out = decode_server_frame_v1(&wire).unwrap();
        assert_eq!(out, msg);
    }
}

#[test]
fn wrong_magic() {
    let mut wire =
        encode_client_message_v1(&ClientMessage::Query(QueryPayload { sql: "x".into() })).unwrap();
    wire[0] = b'X';
    let err = decode_client_frame_v1(&wire).unwrap_err();
    assert_eq!(err, ProtocolError::BadMagic);
}

#[test]
fn version_mismatch() {
    let msg = ClientMessage::Query(QueryPayload {
        sql: "SELECT 1".into(),
    });
    let wire = encode_client_message_v1(&msg).unwrap();
    let err = crate::network::framing::decode_client_frame(999, &wire).unwrap_err();
    assert_eq!(
        err,
        ProtocolError::UnsupportedVersion {
            expected: 999,
            got: PROTOCOL_VERSION_V1
        }
    );
}

#[test]
fn truncated_header() {
    let err = FrameHeader::decode(&[0u8; 4]).unwrap_err();
    assert_eq!(
        err,
        ProtocolError::TruncatedHeader {
            expected: FRAME_HEADER_LEN,
            got: 4
        }
    );
}

#[test]
fn truncated_payload() {
    let mut wire =
        encode_client_message_v1(&ClientMessage::Query(QueryPayload { sql: "hi".into() })).unwrap();
    wire.truncate(wire.len().saturating_sub(1));
    let err = decode_client_frame_v1(&wire).unwrap_err();
    assert!(matches!(err, ProtocolError::TruncatedFrame { .. }));
}

#[test]
fn oversized_payload_declared_in_header() {
    let mut buf = vec![0u8; FRAME_HEADER_LEN + 4];
    buf[..4].copy_from_slice(&FRAME_MAGIC);
    buf[4..6].copy_from_slice(&PROTOCOL_VERSION_V1.to_le_bytes());
    buf[6..8].copy_from_slice(&MessageKind::Query.as_u16().to_le_bytes());
    buf[8..12].copy_from_slice(&(MAX_FRAME_PAYLOAD_BYTES + 1).to_le_bytes());
    let err = decode_client_frame_v1(&buf).unwrap_err();
    assert!(matches!(err, ProtocolError::PayloadTooLarge { .. }));
}

#[test]
fn unknown_message_kind() {
    let mut buf = vec![0u8; FRAME_HEADER_LEN];
    buf[..4].copy_from_slice(&FRAME_MAGIC);
    buf[4..6].copy_from_slice(&PROTOCOL_VERSION_V1.to_le_bytes());
    buf[6..8].copy_from_slice(&9999u16.to_le_bytes());
    buf[8..12].copy_from_slice(&0u32.to_le_bytes());
    let err = decode_client_frame_v1(&buf).unwrap_err();
    assert_eq!(err, ProtocolError::UnknownMessageKind(9999));
}

#[test]
fn wrong_direction_client_decode_of_server_frame() {
    let wire = encode_server_message_v1(&ServerMessage::ExecutionOk(ExecutionOkPayload {
        rows_affected: 0,
    }))
    .unwrap();
    let err = decode_client_frame_v1(&wire).unwrap_err();
    assert!(matches!(
        err,
        ProtocolError::WrongDirection {
            kind: MessageKind::ExecutionOk,
            ..
        }
    ));
}

#[test]
fn framing_error_display_covers_error_types() {
    let e = EncodeError::PayloadTooLarge { len: 99, max: 1 };
    assert!(e.to_string().contains("99"));
    assert!(e.to_string().contains('1'));

    let e = EncodeError::Io(std::io::Error::other("disk"));
    assert!(e.to_string().to_lowercase().contains("i/o"));

    let postcard_err = postcard::from_bytes::<QueryPayload>(b"\xff").unwrap_err();
    let e = EncodeError::Postcard(postcard_err);
    assert!(!e.to_string().is_empty());

    assert_eq!(FrameDirection::Client.to_string(), "client");
    assert_eq!(FrameDirection::Server.to_string(), "server");

    let displays = [
        ProtocolError::TruncatedHeader {
            expected: 12,
            got: 1,
        }
        .to_string(),
        ProtocolError::BadMagic.to_string(),
        ProtocolError::UnsupportedVersion {
            expected: 1,
            got: 2,
        }
        .to_string(),
        ProtocolError::UnknownMessageKind(42).to_string(),
        ProtocolError::WrongDirection {
            kind: MessageKind::Query,
            direction: FrameDirection::Server,
        }
        .to_string(),
        ProtocolError::PayloadTooLarge { len: 3, max: 2 }.to_string(),
        ProtocolError::TruncatedFrame { need: 20, got: 10 }.to_string(),
        ProtocolError::PostcardDecode("bad".into()).to_string(),
    ];
    assert!(displays.iter().all(|s| !s.is_empty()));
}

#[test]
fn decode_postcard_error_maps_to_protocol_error() {
    let mut buf = vec![0u8; FRAME_HEADER_LEN + 1];
    buf[..4].copy_from_slice(&FRAME_MAGIC);
    buf[4..6].copy_from_slice(&PROTOCOL_VERSION_V1.to_le_bytes());
    buf[6..8].copy_from_slice(&MessageKind::Query.as_u16().to_le_bytes());
    buf[8..12].copy_from_slice(&1u32.to_le_bytes());
    buf[12] = 0xff;
    let err = decode_client_frame_v1(&buf).unwrap_err();
    assert!(matches!(err, ProtocolError::PostcardDecode(_)));
    assert!(!err.to_string().is_empty());
}

struct FailSecondWrite {
    first: bool,
}

impl Write for FailSecondWrite {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        if !self.first {
            self.first = true;
            Ok(buf.len())
        } else {
            Err(std::io::Error::other("intentional write failure"))
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[test]
fn encode_client_message_write_io_error() {
    let mut w = FailSecondWrite { first: false };
    let msg = ClientMessage::Query(QueryPayload { sql: "x".into() });
    let err = encode_client_message_write(PROTOCOL_VERSION_V1, &msg, &mut w).unwrap_err();
    assert!(matches!(err, EncodeError::Io(_)));
    assert!(!err.to_string().is_empty());
}

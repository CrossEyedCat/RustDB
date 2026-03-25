//! Application payloads and high-level `ClientMessage` / `ServerMessage` enums.

use serde::{Deserialize, Serialize};

/// Wire discriminant for [`crate::network::framing::FrameHeader::message_kind`].
///
/// Values are stable protocol identifiers; do not renumber—add new variants at the end.
#[repr(u16)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageKind {
    Query = 1,
    ResultSet = 2,
    ExecutionOk = 3,
    Error = 4,
    ClientHello = 5,
    ServerReady = 6,
}

impl MessageKind {
    pub const fn as_u16(self) -> u16 {
        self as u16
    }
}

impl TryFrom<u16> for MessageKind {
    type Error = ();

    /// `TryFrom::Error` is spelled explicitly because `MessageKind::Error` would make `Self::Error` ambiguous.
    fn try_from(value: u16) -> Result<MessageKind, ()> {
        match value {
            1 => Ok(MessageKind::Query),
            2 => Ok(MessageKind::ResultSet),
            3 => Ok(MessageKind::ExecutionOk),
            4 => Ok(MessageKind::Error),
            5 => Ok(MessageKind::ClientHello),
            6 => Ok(MessageKind::ServerReady),
            _ => Err(()),
        }
    }
}

// --- Client → server payloads ------------------------------------------------

/// SQL query and optional hints (v1 minimal: SQL text only).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryPayload {
    pub sql: String,
}

/// Optional capability probe (v1 minimal).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClientHelloPayload {
    pub client_version: String,
}

/// Messages sent from client to server.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClientMessage {
    Query(QueryPayload),
    ClientHello(ClientHelloPayload),
}

// --- Server → client payloads ------------------------------------------------

/// Tabular result (v1 minimal: string columns and rows).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResultSetPayload {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<String>>,
}

/// Non-query statement finished (DDL/DML without row set).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionOkPayload {
    pub rows_affected: u64,
}

/// Stable engine/protocol error carried in-band.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ErrorPayload {
    pub code: u32,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServerReadyPayload {
    pub server_version: String,
}

/// Messages sent from server to client.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ServerMessage {
    ResultSet(ResultSetPayload),
    ExecutionOk(ExecutionOkPayload),
    Error(ErrorPayload),
    ServerReady(ServerReadyPayload),
}

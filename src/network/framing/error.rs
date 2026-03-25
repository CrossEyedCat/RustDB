//! Framing encode/decode errors (application protocol on top of QUIC streams).

use thiserror::Error;

/// Failure while building a wire frame (header + postcard payload).
#[derive(Error, Debug)]
pub enum EncodeError {
    /// Postcard could not serialize the payload.
    #[error("postcard encode error: {0}")]
    Postcard(#[from] postcard::Error),

    /// I/O while writing a frame (e.g. `Write` impl).
    #[error("i/o error: {0}")]
    Io(#[from] std::io::Error),

    /// Payload would exceed [`super::MAX_FRAME_PAYLOAD_BYTES`].
    #[error("payload length {len} exceeds maximum {max}")]
    PayloadTooLarge { len: usize, max: usize },
}

/// Failure while parsing a full frame from bytes.
#[derive(Error, Debug, PartialEq, Eq)]
pub enum ProtocolError {
    /// Fewer than 12 bytes available for the header.
    #[error("truncated header: expected {expected} bytes, got {got}")]
    TruncatedHeader { expected: usize, got: usize },

    /// First four bytes were not `RDB1`.
    #[error("bad frame magic (expected RDB1)")]
    BadMagic,

    /// `protocol_version` in the header does not match the expected value for this peer.
    #[error("unsupported protocol version: expected {expected}, got {got}")]
    UnsupportedVersion { expected: u16, got: u16 },

    /// `message_kind` is not a known discriminant.
    #[error("unknown message kind: {0}")]
    UnknownMessageKind(u16),

    /// Kind is known but not valid for this direction (client vs server).
    #[error("message kind {kind:?} is not valid for {direction} frames")]
    WrongDirection {
        kind: super::messages::MessageKind,
        direction: FrameDirection,
    },

    /// Declared payload length exceeds the configured maximum.
    #[error("payload length {len} exceeds maximum {max}")]
    PayloadTooLarge { len: u32, max: u32 },

    /// Buffer ends before the declared payload (truncated frame body).
    #[error("truncated frame: need {need} bytes total, got {got}")]
    TruncatedFrame { need: usize, got: usize },

    /// Postcard could not deserialize the payload for this message kind.
    #[error("postcard decode error: {0}")]
    PostcardDecode(String),
}

/// Whether we are decoding a client-originated or server-originated frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameDirection {
    Client,
    Server,
}

impl std::fmt::Display for FrameDirection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FrameDirection::Client => write!(f, "client"),
            FrameDirection::Server => write!(f, "server"),
        }
    }
}

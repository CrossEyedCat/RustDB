//! Fixed 12-byte application frame header (see `docs/network/framing.md`).

/// ASCII magic identifying RustDB application frames on a QUIC byte stream.
pub const FRAME_MAGIC: [u8; 4] = *b"RDB1";

/// Current wire protocol version (bump on incompatible header or payload layout changes).
pub const PROTOCOL_VERSION_V1: u16 = 1;

/// Upper bound on **payload** size (excluding the 12-byte header). Enforced on encode and decode.
pub const MAX_FRAME_PAYLOAD_BYTES: u32 = 16 * 1024 * 1024;

/// Fixed header size: magic (4) + protocol_version (2) + message_kind (2) + payload_len (4).
pub const FRAME_HEADER_LEN: usize = 12;

/// Parsed header fields; `message_kind` is the raw `u16` wire value (see [`crate::network::framing::MessageKind`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FrameHeader {
    pub protocol_version: u16,
    pub message_kind: u16,
    pub payload_len: u32,
}

impl FrameHeader {
    /// Decode a header from the first 12 bytes of `buf`.
    pub fn decode(buf: &[u8]) -> Result<Self, super::error::ProtocolError> {
        if buf.len() < FRAME_HEADER_LEN {
            return Err(super::error::ProtocolError::TruncatedHeader {
                expected: FRAME_HEADER_LEN,
                got: buf.len(),
            });
        }
        if buf[..4] != FRAME_MAGIC {
            return Err(super::error::ProtocolError::BadMagic);
        }
        let protocol_version = u16::from_le_bytes([buf[4], buf[5]]);
        let message_kind = u16::from_le_bytes([buf[6], buf[7]]);
        let payload_len = u32::from_le_bytes([buf[8], buf[9], buf[10], buf[11]]);
        Ok(Self {
            protocol_version,
            message_kind,
            payload_len,
        })
    }

    /// Write this header to the first 12 bytes of `out`.
    pub fn encode_into(&self, out: &mut [u8; FRAME_HEADER_LEN]) {
        out[..4].copy_from_slice(&FRAME_MAGIC);
        out[4..6].copy_from_slice(&self.protocol_version.to_le_bytes());
        out[6..8].copy_from_slice(&self.message_kind.to_le_bytes());
        out[8..12].copy_from_slice(&self.payload_len.to_le_bytes());
    }
}

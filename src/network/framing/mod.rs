//! Application framing: fixed `RDB1` header + postcard-encoded payloads ([`FrameHeader`]).
//!
//! See `docs/network/framing.md`.

mod codec;
mod error;
mod header;
pub mod messages;

pub use codec::{
    decode_client_frame, decode_client_frame_v1, decode_server_frame, decode_server_frame_v1,
    encode_client_message, encode_client_message_v1, encode_client_message_write,
    encode_server_message, encode_server_message_v1, encode_server_message_write,
};
pub use error::{EncodeError, FrameDirection, ProtocolError};
pub use header::{
    FrameHeader, FRAME_HEADER_LEN, FRAME_MAGIC, MAX_FRAME_PAYLOAD_BYTES, PROTOCOL_VERSION_V1,
};
pub use messages::{
    ClientHelloPayload, ClientMessage, ErrorPayload, ExecutionOkPayload, MessageKind,
    QueryPayload, ResultSetPayload, ServerMessage, ServerReadyPayload,
};

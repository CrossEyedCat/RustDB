//! Application framing: fixed `RDB1` header + postcard-encoded payloads ([`FrameHeader`]).
//!
//! See `docs/network/framing.md`.

mod codec;
mod error;
mod header;
pub mod messages;

pub use codec::{
    cached_execution_ok_frame_v1, classify_server_frame_v1, decode_client_frame,
    decode_client_frame_v1, decode_server_frame, decode_server_frame_v1, encode_client_message,
    encode_client_message_v1, encode_client_message_write, encode_execute_tpcc_frame_v1,
    encode_execute_tpcc_frame_write, encode_execution_ok_frame, encode_execution_ok_frame_write,
    encode_server_message, encode_server_message_v1, encode_server_message_write,
    server_frame_message_kind, ServerFrameClass, TPCC_WIRE_KIND_ORDER_STATUS,
};
pub use error::{EncodeError, FrameDirection, ProtocolError};
pub use header::{
    FrameHeader, FRAME_HEADER_LEN, FRAME_MAGIC, MAX_FRAME_PAYLOAD_BYTES, PROTOCOL_VERSION_V1,
};
pub use messages::{
    ClientHelloPayload, ClientMessage, ErrorPayload, ExecuteScriptPayload, ExecuteTpccPayload,
    ExecutionOkPayload, MessageKind, QueryPayload, ResultSetPayload, ServerMessage,
    ServerReadyPayload,
};

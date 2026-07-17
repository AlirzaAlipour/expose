//! Shared protocol/types crate for the Expose tunnel service.

pub mod buffer;
pub mod error;
pub mod protocol;
pub mod types;
pub mod utils;

pub use buffer::IntoBytes;
pub use error::{ClientResult, ConfigError, EncodingError, ExposeError, Result, ServerResult};
pub use protocol::{
    decode_message, encode_message, major_version, minor_version, versions_compatible,
    ConnectRequest, ConnectResponse, ErrorCode, Message, VersionCheckResult, PROTOCOL_VERSION,
};
pub use types::{RequestLimits, TcpTuningConfig, TunnelAssignment, TunnelConfig, TunnelProtocol};

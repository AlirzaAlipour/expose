//! Binary protocol definitions shared between the client and server.

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::error::{EncodingError, Result};
use crate::types::{RequestLimits, TunnelProtocol};
use crate::utils;

/// Current protocol version (MAJOR * 100 + MINOR).
pub const PROTOCOL_VERSION: u16 = 200;

/// Extract the major protocol component.
pub const fn major_version(version: u16) -> u16 {
    version / 100
}

/// Extract the minor protocol component.
pub const fn minor_version(version: u16) -> u16 {
    version % 100
}

/// Returns true when both versions share the same major number.
pub fn versions_compatible(client_version: u16, server_version: u16) -> bool {
    major_version(client_version) == major_version(server_version)
}

/// Result describing the compatibility of two protocol versions.
#[derive(Debug, Clone)]
pub enum VersionCheckResult {
    /// Versions are compatible and may communicate safely.
    Compatible,
    /// Client is older than the minimum supported major version.
    ClientTooOld {
        client_version: u16,
        server_version: u16,
        min_supported: u16,
    },
    /// Client is newer than the server implementation.
    ClientTooNew {
        client_version: u16,
        server_version: u16,
    },
}

impl VersionCheckResult {
    /// Determine compatibility from client/server versions.
    pub fn check(client_version: u16, server_version: u16) -> Self {
        if versions_compatible(client_version, server_version) {
            Self::Compatible
        } else if client_version < server_version {
            Self::ClientTooOld {
                client_version,
                server_version,
                min_supported: major_version(server_version) * 100,
            }
        } else {
            Self::ClientTooNew {
                client_version,
                server_version,
            }
        }
    }

    /// Convenience helper for compatibility checks.
    pub fn is_compatible(&self) -> bool {
        matches!(self, Self::Compatible)
    }

    /// Optional human-readable explanation.
    pub fn error_message(&self) -> Option<String> {
        match self {
            Self::Compatible => None,
            Self::ClientTooOld {
                client_version,
                server_version,
                min_supported,
            } => Some(format!(
                "Client version {client_version} is incompatible with server version {server_version}. Upgrade the client to version {min_supported} or newer."
            )),
            Self::ClientTooNew {
                client_version,
                server_version,
            } => Some(format!(
                "Client version {client_version} is newer than server version {server_version}. Upgrade the server to a compatible build."
            )),
        }
    }
}

/// Hard limit for payload sizes unless overridden by the server.
pub const DEFAULT_MAX_BODY_SIZE: usize = 10 * 1024 * 1024;

/// Initial message sent by the client when establishing a tunnel.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ConnectRequest {
    /// Version of the client protocol implementation.
    pub protocol_version: u16,
    /// Optional API key for authentication.
    pub api_key: Option<String>,
    /// Requested subdomain.
    pub desired_subdomain: Option<String>,
    /// Requested tunnel protocol (HTTP/TCP).
    pub tunnel_protocol: TunnelProtocol,
    /// Client semantic version string.
    pub client_version: String,
    /// Arbitrary metadata for audit logs.
    pub metadata: Option<String>,
}

impl ConnectRequest {
    /// Convenience helper used by the CLI to prepare the struct.
    pub fn new(
        api_key: Option<String>,
        desired_subdomain: Option<String>,
        tunnel_protocol: TunnelProtocol,
        client_version: impl Into<String>,
    ) -> Self {
        Self {
            protocol_version: PROTOCOL_VERSION,
            api_key,
            desired_subdomain,
            tunnel_protocol,
            client_version: client_version.into(),
            metadata: None,
        }
    }
}

/// Server acknowledgement for a connect request.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ConnectResponse {
    /// Version of the protocol negotiated by the server.
    pub protocol_version: u16,
    /// Assigned tunnel identifier.
    pub tunnel_id: Uuid,
    /// Subdomain eventually exposed.
    pub assigned_subdomain: String,
    /// Primary domain of the server cluster.
    pub domain: String,
    /// Selected tunnel protocol.
    pub tunnel_protocol: TunnelProtocol,
    /// Optional informational message.
    pub message: Option<String>,
    /// Request limits enforced by the server.
    pub limits: RequestLimits,
    /// Scheme advertised for the public URL (http or https).
    pub public_scheme: String,
    /// Optional explicit public port (None = scheme default).
    pub public_port: Option<u16>,
    /// Fully qualified public URL for display/logging.
    pub public_url: String,
}

impl ConnectResponse {
    /// Build a response with correct scheme/port metadata.
    pub fn new(
        tunnel_id: Uuid,
        assigned_subdomain: String,
        domain: String,
        tunnel_protocol: TunnelProtocol,
        tls_enabled: bool,
        public_port: Option<u16>,
        limits: RequestLimits,
    ) -> Self {
        let public_scheme = match tunnel_protocol {
            TunnelProtocol::Http => {
                if tls_enabled {
                    "https"
                } else {
                    "http"
                }
            }
            TunnelProtocol::Tcp => "tcp",
        };

        let effective_port = public_port;

        let default_http_port = if tls_enabled { 443 } else { 80 };
        let url = match tunnel_protocol {
            TunnelProtocol::Http => {
                let include_port = match effective_port {
                    Some(port) => port != default_http_port,
                    None => false,
                };
                if include_port {
                    format!(
                        "{public_scheme}://{}.{}:{}",
                        assigned_subdomain,
                        domain,
                        effective_port.unwrap()
                    )
                } else {
                    format!("{public_scheme}://{}.{}", assigned_subdomain, domain)
                }
            }
            TunnelProtocol::Tcp => match effective_port {
                Some(port) => format!("tcp://{}.{}:{}", assigned_subdomain, domain, port),
                None => format!("tcp://{}.{}", assigned_subdomain, domain),
            },
        };

        let advertised_port = match tunnel_protocol {
            TunnelProtocol::Http => {
                if matches!(effective_port, Some(port) if port != default_http_port) {
                    effective_port
                } else {
                    None
                }
            }
            TunnelProtocol::Tcp => effective_port,
        };

        Self {
            protocol_version: PROTOCOL_VERSION,
            tunnel_id,
            assigned_subdomain,
            domain,
            tunnel_protocol,
            message: None,
            limits,
            public_scheme: public_scheme.to_string(),
            public_port: advertised_port,
            public_url: url,
        }
    }

    /// Public URL accessor for legacy call sites.
    pub fn public_url(&self) -> &str {
        &self.public_url
    }
}

/// Envelope for all messages exchanged across the tunnel.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum Message {
    /// Initiate a tunnel from client to server.
    Connect(ConnectRequest),
    /// Acknowledge a tunnel connection from server to client.
    ConnectAck(ConnectResponse),
    /// HTTP request proxied from the internet to the client.
    HttpRequest {
        /// Unique request identifier.
        id: Uuid,
        /// HTTP method string.
        method: String,
        /// Raw request path and query string.
        path: String,
        /// Request headers.
        headers: Vec<(String, String)>,
        /// Serialized HTTP body.
        body: Bytes,
    },
    /// Response returned by the client and proxied back to the requester.
    HttpResponse {
        /// Unique request identifier (matches the request).
        id: Uuid,
        /// HTTP status code.
        status: u16,
        /// Response headers.
        headers: Vec<(String, String)>,
        /// Response body.
        body: Bytes,
    },
    /// Streaming start for HTTP request bodies.
    HttpRequestStart {
        /// Unique request identifier.
        id: Uuid,
        /// HTTP method string.
        method: String,
        /// Raw request path and query string.
        path: String,
        /// Request headers.
        headers: Vec<(String, String)>,
        /// Optional content-length header.
        content_length: Option<u64>,
    },
    /// Streaming chunk for HTTP request bodies.
    HttpRequestChunk {
        /// Unique request identifier.
        id: Uuid,
        /// Payload chunk.
        data: Bytes,
        /// Sequence number used for ordering.
        sequence: u32,
    },
    /// Streaming end marker for HTTP requests.
    HttpRequestEnd {
        /// Unique request identifier.
        id: Uuid,
    },
    /// Streaming start for HTTP responses.
    HttpResponseStart {
        /// Unique request identifier.
        id: Uuid,
        /// HTTP status code.
        status: u16,
        /// Response headers.
        headers: Vec<(String, String)>,
        /// Optional content-length.
        content_length: Option<u64>,
    },
    /// Streaming chunk for HTTP responses.
    HttpResponseChunk {
        /// Unique request identifier.
        id: Uuid,
        /// Payload chunk.
        data: Bytes,
        /// Sequence number used for ordering.
        sequence: u32,
    },
    /// Streaming end marker for HTTP responses.
    HttpResponseEnd {
        /// Unique request identifier.
        id: Uuid,
    },
    /// Request to open a TCP stream through the tunnel.
    TcpConnect {
        /// Logical connection identifier.
        connection_id: Uuid,
        /// Remote address observed by the server.
        remote_addr: String,
    },
    /// Acknowledges whether the client opened the TCP connection locally.
    TcpConnectAck {
        /// Logical connection identifier.
        connection_id: Uuid,
        /// Indicates success or failure.
        success: bool,
        /// Optional error message when `success = false`.
        error: Option<String>,
    },
    /// Bidirectional TCP data frame.
    TcpData {
        /// Logical connection identifier.
        connection_id: Uuid,
        /// Payload data.
        data: Bytes,
        /// Sequence number used for diagnostics.
        sequence: u64,
    },
    /// Indicates that a TCP stream closed.
    TcpClose {
        /// Logical connection identifier.
        connection_id: Uuid,
        /// Reason the stream closed.
        reason: TcpCloseReason,
    },
    /// Signal a graceful shutdown of the tunnel.
    Disconnect {
        /// Optional reason string for observability.
        reason: Option<String>,
    },
    /// Report a protocol-level error.
    Error {
        /// Structured error code to aid clients.
        code: ErrorCode,
        /// Human readable error message.
        message: String,
    },
}

/// Error codes surfaced through [`Message::Error`].
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[repr(u16)]
pub enum ErrorCode {
    /// Authentication failed.
    AuthenticationFailed = 1001,
    /// Requested subdomain is unavailable.
    SubdomainUnavailable = 1002,
    /// Rate limit exceeded.
    RateLimitExceeded = 1003,
    /// Protocol versions differ.
    ProtocolMismatch = 1004,
    /// Internal server error.
    InternalError = 1099,
}

impl Message {
    /// Serialize message using the agreed upon binary format.
    pub fn encode(&self) -> Result<Vec<u8>> {
        self.validate(DEFAULT_MAX_BODY_SIZE)?;
        Ok(bincode::serialize(self).map_err(EncodingError::from)?)
    }

    /// Decode a message from the provided bytes.
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let msg: Message = bincode::deserialize(bytes).map_err(EncodingError::from)?;
        msg.validate(DEFAULT_MAX_BODY_SIZE)?;
        Ok(msg)
    }

    /// Validate message invariants (e.g. size limits).
    pub fn validate(&self, limit: usize) -> Result<()> {
        match self {
            Message::HttpRequest { body, .. } | Message::HttpResponse { body, .. } => {
                utils::validate_body_size(body.len(), limit)?;
                Ok(())
            }
            Message::HttpRequestChunk { data, .. } | Message::HttpResponseChunk { data, .. } => {
                utils::validate_body_size(data.len(), limit)?;
                Ok(())
            }
            Message::TcpData { data, .. } => {
                utils::validate_body_size(data.len(), limit)?;
                Ok(())
            }
            _ => Ok(()),
        }
    }
}

/// Convenience helper for streaming use-cases where encode/decode are called explicitly.
pub fn encode_message(msg: &Message) -> Result<Vec<u8>> {
    msg.encode()
}

/// Deserialize from bytes into the strongly typed [`Message`].
pub fn decode_message(bytes: &[u8]) -> Result<Message> {
    Message::decode(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn message_roundtrip() {
        let msg = Message::HttpRequest {
            id: Uuid::new_v4(),
            method: "GET".into(),
            path: "/".into(),
            headers: vec![("host".into(), "example".into())],
            body: Bytes::new(),
        };
        let buf = msg.encode().unwrap();
        let decoded = Message::decode(&buf).unwrap();
        assert_eq!(msg, decoded);
    }

    #[test]
    fn message_roundtrip_all_variants() {
        let id = Uuid::new_v4();
        let messages = vec![
            Message::Connect(ConnectRequest::new(None, None, TunnelProtocol::Http, "1.0")),
            Message::ConnectAck(ConnectResponse::new(
                id,
                "alpha".into(),
                "example.com".into(),
                TunnelProtocol::Http,
                true,
                None,
                RequestLimits::default(),
            )),
            Message::HttpRequest {
                id,
                method: "POST".into(),
                path: "/demo".into(),
                headers: vec![("host".into(), "example".into())],
                body: Bytes::from_static(b"ping"),
            },
            Message::HttpResponse {
                id,
                status: 200,
                headers: vec![("content-type".into(), "text/plain".into())],
                body: Bytes::from_static(b"pong"),
            },
            Message::HttpRequestStart {
                id,
                method: "POST".into(),
                path: "/stream".into(),
                headers: vec![("content-type".into(), "application/json".into())],
                content_length: Some(5),
            },
            Message::HttpRequestChunk {
                id,
                data: Bytes::from_static(b"hello"),
                sequence: 0,
            },
            Message::HttpRequestEnd { id },
            Message::HttpResponseStart {
                id,
                status: 201,
                headers: vec![("content-type".into(), "application/json".into())],
                content_length: None,
            },
            Message::HttpResponseChunk {
                id,
                data: Bytes::from_static(b"world"),
                sequence: 1,
            },
            Message::HttpResponseEnd { id },
            Message::TcpConnect {
                connection_id: id,
                remote_addr: "127.0.0.1:5000".into(),
            },
            Message::TcpConnectAck {
                connection_id: id,
                success: true,
                error: None,
            },
            Message::TcpData {
                connection_id: id,
                data: Bytes::from_static(b"tcp"),
                sequence: 42,
            },
            Message::TcpClose {
                connection_id: id,
                reason: TcpCloseReason::Normal,
            },
            Message::Disconnect {
                reason: Some("test".into()),
            },
            Message::Error {
                code: ErrorCode::InternalError,
                message: "boom".into(),
            },
        ];

        for message in messages {
            let buf = message.encode().expect("encode");
            let decoded = Message::decode(&buf).expect("decode");
            assert_eq!(message, decoded);
        }
    }

    #[test]
    fn test_connect_response_http_default_port() {
        let response = ConnectResponse::new(
            Uuid::new_v4(),
            "demo".into(),
            "example.com".into(),
            TunnelProtocol::Http,
            false,
            None,
            RequestLimits::default(),
        );

        assert_eq!(response.public_scheme, "http");
        assert_eq!(response.public_url, "http://demo.example.com");
    }

    #[test]
    fn test_connect_response_https_default_port() {
        let response = ConnectResponse::new(
            Uuid::new_v4(),
            "demo".into(),
            "example.com".into(),
            TunnelProtocol::Http,
            true,
            Some(443),
            RequestLimits::default(),
        );

        assert_eq!(response.public_scheme, "https");
        assert_eq!(response.public_url, "https://demo.example.com");
    }

    #[test]
    fn test_connect_response_custom_port() {
        let response = ConnectResponse::new(
            Uuid::new_v4(),
            "demo".into(),
            "example.com".into(),
            TunnelProtocol::Http,
            false,
            Some(8080),
            RequestLimits::default(),
        );

        assert_eq!(response.public_url, "http://demo.example.com:8080");
    }

    #[test]
    fn test_connect_response_https_443_no_port_in_url() {
        let response = ConnectResponse::new(
            Uuid::new_v4(),
            "demo".into(),
            "example.com".into(),
            TunnelProtocol::Http,
            true,
            Some(443),
            RequestLimits::default(),
        );

        assert_eq!(response.public_url, "https://demo.example.com");
    }
}
/// Reasons a TCP tunnel connection closed.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum TcpCloseReason {
    /// Normal remote close.
    Normal,
    /// The peer reset the connection.
    Reset,
    /// Connection timed out.
    Timeout,
    /// Error with message.
    Error(String),
}

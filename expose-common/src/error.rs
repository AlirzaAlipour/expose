//! Shared error definitions for the Expose workspace.

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde_json::json;
use thiserror::Error;

use crate::protocol::ErrorCode;

/// Result alias for fallible operations in shared libraries.
pub type Result<T> = std::result::Result<T, ExposeError>;
/// Result alias specialized for the server.
pub type ServerResult<T> = Result<T>;
/// Result alias specialized for the client.
pub type ClientResult<T> = Result<T>;

/// Root error type for every crate in the workspace.
#[derive(Debug, Error)]
pub enum ExposeError {
    // ==================== Protocol ====================
    /// Failed to encode or decode protocol frames.
    #[error("protocol encoding error: {0}")]
    Encoding(#[from] EncodingError),
    /// Protocol version mismatch between peers.
    #[error("protocol version mismatch: client={client_version}, server={server_version}")]
    VersionMismatch {
        client_version: u16,
        server_version: u16,
    },
    /// Invalid or unexpected protocol message encountered.
    #[error("invalid message: {context}")]
    InvalidMessage { context: String },

    // ==================== Authentication ====================
    /// API key validation failed.
    #[error("authentication failed: {reason}")]
    Authentication { reason: String },
    /// Admin API token validation failed.
    #[error("admin authorization failed")]
    AdminAuthorization,

    // ==================== Tunnel Errors ====================
    /// Requested subdomain is already taken.
    #[error("subdomain '{subdomain}' is already in use")]
    SubdomainTaken { subdomain: String },
    /// Sanitized subdomain does not satisfy requirements.
    #[error("invalid subdomain '{subdomain}': {reason}")]
    InvalidSubdomain { subdomain: String, reason: String },
    /// Requested tunnel not found.
    #[error("tunnel not found: {identifier}")]
    TunnelNotFound { identifier: String },
    /// Tunnel disconnected or closed unexpectedly.
    #[error("tunnel disconnected")]
    TunnelDisconnected { reason: Option<String> },

    // ==================== Capacity ====================
    /// Rate limit exceeded – client should back off.
    #[error("rate limit exceeded, retry after {retry_after_secs}s")]
    RateLimited { retry_after_secs: u64 },
    /// Resource capacity exceeded (e.g. max tunnels).
    #[error("capacity exceeded: {resource}")]
    CapacityExceeded { resource: String },
    /// Request payload too large.
    #[error("payload too large: {size} bytes exceeds limit of {limit} bytes")]
    PayloadTooLarge { size: usize, limit: usize },

    // ==================== Network ====================
    /// Generic network/IO failure.
    #[error("network error: {0}")]
    Network(#[from] std::io::Error),
    /// WebSocket specific failure.
    #[error("websocket error: {0}")]
    WebSocket(#[from] tokio_tungstenite::tungstenite::Error),
    /// Local HTTP proxy failed.
    #[error("HTTP error: {0}")]
    Http(#[from] hyper::Error),
    /// Operation timed out.
    #[error("timeout: {operation} took longer than {timeout_secs}s")]
    Timeout {
        operation: String,
        timeout_secs: u64,
    },
    /// Local upstream refused the connection.
    #[error("connection refused to {address}")]
    ConnectionRefused { address: String },

    // ==================== Configuration ====================
    /// Configuration parsing or validation error.
    #[error("configuration error: {0}")]
    Config(#[from] ConfigError),

    // ==================== Internal ====================
    /// Internal invariant violated.
    #[error("internal error: {0}")]
    Internal(String),
}

/// Encoding/decoding failures surfaced through [`ExposeError`].
#[derive(Debug, Error)]
pub enum EncodingError {
    #[error("bincode serialization failed: {0}")]
    Bincode(#[from] bincode::Error),
    #[error("invalid UTF-8 in message: {0}")]
    Utf8(#[from] std::str::Utf8Error),
    #[error("message too large: {size} bytes")]
    MessageTooLarge { size: usize },
}

/// Configuration related errors shared between server and client.
#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("failed to read config file: {0}")]
    FileRead(#[from] std::io::Error),
    #[error("failed to parse config: {0}")]
    Parse(#[from] toml::de::Error),
    #[error("validation error: {0}")]
    Validation(String),
    #[error("missing required field: {field}")]
    MissingField { field: String },
}

impl ExposeError {
    /// Convert error into an HTTP status code.
    pub fn http_status(&self) -> StatusCode {
        match self {
            Self::Authentication { .. } => StatusCode::UNAUTHORIZED,
            Self::AdminAuthorization => StatusCode::FORBIDDEN,
            Self::SubdomainTaken { .. } => StatusCode::CONFLICT,
            Self::InvalidSubdomain { .. } => StatusCode::BAD_REQUEST,
            Self::TunnelNotFound { .. } => StatusCode::NOT_FOUND,
            Self::RateLimited { .. } => StatusCode::TOO_MANY_REQUESTS,
            Self::CapacityExceeded { .. } => StatusCode::SERVICE_UNAVAILABLE,
            Self::PayloadTooLarge { .. } => StatusCode::PAYLOAD_TOO_LARGE,
            Self::Timeout { .. } => StatusCode::GATEWAY_TIMEOUT,
            Self::ConnectionRefused { .. } => StatusCode::BAD_GATEWAY,
            Self::VersionMismatch { .. } => StatusCode::BAD_REQUEST,
            Self::Config(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Self::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    /// Convert into protocol [`ErrorCode`] for `Message::Error` payloads.
    pub fn error_code(&self) -> ErrorCode {
        match self {
            Self::Authentication { .. } | Self::AdminAuthorization => {
                ErrorCode::AuthenticationFailed
            }
            Self::SubdomainTaken { .. } | Self::InvalidSubdomain { .. } => {
                ErrorCode::SubdomainUnavailable
            }
            Self::RateLimited { .. } => ErrorCode::RateLimitExceeded,
            Self::VersionMismatch { .. } => ErrorCode::ProtocolMismatch,
            _ => ErrorCode::InternalError,
        }
    }

    /// Indicates whether the caller may retry automatically.
    pub fn is_retriable(&self) -> bool {
        matches!(
            self,
            Self::Network(_)
                | Self::WebSocket(_)
                | Self::Timeout { .. }
                | Self::TunnelDisconnected { .. }
        )
    }
}

impl IntoResponse for ExposeError {
    fn into_response(self) -> Response {
        let status = self.http_status();
        let body = json!({
            "error": {
                "code": self.error_code() as u16,
                "message": self.to_string(),
            }
        });

        let mut response = Json(body).into_response();
        *response.status_mut() = status;
        response
    }
}

impl From<anyhow::Error> for ExposeError {
    fn from(value: anyhow::Error) -> Self {
        Self::Internal(value.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::StatusCode;
    use axum::response::IntoResponse;
    use serde_json::Value;

    #[test]
    fn http_status_matches_variants() {
        assert_eq!(
            ExposeError::Authentication {
                reason: "invalid".into(),
            }
            .http_status(),
            StatusCode::UNAUTHORIZED
        );

        assert_eq!(
            ExposeError::CapacityExceeded {
                resource: "tunnels".into(),
            }
            .http_status(),
            StatusCode::SERVICE_UNAVAILABLE
        );

        assert_eq!(
            ExposeError::Timeout {
                operation: "test".into(),
                timeout_secs: 1,
            }
            .http_status(),
            StatusCode::GATEWAY_TIMEOUT
        );
    }

    #[test]
    fn error_code_matches_variants() {
        assert_eq!(
            ExposeError::SubdomainTaken {
                subdomain: "demo".into(),
            }
            .error_code(),
            ErrorCode::SubdomainUnavailable
        );

        assert_eq!(
            ExposeError::RateLimited {
                retry_after_secs: 5,
            }
            .error_code(),
            ErrorCode::RateLimitExceeded
        );

        assert_eq!(
            ExposeError::Internal("boom".into()).error_code(),
            ErrorCode::InternalError
        );
    }

    #[test]
    fn retriable_flag_reflects_variant() {
        assert!(
            ExposeError::Network(std::io::Error::new(std::io::ErrorKind::Other, "io"))
                .is_retriable()
        );
        assert!(ExposeError::TunnelDisconnected { reason: None }.is_retriable());
        assert!(!ExposeError::Authentication {
            reason: "invalid".into()
        }
        .is_retriable());
    }

    #[test]
    fn into_response_sets_status_and_body() {
        let response = ExposeError::Authentication {
            reason: "denied".into(),
        }
        .into_response();

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

        let body_bytes =
            tokio_test::block_on(axum::body::to_bytes(response.into_body(), usize::MAX)).unwrap();
        let payload: Value = serde_json::from_slice(&body_bytes).unwrap();
        assert_eq!(
            payload["error"]["code"],
            ErrorCode::AuthenticationFailed as u16
        );
        assert!(payload["error"]["message"]
            .as_str()
            .unwrap()
            .contains("authentication failed"));
    }
}

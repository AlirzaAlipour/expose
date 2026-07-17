//! Shared strongly typed structures that describe tunnel metadata.

use serde::{Deserialize, Serialize};
use std::fmt;
use uuid::Uuid;

/// Supported tunnel protocols.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "lowercase")]
pub enum TunnelProtocol {
    /// HTTP proxy support.
    Http,
    /// Generic TCP tunneling (reserved for future use).
    Tcp,
}

impl Default for TunnelProtocol {
    fn default() -> Self {
        Self::Http
    }
}

impl fmt::Display for TunnelProtocol {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TunnelProtocol::Http => write!(f, "http"),
            TunnelProtocol::Tcp => write!(f, "tcp"),
        }
    }
}

/// Configuration required for establishing a tunnel from the client.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TunnelConfig {
    /// Selected protocol for the tunnel.
    pub protocol: TunnelProtocol,
    /// Local host that should receive forwarded requests.
    pub local_host: String,
    /// Local port for the upstream service.
    pub local_port: u16,
    /// Desired subdomain, if any.
    pub subdomain: Option<String>,
    /// WebSocket endpoint for the tunnel server.
    pub server_url: String,
    /// Optional API key for authenticating with the server.
    pub api_key: Option<String>,
    /// Maximum number of reconnect attempts (0 = infinite).
    pub reconnect_max_attempts: u32,
    /// Base delay in milliseconds for reconnect backoff calculations.
    pub reconnect_base_delay_ms: u64,
    /// TCP tuning configuration applied to tunnel connections.
    pub tcp_tuning: TcpTuningConfig,
}

impl TunnelConfig {
    /// Fully qualified local endpoint string (e.g. `localhost:8080`).
    pub fn local_endpoint(&self) -> String {
        format!("{}:{}", self.local_host, self.local_port)
    }
}

impl Default for TunnelConfig {
    fn default() -> Self {
        Self {
            protocol: TunnelProtocol::Http,
            local_host: "127.0.0.1".into(),
            local_port: 8080,
            subdomain: None,
            server_url: "wss://tunnel.example.com".into(),
            api_key: None,
            reconnect_max_attempts: 10,
            reconnect_base_delay_ms: 1_000,
            tcp_tuning: TcpTuningConfig::default(),
        }
    }
}

/// TCP tuning parameters for optimized tunnel performance.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct TcpTuningConfig {
    /// Disable Nagle's algorithm (TCP_NODELAY).
    pub nodelay: bool,
    /// Enable TCP keepalive probes.
    pub keepalive_enabled: bool,
    /// Keepalive time in seconds before sending the first probe.
    pub keepalive_time_secs: u64,
    /// Keepalive interval in seconds between probes.
    pub keepalive_interval_secs: u64,
    /// Optional send buffer size in bytes.
    pub send_buffer_size: Option<usize>,
    /// Optional receive buffer size in bytes.
    pub recv_buffer_size: Option<usize>,
}

impl Default for TcpTuningConfig {
    fn default() -> Self {
        Self {
            nodelay: true,
            keepalive_enabled: true,
            keepalive_time_secs: 60,
            keepalive_interval_secs: 10,
            send_buffer_size: Some(262_144),
            recv_buffer_size: Some(262_144),
        }
    }
}

/// Assignment returned from the server after a successful connect request.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TunnelAssignment {
    /// Unique identifier for the tunnel session.
    pub tunnel_id: Uuid,
    /// Subdomain allocated by the server.
    pub subdomain: String,
    /// Base domain served by the tunnel cluster.
    pub domain: String,
    /// Effective protocol for this tunnel.
    pub protocol: TunnelProtocol,
}

impl TunnelAssignment {
    /// Human friendly public URL for the tunnel.
    pub fn public_url(&self) -> String {
        match self.protocol {
            TunnelProtocol::Http => format!("https://{}.{}", self.subdomain, self.domain),
            TunnelProtocol::Tcp => format!("tcp://{}.{}", self.subdomain, self.domain),
        }
    }
}

/// Limits communicated between server and client.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RequestLimits {
    /// Maximum body size supported for a single proxied request.
    pub max_body_bytes: usize,
    /// Maximum number of headers accepted.
    pub max_headers: usize,
}

impl Default for RequestLimits {
    fn default() -> Self {
        Self {
            max_body_bytes: 10 * 1024 * 1024,
            max_headers: 64,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn public_url_rendering() {
        let assignment = TunnelAssignment {
            tunnel_id: Uuid::nil(),
            subdomain: "alpha".into(),
            domain: "tunnel.example.com".into(),
            protocol: TunnelProtocol::Http,
        };
        assert_eq!(assignment.public_url(), "https://alpha.tunnel.example.com");
    }
}

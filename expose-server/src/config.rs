//! Server configuration loading utilities and security primitives.

use crate::error::{ExposeError, Result};
use expose_common::types::{RequestLimits, RoutingMode, TcpTuningConfig};
use serde::de::Error as DeError;
use serde::{Deserialize, Deserializer};
use std::fs;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;
use subtle::ConstantTimeEq;

use expose_common::error::ConfigError;
use sha2::{Digest, Sha256};

const DEFAULT_REQUEST_TIMEOUT_SECS: u64 = 30;

/// Wrapper for API keys providing constant-time comparison.
///
/// The original key bytes are never logged. Instead, a short SHA-256 hash
/// prefix is available for diagnostic purposes.
///
/// # Examples
/// ```rust
/// # use expose_server::config::SecureApiKey;
/// let key = SecureApiKey::new("0123456789abcdef0123456789abcdef").unwrap();
/// assert!(key.verify("0123456789abcdef0123456789abcdef"));
/// ```
#[derive(Clone, PartialEq, Eq)]
pub struct SecureApiKey {
    key_bytes: Vec<u8>,
    key_hash: [u8; 32],
}

impl SecureApiKey {
    /// Minimum acceptable key length in bytes.
    pub const MIN_LENGTH: usize = 16;

    /// Builds a secure key from a raw string, validating minimum length.
    ///
    /// # Arguments
    /// * `key` - Raw API key bytes supplied via configuration or environment.
    ///
    /// # Returns
    /// Parsed secure key ready for constant-time comparisons.
    ///
    /// # Errors
    /// Returns [`ConfigError::Validation`] when the key is shorter than
    /// [`SecureApiKey::MIN_LENGTH`].
    ///
    /// # Panics
    /// Panics are never expected here.
    ///
    /// # Examples
    /// ```rust
    /// # use expose_server::config::SecureApiKey;
    /// assert!(SecureApiKey::new("0123456789abcdef0123456789abcdef").is_ok());
    /// ```
    pub fn new(key: &str) -> std::result::Result<Self, ConfigError> {
        let key_bytes = key.as_bytes().to_vec();

        if key_bytes.len() < Self::MIN_LENGTH {
            return Err(ConfigError::Validation(format!(
                "API key must be at least {} characters, got {}",
                Self::MIN_LENGTH,
                key_bytes.len()
            )));
        }

        let mut hasher = Sha256::new();
        hasher.update(&key_bytes);
        let key_hash: [u8; 32] = hasher.finalize().into();

        Ok(Self {
            key_bytes,
            key_hash,
        })
    }

    /// Performs constant-time verification against a candidate key.
    ///
    /// # Arguments
    /// * `candidate` - API key provided by the client during handshake.
    ///
    /// # Returns
    /// `true` when the keys match, `false` otherwise.
    ///
    /// # Errors
    /// This function does not produce errors.
    ///
    /// # Panics
    /// Never panics.
    ///
    /// # Examples
    /// ```rust
    /// # use expose_server::config::SecureApiKey;
    /// let key = SecureApiKey::new("0123456789abcdef0123456789abcdef").unwrap();
    /// assert!(key.verify("0123456789abcdef0123456789abcdef"));
    /// assert!(!key.verify("00000000000000000000000000000000"));
    /// ```
    pub fn verify(&self, candidate: &str) -> bool {
        let candidate_bytes = candidate.as_bytes();
        if candidate_bytes.len() != self.key_bytes.len() {
            let _ = candidate_bytes.ct_eq(candidate_bytes);
            return false;
        }
        self.key_bytes.ct_eq(candidate_bytes).into()
    }

    /// Truncated SHA-256 hash prefix for safe logging.
    pub fn hash_prefix(&self) -> String {
        hex::encode(&self.key_hash[..8])
    }
}

impl std::fmt::Debug for SecureApiKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SecureApiKey")
            .field("hash_prefix", &self.hash_prefix())
            .finish()
    }
}

/// Configuration for pending request management.
///
/// Values here are used by `TunnelManager` to guard against resource
/// exhaustion attacks.
///
/// # Examples
/// ```rust
/// # use expose_server::config::PendingRequestConfig;
/// let cfg = PendingRequestConfig::default();
/// assert_eq!(cfg.max_per_tunnel, 100);
/// ```
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct PendingRequestConfig {
    /// Maximum pending requests allowed per tunnel.
    pub max_per_tunnel: usize,
    /// Maximum pending requests allowed globally.
    pub max_global: usize,
    /// Sweeper interval for timing out stale requests.
    #[serde(
        rename = "sweep_interval_ms",
        deserialize_with = "deserialize_duration_ms"
    )]
    pub sweep_interval: Duration,
    /// Default timeout applied when callers do not specify one.
    #[serde(
        rename = "default_timeout_secs",
        deserialize_with = "deserialize_duration_secs"
    )]
    pub default_timeout: Duration,
}

impl Default for PendingRequestConfig {
    fn default() -> Self {
        Self {
            max_per_tunnel: 100,
            max_global: 10_000,
            sweep_interval: Duration::from_secs(1),
            default_timeout: Duration::from_secs(30),
        }
    }
}

/// Streaming configuration for large HTTP bodies.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct StreamingConfig {
    /// Enable streaming for requests exceeding the threshold.
    pub enabled: bool,
    /// Size beyond which requests are streamed instead of buffered.
    pub threshold_bytes: usize,
    /// Chunk size used when streaming payloads.
    pub chunk_size_bytes: usize,
    /// Hard cap for streamed bodies.
    pub max_body_bytes: usize,
}

impl Default for StreamingConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            threshold_bytes: 1024 * 1024,
            chunk_size_bytes: 64 * 1024,
            max_body_bytes: 100 * 1024 * 1024,
        }
    }
}

/// Metrics configuration for the Prometheus exporter.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct MetricsConfig {
    /// Whether the exporter should run.
    pub enabled: bool,
    /// HTTP bind address for the metrics endpoint.
    pub bind_address: SocketAddr,
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            bind_address: "127.0.0.1:9090".parse().unwrap(),
        }
    }
}

/// Admin API configuration block.
///
/// Operators can disable the admin API entirely, require bearer tokens,
/// or temporarily allow insecure access for local development.
///
/// # Examples
/// ```rust
/// # use expose_server::config::AdminConfig;
/// let cfg = AdminConfig::default();
/// assert!(!cfg.is_enabled());
/// ```
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct AdminConfig {
    #[serde(deserialize_with = "deserialize_optional_secure_key")]
    /// Bearer token required for admin requests.
    pub token: Option<SecureApiKey>,
    /// Allow unauthenticated access (dangerous outside dev).
    pub insecure_admin: bool,
    /// Optional dedicated bind address for admin server.
    pub bind_address: Option<SocketAddr>,
    /// Requests per minute allowed for admin endpoints.
    pub rate_limit_per_minute: u32,
}

impl AdminConfig {
    /// Returns true when the admin API should be enabled.
    pub fn is_enabled(&self) -> bool {
        self.token.is_some() || self.insecure_admin
    }

    /// Validates admin configuration depending on deployment mode.
    ///
    /// # Arguments
    /// * `is_production` - Whether the server is running in production.
    ///
    /// # Returns
    /// `Ok(())` when the configuration is safe for the environment.
    ///
    /// # Errors
    /// Returns [`ConfigError::Validation`] if insecure mode is enabled in
    /// production or other invariants fail.
    pub fn validate(&self, is_production: bool) -> std::result::Result<(), ConfigError> {
        if is_production && self.insecure_admin {
            return Err(ConfigError::Validation(
                "insecure_admin=true is not allowed in production".into(),
            ));
        }
        Ok(())
    }
}

impl Default for AdminConfig {
    fn default() -> Self {
        Self {
            token: None,
            insecure_admin: false,
            bind_address: None,
            rate_limit_per_minute: 60,
        }
    }
}

/// Runtime configuration for the tunnel server.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct ServerConfig {
    pub bind_address: Option<String>,
    pub https_bind_address: Option<String>,
    pub tls_enabled: bool,
    pub tls_cert_path: Option<PathBuf>,
    pub tls_key_path: Option<PathBuf>,
    pub admin_bind: Option<String>,
    pub domain: String,
    pub public_port: Option<u16>,
    /// How incoming requests are routed to tunnels.
    #[serde(default)]
    pub routing_mode: RoutingMode,
    /// URL path prefix for path-based routing.
    #[serde(default = "default_path_prefix")]
    pub path_prefix: String,
    pub rate_limit_requests_per_minute: u32,
    pub rate_limit_burst_size: u32,
    #[serde(deserialize_with = "deserialize_api_keys")]
    pub api_keys: Vec<SecureApiKey>,
    pub request_body_limit_bytes: usize,
    pub request_timeout_secs: u64,
    pub tcp_tuning: TcpTuningConfig,
    pub admin: AdminConfig,
    pub pending_requests: PendingRequestConfig,
    pub streaming: StreamingConfig,
    pub metrics: MetricsConfig,
    pub tcp_forward: TcpForwardConfig,
    pub limits: ResourceLimits,
    #[serde(default, rename = "max_tunnels")]
    legacy_max_tunnels: Option<usize>,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            bind_address: Some("127.0.0.1:8080".into()),
            https_bind_address: None,
            tls_enabled: false,
            tls_cert_path: None,
            tls_key_path: None,
            admin_bind: Some("127.0.0.1:9090".into()),
            domain: "tunnel.localhost".into(),
            public_port: None,
            routing_mode: RoutingMode::Path,
            path_prefix: default_path_prefix(),
            rate_limit_requests_per_minute: 120,
            rate_limit_burst_size: 10,
            api_keys: Vec::new(),
            request_body_limit_bytes: 10 * 1024 * 1024,
            request_timeout_secs: DEFAULT_REQUEST_TIMEOUT_SECS,
            tcp_tuning: TcpTuningConfig::default(),
            admin: AdminConfig::default(),
            pending_requests: PendingRequestConfig::default(),
            streaming: StreamingConfig::default(),
            metrics: MetricsConfig::default(),
            tcp_forward: TcpForwardConfig::default(),
            limits: ResourceLimits::default(),
            legacy_max_tunnels: None,
        }
    }
}

/// TCP forwarding configuration (host for per-tunnel listeners).
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct TcpForwardConfig {
    /// Address used when binding per-tunnel TCP listeners (host or host:port pattern).
    pub bind_host: String,
}

impl Default for TcpForwardConfig {
    fn default() -> Self {
        Self {
            bind_host: "0.0.0.0".into(),
        }
    }
}

impl ServerConfig {
    /// Load configuration from disk or fall back to defaults.
    pub fn load(path: Option<&Path>) -> Result<Self> {
        let mut cfg: ServerConfig = if let Some(path) = path {
            let contents = fs::read_to_string(path)?;
            toml::from_str(&contents)
                .map_err(ConfigError::Parse)
                .map_err(ExposeError::from)?
        } else {
            ServerConfig::default()
        };

        if cfg.tls_enabled {
            if cfg.https_bind_address().is_none() {
                return Err(ExposeError::Config(ConfigError::Validation(
                    "https_bind_address must be set when tls_enabled = true".into(),
                )));
            }
            cfg.tls_paths()?;
        }

        if cfg.admin.bind_address.is_none() {
            if let Some(bind) = cfg.admin_bind.as_deref() {
                if let Ok(addr) = bind.parse::<SocketAddr>() {
                    cfg.admin.bind_address = Some(addr);
                }
            }
        }

        if let Some(legacy) = cfg.legacy_max_tunnels.take() {
            cfg.limits.max_tunnels = legacy;
        }
        if cfg.limits.max_pending_requests != default_max_pending()
            && cfg.pending_requests.max_global == PendingRequestConfig::default().max_global
        {
            cfg.pending_requests.max_global = cfg.limits.max_pending_requests;
        }

        let is_production = std::env::var("EXPOSE_ENV")
            .map(|value| value.eq_ignore_ascii_case("production"))
            .unwrap_or(false);
        cfg.admin
            .validate(is_production)
            .map_err(ExposeError::from)?;

        cfg.validate_routing()?;

        Ok(cfg)
    }

    /// Determines whether an API key is valid.
    pub fn validate_api_key(&self, key: Option<&str>) -> bool {
        if self.api_keys.is_empty() {
            tracing::debug!("No API keys configured, allowing anonymous access");
            return true;
        }
        let candidate = match key {
            Some(value) => value,
            None => {
                tracing::debug!("API key required but missing");
                return false;
            }
        };
        self.api_keys.iter().any(|k| k.verify(candidate))
    }

    /// Validates admin bearer tokens when the API is enabled.
    pub fn validate_admin_token(&self, candidate: &str) -> bool {
        self.admin
            .token
            .as_ref()
            .map(|token| token.verify(candidate))
            .unwrap_or(false)
    }

    /// Indicates whether the admin API should accept traffic.
    pub fn is_admin_enabled(&self) -> bool {
        self.admin.is_enabled()
    }

    /// Request limits advertised to clients.
    pub fn limits(&self) -> RequestLimits {
        RequestLimits {
            max_body_bytes: self.request_body_limit_bytes,
            max_headers: 64,
        }
    }

    /// Returns sanitized per-minute and burst limits (never zero).
    pub fn rate_limit_config(&self) -> (u32, u32) {
        (
            self.rate_limit_requests_per_minute.max(1),
            self.rate_limit_burst_size.max(1),
        )
    }

    /// Optional HTTP bind address string, ignoring empty values.
    pub fn http_bind_address(&self) -> Option<&str> {
        self.bind_address.as_deref().and_then(|addr| {
            if addr.trim().is_empty() {
                None
            } else {
                Some(addr)
            }
        })
    }

    /// Optional HTTPS bind address string, ignoring empty values.
    pub fn https_bind_address(&self) -> Option<&str> {
        self.https_bind_address.as_deref().and_then(|addr| {
            if addr.trim().is_empty() {
                None
            } else {
                Some(addr)
            }
        })
    }

    /// Resolve certificate/key paths and ensure TLS is properly configured.
    pub fn tls_paths(&self) -> Result<(&Path, &Path)> {
        let cert = self.tls_cert_path.as_deref().ok_or_else(|| {
            ExposeError::Config(ConfigError::Validation(
                "tls_cert_path must be set when tls_enabled = true".into(),
            ))
        })?;
        let key = self.tls_key_path.as_deref().ok_or_else(|| {
            ExposeError::Config(ConfigError::Validation(
                "tls_key_path must be set when tls_enabled = true".into(),
            ))
        })?;
        Ok((cert, key))
    }

    /// Returns the public-facing port for URL construction.
    pub fn effective_public_port(&self) -> Option<u16> {
        if let Some(port) = self.public_port {
            return Some(port);
        }

        fn parse(addr: Option<&str>) -> Option<u16> {
            addr?.parse::<SocketAddr>().ok().map(|socket| socket.port())
        }

        if self.tls_enabled {
            parse(self.https_bind_address())
        } else {
            parse(self.http_bind_address())
        }
    }

    #[allow(dead_code)]
    pub fn advertised_public_port(&self) -> Option<u16> {
        self.effective_public_port()
    }

    fn validate_routing(&self) -> Result<()> {
        if self.routing_mode.supports_path() {
            if !self.path_prefix.starts_with('/') {
                return Err(ExposeError::Config(ConfigError::Validation(
                    "path_prefix must start with '/'".into(),
                )));
            }
            if self.path_prefix.len() < 2 {
                return Err(ExposeError::Config(ConfigError::Validation(
                    "path_prefix must be at least 2 characters (e.g., '/t')".into(),
                )));
            }
            if self.path_prefix.ends_with('/') {
                return Err(ExposeError::Config(ConfigError::Validation(
                    "path_prefix must not end with '/'".into(),
                )));
            }
            let prefix_body = &self.path_prefix[1..];
            if !prefix_body
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
            {
                return Err(ExposeError::Config(ConfigError::Validation(
                    "path_prefix may only contain alphanumerics, hyphens, and underscores after '/'".into(),
                )));
            }
        }

        tracing::info!(
            routing_mode = %self.routing_mode,
            path_prefix = %self.path_prefix,
            "Routing configuration loaded"
        );
        Ok(())
    }
}

fn default_path_prefix() -> String {
    "/t".to_string()
}

/// Server resource limits configured via `[limits]`.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct ResourceLimits {
    /// Maximum concurrent tunnels (0 = unlimited).
    #[serde(default = "default_max_tunnels")]
    pub max_tunnels: usize,
    /// Maximum number of tunnels per API key (0 = unlimited).
    pub max_tunnels_per_key: usize,
    /// Maximum pending requests allowed globally.
    #[serde(default = "default_max_pending")]
    pub max_pending_requests: usize,
}

fn default_max_tunnels() -> usize {
    1000
}

fn default_max_pending() -> usize {
    10_000
}

impl Default for ResourceLimits {
    fn default() -> Self {
        Self {
            max_tunnels: default_max_tunnels(),
            max_tunnels_per_key: 0,
            max_pending_requests: default_max_pending(),
        }
    }
}

fn deserialize_api_keys<'de, D>(deserializer: D) -> std::result::Result<Vec<SecureApiKey>, D::Error>
where
    D: Deserializer<'de>,
{
    let raw: Vec<String> = Vec::<String>::deserialize(deserializer)?;
    raw.iter()
        .map(|value| SecureApiKey::new(value).map_err(|err| DeError::custom(err.to_string())))
        .collect()
}

fn deserialize_optional_secure_key<'de, D>(
    deserializer: D,
) -> std::result::Result<Option<SecureApiKey>, D::Error>
where
    D: Deserializer<'de>,
{
    let value: Option<String> = Option::<String>::deserialize(deserializer)?;
    if let Some(key) = value {
        SecureApiKey::new(&key)
            .map(Some)
            .map_err(|err| DeError::custom(err.to_string()))
    } else {
        Ok(None)
    }
}

fn deserialize_duration_ms<'de, D>(deserializer: D) -> std::result::Result<Duration, D::Error>
where
    D: Deserializer<'de>,
{
    let millis = u64::deserialize(deserializer)?;
    Ok(Duration::from_millis(millis))
}

fn deserialize_duration_secs<'de, D>(deserializer: D) -> std::result::Result<Duration, D::Error>
where
    D: Deserializer<'de>,
{
    let secs = u64::deserialize(deserializer)?;
    Ok(Duration::from_secs(secs))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_secure_api_key_valid_key_accepted() {
        let key = SecureApiKey::new("this-is-a-valid-api-key-1234").unwrap();
        assert!(key.verify("this-is-a-valid-api-key-1234"));
    }

    #[test]
    fn test_secure_api_key_wrong_key_rejected() {
        let key = SecureApiKey::new("this-is-a-valid-api-key-1234").unwrap();
        assert!(!key.verify("this-is-a-wrong-api-key-5678"));
    }

    #[test]
    fn test_secure_api_key_different_length_rejected() {
        let key = SecureApiKey::new("this-is-a-valid-api-key-1234").unwrap();
        assert!(!key.verify("short"));
        assert!(!key.verify("this-is-a-valid-api-key-1234-extra"));
    }

    #[test]
    fn test_secure_api_key_too_short_returns_error() {
        let result = SecureApiKey::new("short");
        assert!(matches!(result, Err(ConfigError::Validation(_))));
    }

    #[test]
    fn test_server_config_empty_keys_allows_anonymous() {
        let config = ServerConfig {
            api_keys: vec![],
            ..Default::default()
        };
        assert!(config.validate_api_key(None));
        assert!(config.validate_api_key(Some("anything")));
    }

    #[test]
    fn test_server_config_with_keys_requires_match() {
        let config = ServerConfig {
            api_keys: vec![SecureApiKey::new("valid-key-1234567890123456").unwrap()],
            ..Default::default()
        };
        assert!(!config.validate_api_key(None));
        assert!(!config.validate_api_key(Some("wrong-key-987654321")));
        assert!(config.validate_api_key(Some("valid-key-1234567890123456")));
    }

    #[test]
    fn test_admin_config_validation_rejects_insecure_production() {
        let config = AdminConfig {
            token: None,
            insecure_admin: true,
            ..Default::default()
        };
        let result = config.validate(true);
        assert!(matches!(result, Err(ConfigError::Validation(_))));
    }

    #[test]
    fn test_server_config_default_routing_mode_is_path() {
        let config = ServerConfig::default();
        assert_eq!(config.routing_mode, RoutingMode::Path);
    }

    #[test]
    fn test_server_config_default_path_prefix() {
        let config = ServerConfig::default();
        assert_eq!(config.path_prefix, "/t");
    }

    #[test]
    fn test_server_config_path_prefix_validation_missing_slash() {
        let config = ServerConfig {
            path_prefix: "t".into(),
            routing_mode: RoutingMode::Path,
            ..Default::default()
        };
        assert!(config.validate_routing().is_err());
    }

    #[test]
    fn test_server_config_path_prefix_validation_trailing_slash() {
        let config = ServerConfig {
            path_prefix: "/t/".into(),
            routing_mode: RoutingMode::Path,
            ..Default::default()
        };
        assert!(config.validate_routing().is_err());
    }

    #[test]
    fn test_server_config_path_prefix_valid() {
        let config = ServerConfig {
            path_prefix: "/tunnel".into(),
            routing_mode: RoutingMode::Path,
            ..Default::default()
        };
        assert!(config.validate_routing().is_ok());
    }

    #[test]
    fn test_effective_public_port_explicit() {
        let config = ServerConfig {
            public_port: Some(443),
            ..Default::default()
        };
        assert_eq!(config.effective_public_port(), Some(443));
    }

    #[test]
    fn test_effective_public_port_from_bind_address() {
        let config = ServerConfig {
            bind_address: Some("0.0.0.0:8080".into()),
            public_port: None,
            tls_enabled: false,
            ..Default::default()
        };
        assert_eq!(config.effective_public_port(), Some(8080));
    }

    #[test]
    fn test_config_deserializes_routing_mode_from_toml() {
        let toml_str = r#"
            bind_address = "0.0.0.0:8080"
            domain = "example.com"
            tls_enabled = false
            routing_mode = "both"
            path_prefix = "/tunnel"
        "#;

        let config: ServerConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(config.routing_mode, RoutingMode::Both);
        assert_eq!(config.path_prefix, "/tunnel");
    }

    #[test]
    fn test_config_defaults_when_routing_fields_absent() {
        let toml_str = r#"
            bind_address = "0.0.0.0:8080"
            domain = "example.com"
            tls_enabled = false
        "#;

        let config: ServerConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(config.routing_mode, RoutingMode::Path);
        assert_eq!(config.path_prefix, "/t");
    }
}

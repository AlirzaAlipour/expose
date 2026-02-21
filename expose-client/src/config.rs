//! Configuration helpers combining CLI and file based settings.

use crate::cli::{Cli, Command};
use crate::error::{ClientError, Result};
use expose_common::error::ConfigError;
use expose_common::types::{TcpTuningConfig, TunnelConfig, TunnelProtocol};
use expose_common::utils;
use serde::Deserialize;
use std::fs;
use std::path::Path;

fn validation_error(msg: impl Into<String>) -> ClientError {
    ClientError::from(ConfigError::Validation(msg.into()))
}

/// Configuration as stored on disk (TOML file).
#[derive(Debug, Clone, Deserialize, Default)]
pub struct FileConfig {
    pub server_url: Option<String>,
    pub api_key: Option<String>,
    pub local_host: Option<String>,
    pub local_port: Option<u16>,
    pub protocol: Option<TunnelProtocol>,
    pub subdomain: Option<String>,
    pub reconnect_max_attempts: Option<u32>,
    pub reconnect_base_delay_ms: Option<u64>,
    pub tcp_tuning: Option<TcpTuningConfig>,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct MultiFileConfig {
    pub server_url: Option<String>,
    pub api_key: Option<String>,
    pub reconnect_max_attempts: Option<u32>,
    pub reconnect_base_delay_ms: Option<u64>,
    pub tunnels: Vec<MultiFileTunnel>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct MultiFileTunnel {
    pub name: Option<String>,
    pub protocol: Option<TunnelProtocol>,
    pub local_host: Option<String>,
    pub local_port: u16,
    pub subdomain: Option<String>,
}

#[derive(Debug, Clone)]
pub struct MultiRuntimeConfig {
    pub server_url: String,
    pub api_key: Option<String>,
    pub reconnect_max_attempts: u32,
    pub reconnect_base_delay_ms: u64,
    pub tunnels: Vec<TunnelConfig>,
}

/// Load a configuration file if provided.
pub fn load_file(path: Option<&Path>) -> Result<FileConfig> {
    if let Some(path) = path {
        let contents = fs::read_to_string(path)
            .map_err(|err| ClientError::from(ConfigError::FileRead(err)))?;
        let cfg: FileConfig = toml::from_str(&contents)
            .map_err(ConfigError::Parse)
            .map_err(ClientError::from)?;
        Ok(cfg)
    } else {
        Ok(FileConfig::default())
    }
}

/// Combine CLI arguments with file configuration to build the runtime tunnel configuration.
pub fn build_runtime_config(cli: &Cli) -> Result<TunnelConfig> {
    let file_cfg = load_file(cli.config.as_deref())?;

    let mut config = TunnelConfig::default();
    if let Some(server) = cli.server.clone().or(file_cfg.server_url.clone()) {
        config.server_url = server;
    }

    config.api_key = cli.api_key.clone().or(file_cfg.api_key.clone());

    let (protocol, port, command_host) = match &cli.command {
        Command::Http { port, host } => (TunnelProtocol::Http, *port, host.clone()),
        Command::Tcp { port, host } => (TunnelProtocol::Tcp, *port, host.clone()),
        Command::Multi { .. } => {
            return Err(validation_error(
                "multi command relies on --config file; use that instead of single-tunnel options",
            ));
        }
    };

    config.protocol = protocol;
    config.local_port = port;

    const DEFAULT_HOST: &str = "127.0.0.1";
    config.local_host = if command_host == DEFAULT_HOST {
        file_cfg
            .local_host
            .clone()
            .unwrap_or_else(|| command_host.clone())
    } else {
        command_host
    };

    if let Some(subdomain) = cli.subdomain.clone().or(file_cfg.subdomain.clone()) {
        let sanitized = utils::sanitize_subdomain(&subdomain).ok_or_else(|| {
            validation_error(format!(
                "Subdomain '{subdomain}' is invalid. Use lowercase letters, numbers, and hyphens (e.g., 'my-app')."
            ))
        })?;
        config.subdomain = Some(sanitized);
    }

    // Validate the server URL early to provide quick feedback.
    url::Url::parse(&config.server_url)
        .map_err(|err| validation_error(format!("Invalid server URL: {err}")))?;

    if let Some(max_attempts) = cli.reconnect_attempts.or(file_cfg.reconnect_max_attempts) {
        config.reconnect_max_attempts = max_attempts;
    }

    if let Some(base_delay) = cli
        .reconnect_base_delay_ms
        .or(file_cfg.reconnect_base_delay_ms)
    {
        config.reconnect_base_delay_ms = base_delay.max(100);
    }

    if let Some(tuning) = file_cfg.tcp_tuning {
        config.tcp_tuning = tuning;
    }

    Ok(config)
}

/// Load a multi-tunnel configuration file.
pub fn load_multi_runtime_config(path: &Path) -> Result<MultiRuntimeConfig> {
    let contents =
        fs::read_to_string(path).map_err(|err| ClientError::from(ConfigError::FileRead(err)))?;
    let file_cfg: MultiFileConfig = toml::from_str(&contents)
        .map_err(ConfigError::Parse)
        .map_err(ClientError::from)?;

    let server_url = file_cfg
        .server_url
        .ok_or_else(|| validation_error("server_url must be set for multi tunnels"))?;

    let reconnect_max_attempts = file_cfg.reconnect_max_attempts.unwrap_or(0);
    let reconnect_base_delay_ms = file_cfg.reconnect_base_delay_ms.unwrap_or(1_000);

    let mut tunnels = Vec::new();
    for entry in file_cfg.tunnels.iter() {
        let mut cfg = TunnelConfig::default();
        cfg.server_url = server_url.clone();
        cfg.api_key = file_cfg.api_key.clone();
        cfg.protocol = entry.protocol.unwrap_or(TunnelProtocol::Http);
        cfg.local_host = entry
            .local_host
            .clone()
            .unwrap_or_else(|| "127.0.0.1".into());
        cfg.local_port = entry.local_port;

        if let Some(subdomain) = entry.subdomain.as_ref() {
            let sanitized = utils::sanitize_subdomain(subdomain).ok_or_else(|| {
                validation_error(format!(
                    "Subdomain '{subdomain}' is invalid. Use lowercase letters, numbers, and hyphens."
                ))
            })?;
            cfg.subdomain = Some(sanitized);
        }

        tunnels.push(cfg);
    }

    if tunnels.is_empty() {
        return Err(validation_error(
            "multi config must list at least one tunnel",
        ));
    }

    Ok(MultiRuntimeConfig {
        server_url,
        api_key: file_cfg.api_key.clone(),
        reconnect_max_attempts,
        reconnect_base_delay_ms,
        tunnels,
    })
}

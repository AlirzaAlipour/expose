//! Command line interface definition for the Expose client.

use clap::{Parser, Subcommand};
use std::path::{Path, PathBuf};

/// Command line options for the Expose client.
#[derive(Debug, Parser)]
#[command(
    author,
    version,
    about = "Expose local services through a secure tunnel"
)]
pub struct Cli {
    /// Optional path to a configuration file.
    #[arg(long, value_name = "PATH")]
    pub config: Option<PathBuf>,

    /// Override the tunnel server URL.
    #[arg(long, global = true, value_name = "URL")]
    pub server: Option<String>,

    /// Provide the API key directly via CLI.
    #[arg(long, global = true, env = "EXPOSE_API_KEY", hide_env_values = true)]
    pub api_key: Option<String>,

    /// Preferred subdomain for the tunnel.
    #[arg(long, global = true, value_name = "NAME")]
    pub subdomain: Option<String>,

    /// Maximum number of reconnect attempts (0 = infinite).
    #[arg(long, global = true, value_name = "NUM")]
    pub reconnect_attempts: Option<u32>,

    /// Base reconnect delay in milliseconds (defaults to 1000).
    #[arg(long, global = true, value_name = "MS")]
    pub reconnect_base_delay_ms: Option<u64>,

    /// Subcommands describing which protocol to expose.
    #[command(subcommand)]
    pub command: Command,
}

/// Supported CLI subcommands.
#[derive(Debug, Subcommand)]
pub enum Command {
    /// Expose a local HTTP server.
    Http {
        /// Local port to forward requests to.
        port: u16,
        /// Override the local host (defaults to 127.0.0.1).
        #[arg(long, default_value = "127.0.0.1", value_name = "HOST")]
        host: String,
    },
    /// Expose a raw TCP port.
    Tcp {
        /// Local port to forward traffic to.
        port: u16,
        /// Override the local host (defaults to 127.0.0.1).
        #[arg(long, default_value = "127.0.0.1", value_name = "HOST")]
        host: String,
    },
    /// Expose multiple services defined in a config file.
    Multi {
        /// Path to the multi-tunnel configuration file.
        #[arg(short, long, value_name = "PATH")]
        config: PathBuf,
    },
}

impl Command {
    /// Validate the selected subcommand.
    pub fn validate(&self) -> Result<(), CliError> {
        match self {
            Command::Http { port, .. } => {
                if *port == 0 {
                    return Err(CliError::InvalidPort("port cannot be 0".into()));
                }
                Ok(())
            }
            Command::Tcp { port, .. } => {
                if *port == 0 {
                    return Err(CliError::InvalidPort("port cannot be 0".into()));
                }
                Ok(())
            }
            Command::Multi { config } => {
                if !Path::new(config).exists() {
                    return Err(CliError::InvalidConfig(format!(
                        "multi config file '{}' not found",
                        config.display()
                    )));
                }
                Ok(())
            }
        }
    }
}

/// CLI validation errors surfaced before runtime configuration is built.
#[derive(Debug)]
pub enum CliError {
    InvalidPort(String),
    InvalidConfig(String),
}

impl std::fmt::Display for CliError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidPort(msg) => write!(f, "Invalid port: {msg}"),
            Self::InvalidConfig(msg) => write!(f, "Configuration error: {msg}"),
        }
    }
}

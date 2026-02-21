//! High level orchestration for the client binary.

use crate::cli::{Cli, Command};
use crate::config;
use crate::error::Result;
use crate::multi;
use crate::tunnel;

/// Entry point invoked by `main` after CLI parsing.
pub async fn run(cli: Cli) -> Result<()> {
    match &cli.command {
        Command::Multi { config: path } => {
            let runtime = config::load_multi_runtime_config(path)?;
            multi::run(runtime).await
        }
        _ => {
            let runtime_config = config::build_runtime_config(&cli)?;
            tunnel::run(runtime_config).await
        }
    }
}

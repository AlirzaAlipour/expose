use clap::Parser;
use expose_server::{config::ServerConfig, metrics, server::Server, tracing::TracingConfig};
use std::path::PathBuf;

#[derive(Debug, Parser)]
#[command(author, version, about = "Expose tunnel server")]
struct Cli {
    /// Optional path to a TOML configuration file.
    #[arg(long)]
    config: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    expose_server::tracing::init_tracing(&TracingConfig::default())?;
    let cli = Cli::parse();
    let config = ServerConfig::load(cli.config.as_deref())?;
    metrics::init_metrics(&config.metrics)?;
    Server::new(config).run().await?;
    Ok(())
}

use clap::Parser;
use expose_client::{cli, client};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();
    let cli = cli::Cli::parse();

    if let Err(err) = cli.command.validate() {
        eprintln!("Error: {err}");
        std::process::exit(1);
    }

    if let Err(err) = client::run(cli).await {
        eprintln!("error: {err}");
        std::process::exit(1);
    }

    Ok(())
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .without_time()
        .init();
}

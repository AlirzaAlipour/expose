use tracing_subscriber::{fmt, prelude::*, EnvFilter};

#[derive(Debug, Clone)]
pub struct TracingConfig {
    pub default_level: String,
    pub json_output: bool,
}

impl Default for TracingConfig {
    fn default() -> Self {
        Self {
            default_level: "info".into(),
            json_output: false,
        }
    }
}

#[derive(Debug, thiserror::Error)]
#[error("failed to initialize tracing: {0}")]
pub struct TracingError(String);

/// Initialize tracing subscribers with structured logging.
pub fn init_tracing(config: &TracingConfig) -> Result<(), TracingError> {
    let env_filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(&config.default_level));

    let subscriber = tracing_subscriber::registry().with(env_filter);

    if config.json_output {
        subscriber
            .with(fmt::layer().json())
            .try_init()
            .map_err(|err| TracingError(err.to_string()))?
    } else {
        subscriber
            .with(fmt::layer().with_target(false))
            .try_init()
            .map_err(|err| TracingError(err.to_string()))?
    }

    Ok(())
}

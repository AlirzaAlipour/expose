use std::future::Future;
use tokio::time::{timeout, Duration};

pub mod fixtures;
pub mod mock_server;

/// Default timeout for most async test operations.
pub const DEFAULT_TEST_TIMEOUT: Duration = Duration::from_secs(5);
/// Faster timeout for operations expected to complete almost instantly.
pub const FAST_TIMEOUT: Duration = Duration::from_secs(1);
/// Timeout for flows that intentionally take longer (e.g. reconnection loops).
pub const SLOW_TIMEOUT: Duration = Duration::from_secs(10);

/// Wraps a future with a timeout and decorates any error with context.
pub async fn with_timeout<F, T>(operation: &str, duration: Duration, fut: F) -> anyhow::Result<T>
where
    F: Future<Output = anyhow::Result<T>>,
{
    match timeout(duration, fut).await {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(err)) => Err(anyhow::anyhow!("operation '{operation}' failed: {err}")),
        Err(_) => Err(anyhow::anyhow!(
            "operation '{operation}' timed out after {:?}",
            duration
        )),
    }
}

/// Initialize tracing for tests exactly once so debugging output is available when needed.
pub fn init_tracing() {
    static INIT: std::sync::Once = std::sync::Once::new();
    INIT.call_once(|| {
        let _ = tracing_subscriber::fmt()
            .with_test_writer()
            .with_max_level(tracing::Level::DEBUG)
            .try_init();
    });
}

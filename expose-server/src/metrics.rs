use std::time::Duration;

use metrics::{counter, describe_counter, describe_gauge, describe_histogram, gauge, histogram};
use metrics_exporter_prometheus::{BuildError, PrometheusBuilder};
use thiserror::Error;

use crate::config::MetricsConfig;
use crate::error::ExposeError;

#[derive(Debug, Error)]
pub enum MetricsError {
    #[error("failed to start metrics exporter: {0}")]
    Install(#[from] BuildError),
}

/// Initialize the Prometheus exporter based on configuration.
pub fn init_metrics(config: &MetricsConfig) -> Result<(), MetricsError> {
    if !config.enabled {
        return Ok(());
    }

    register_descriptions();
    PrometheusBuilder::new()
        .with_http_listener(config.bind_address)
        .install_recorder()?;
    tracing::info!(address = %config.bind_address, "metrics exporter started");
    Ok(())
}

fn register_descriptions() {
    describe_counter!("expose_requests_total", "Total HTTP requests proxied");
    describe_counter!(
        "expose_requests_failed_total",
        "Total proxied HTTP requests that resulted in errors"
    );
    describe_counter!("expose_rate_limit_hits_total", "Rate limit denials");
    describe_counter!(
        "expose_bytes_sent_total",
        "Bytes forwarded to tunnel clients"
    );
    describe_counter!(
        "expose_bytes_received_total",
        "Bytes received from tunnel clients"
    );
    describe_gauge!("expose_tunnels_active", "Number of active tunnels");
    describe_gauge!(
        "expose_pending_requests",
        "Pending HTTP requests waiting on clients"
    );
    describe_histogram!(
        "expose_request_duration_seconds",
        "Duration required to proxy a single HTTP request"
    );
}

/// Helper for emitting server-side metrics.
pub struct ServerMetrics;

impl ServerMetrics {
    pub fn tunnels_active(count: usize) {
        gauge!("expose_tunnels_active", count as f64);
    }

    pub fn request_started(subdomain: &str) {
        counter!("expose_requests_total", 1, "subdomain" => subdomain.to_string());
    }

    pub fn request_completed(subdomain: &str, status: u16, duration: Duration) {
        let status_class = format!("{}xx", status / 100);
        histogram!(
            "expose_request_duration_seconds",
            duration.as_secs_f64(),
            "subdomain" => subdomain.to_string(),
            "status_class" => status_class,
        );
    }

    pub fn request_failed(subdomain: &str, error: &ExposeError) {
        counter!(
            "expose_requests_failed_total",
            1,
            "subdomain" => subdomain.to_string(),
            "reason" => (error.error_code() as u16).to_string(),
        );
    }

    pub fn rate_limit_hit(subdomain: &str) {
        counter!("expose_rate_limit_hits_total", 1, "subdomain" => subdomain.to_string());
    }

    pub fn pending_requests_changed(count: usize) {
        gauge!("expose_pending_requests", count as f64);
    }

    pub fn bytes_sent(subdomain: &str, bytes: usize) {
        counter!("expose_bytes_sent_total", bytes as u64, "subdomain" => subdomain.to_string());
    }

    pub fn bytes_received(subdomain: &str, bytes: usize) {
        counter!(
            "expose_bytes_received_total",
            bytes as u64,
            "subdomain" => subdomain.to_string(),
        );
    }
}

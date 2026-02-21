//! Simple wrapper around `tower` concurrency limiting to avoid resource exhaustion.
//!
//! This controls the global number of in-flight requests hitting the Axum
//! stack. Per-tunnel request throttling is implemented directly inside
//! `tunnel_manager.rs` using governor token buckets.

use tower::limit::ConcurrencyLimitLayer;

/// Build a concurrency limiting layer.
pub fn layer(max_concurrent: usize) -> ConcurrencyLimitLayer {
    ConcurrencyLimitLayer::new(max_concurrent)
}

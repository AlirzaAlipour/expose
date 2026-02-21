//! Library entry point exposing the server modules for integration tests.

pub mod admin;
pub mod config;
pub mod error;
pub mod health;
pub mod io_uring_proxy;
pub mod metrics;
pub mod middleware;
pub mod platform;
pub mod proxy;
pub mod server;
pub mod tcp_proxy;
pub mod tcp_tuning;
pub mod tls;
pub mod tracing;
pub mod tunnel_manager;
pub mod zero_copy;

pub use config::ServerConfig;
pub use server::Server;

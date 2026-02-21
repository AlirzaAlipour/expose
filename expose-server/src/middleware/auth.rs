//! Authorization middleware for admin routes.

use std::sync::Arc;

use axum::extract::{Request, State};
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use tracing::{info, warn};

use crate::config::ServerConfig;

/// Enforces admin authentication and handles insecure/dev mode semantics.
///
/// # Arguments
/// * `State(config)` - Shared server configuration.
/// * `request` - Incoming Axum request.
/// * `next` - Next middleware/service in the chain.
///
/// # Returns
/// [`Response`] with either an error body or the downstream handler output.
///
/// # Errors
/// Surfaced via HTTP responses rather than Rust errors.
///
/// # Panics
/// Never panics.
pub async fn admin_auth_middleware(
    State(config): State<Arc<ServerConfig>>,
    request: Request,
    next: Next,
) -> Response {
    let admin_config = &config.admin;

    if !admin_config.is_enabled() {
        warn!(path = %request.uri().path(), "Admin API disabled");
        return (
            StatusCode::FORBIDDEN,
            [("content-type", "application/json")],
            r#"{"error":"admin API is disabled"}"#,
        )
            .into_response();
    }

    if admin_config.insecure_admin {
        warn!(path = %request.uri().path(), "Admin request allowed in insecure mode");
        return next.run(request).await;
    }

    let auth_header = request
        .headers()
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok());

    let token = match auth_header {
        Some(header) if header.starts_with("Bearer ") => &header[7..],
        _ => {
            warn!(path = %request.uri().path(), "Missing admin Authorization header");
            return (
                StatusCode::UNAUTHORIZED,
                [
                    ("content-type", "application/json"),
                    ("www-authenticate", "Bearer realm=\"admin\""),
                ],
                r#"{"error":"missing or invalid authorization header"}"#,
            )
                .into_response();
        }
    };

    if !config.validate_admin_token(token) {
        warn!(path = %request.uri().path(), "Invalid admin token supplied");
        return (
            StatusCode::FORBIDDEN,
            [("content-type", "application/json")],
            r#"{"error":"invalid admin token"}"#,
        )
            .into_response();
    }

    info!(path = %request.uri().path(), "Admin request authorized");
    next.run(request).await
}

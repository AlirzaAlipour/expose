//! Admin API exposing tunnel metadata with strict authentication.

use std::sync::Arc;

use axum::extract::{Extension, Path, Request, State};
use axum::http::{HeaderValue, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{middleware, Json, Router};
use governor::clock::{Clock, DefaultClock};
use governor::state::{InMemoryState, NotKeyed};
use governor::{Quota, RateLimiter};
use serde_json::json;
use std::num::NonZeroU32;
use uuid::Uuid;

use crate::middleware::auth::admin_auth_middleware;
use crate::server::AppState;

/// Build the admin router with authentication and rate limiting.
pub fn router(state: Arc<AppState>) -> Router {
    let config = state.config.clone();
    let rate_limiter = AdminRateLimiter::new(state.config.admin.rate_limit_per_minute);

    let protected = Router::new()
        .route("/tunnels", get(list_tunnels))
        .route("/tunnels/:id", get(get_tunnel).delete(disconnect_tunnel))
        .route("/stats", get(get_stats))
        .route_layer(middleware::from_fn_with_state(
            rate_limiter,
            admin_rate_limit_middleware,
        ))
        .route_layer(middleware::from_fn_with_state(
            config,
            admin_auth_middleware,
        ))
        .layer(Extension(state.clone()));

    Router::new()
        .merge(protected)
        .route("/health", get(health_check))
        .layer(Extension(state))
}

async fn list_tunnels(Extension(state): Extension<Arc<AppState>>) -> impl IntoResponse {
    Json(state.manager.list())
}

async fn get_tunnel(
    Path(tunnel_id): Path<String>,
    Extension(state): Extension<Arc<AppState>>,
) -> impl IntoResponse {
    match Uuid::parse_str(&tunnel_id) {
        Ok(id) => match state.manager.summary_by_id(&id) {
            Some(summary) => Json(summary).into_response(),
            None => (
                StatusCode::NOT_FOUND,
                Json(json!({"error":"tunnel not found"})),
            )
                .into_response(),
        },
        Err(_) => (
            StatusCode::BAD_REQUEST,
            Json(json!({"error":"invalid tunnel id"})),
        )
            .into_response(),
    }
}

async fn disconnect_tunnel(
    Path(tunnel_id): Path<String>,
    Extension(state): Extension<Arc<AppState>>,
) -> impl IntoResponse {
    match Uuid::parse_str(&tunnel_id) {
        Ok(id) => {
            if state.manager.disconnect_tunnel(&id) {
                Json(json!({"status":"disconnected"})).into_response()
            } else {
                (
                    StatusCode::NOT_FOUND,
                    Json(json!({"error":"tunnel not found"})),
                )
                    .into_response()
            }
        }
        Err(_) => (
            StatusCode::BAD_REQUEST,
            Json(json!({"error":"invalid tunnel id"})),
        )
            .into_response(),
    }
}

async fn get_stats(Extension(state): Extension<Arc<AppState>>) -> impl IntoResponse {
    Json(json!({
        "domain": state.manager.domain(),
        "active_tunnels": state.manager.active_tunnel_count(),
        "pending_requests": state.manager.pending_request_count(),
        "rate_limit_per_minute": state.config.rate_limit_requests_per_minute,
    }))
}

async fn health_check(Extension(state): Extension<Arc<AppState>>) -> impl IntoResponse {
    Json(json!({
        "status": "ok",
        "tunnels": state.manager.active_tunnel_count(),
        "timestamp": chrono::Utc::now().to_rfc3339(),
    }))
}

#[derive(Clone)]
struct AdminRateLimiter {
    limiter: Arc<RateLimiter<NotKeyed, InMemoryState, DefaultClock>>,
}

impl AdminRateLimiter {
    fn new(per_minute: u32) -> Self {
        let quota = Quota::per_minute(NonZeroU32::new(per_minute.max(1)).unwrap());
        Self {
            limiter: Arc::new(RateLimiter::direct(quota)),
        }
    }
}

async fn admin_rate_limit_middleware(
    State(limiter): State<AdminRateLimiter>,
    request: Request,
    next: Next,
) -> Response {
    match limiter.limiter.check() {
        Ok(()) => next.run(request).await,
        Err(negative) => {
            let wait = negative.wait_time_from(DefaultClock::default().now());
            let mut builder = Response::builder().status(StatusCode::TOO_MANY_REQUESTS);
            if let Ok(value) = HeaderValue::from_str(&wait.as_secs().max(1).to_string()) {
                builder = builder.header(axum::http::header::RETRY_AFTER, value);
            }
            builder
                .body("admin API rate limited".into())
                .unwrap_or_else(|err| {
                    (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()).into_response()
                })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use tower::ServiceExt;

    use crate::config::{AdminConfig, SecureApiKey, ServerConfig};
    use crate::platform;
    use crate::tcp_proxy::TcpTunnelRegistry;
    use crate::tunnel_manager::TunnelManager;
    use expose_common::error::ConfigError;
    use std::time::Instant;
    fn test_state(config: ServerConfig) -> Arc<AppState> {
        let (per_minute, burst) = config.rate_limit_config();
        let manager = Arc::new(TunnelManager::new(
            config.domain.clone(),
            config.limits(),
            per_minute,
            burst,
            config.pending_requests.clone(),
            config.limits.max_tunnels,
            config.limits.max_tunnels_per_key,
        ));
        let config = Arc::new(config);
        let tcp_registry = Arc::new(TcpTunnelRegistry::new(&config.tcp_forward));
        Arc::new(AppState {
            config,
            manager,
            tcp_registry,
            platform_caps: platform::PlatformCapabilities {
                io_uring_available: false,
                io_uring_version: None,
            },
            started_at: Instant::now(),
        })
    }

    #[tokio::test]
    async fn test_admin_disabled_returns_403() {
        let mut config = ServerConfig::default();
        config.admin = AdminConfig::default();
        let app = crate::admin::router(test_state(config));
        let response = app
            .oneshot(Request::get("/tunnels").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn test_admin_missing_token_returns_401() {
        let mut config = ServerConfig::default();
        config.admin.token = Some(SecureApiKey::new("test-admin-token-123456").unwrap());
        let app = crate::admin::router(test_state(config));
        let response = app
            .oneshot(Request::get("/tunnels").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn test_admin_valid_token_succeeds() {
        let mut config = ServerConfig::default();
        config.admin.token = Some(SecureApiKey::new("test-admin-token-123456").unwrap());
        let app = crate::admin::router(test_state(config));
        let response = app
            .oneshot(
                Request::get("/tunnels")
                    .header("Authorization", "Bearer test-admin-token-123456")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_admin_insecure_mode_allows_without_token() {
        let mut config = ServerConfig::default();
        config.admin.insecure_admin = true;
        let app = crate::admin::router(test_state(config));
        let response = app
            .oneshot(Request::get("/tunnels").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[test]
    fn test_admin_config_validation_rejects_insecure_production() {
        let config = AdminConfig {
            token: None,
            insecure_admin: true,
            bind_address: None,
            rate_limit_per_minute: 60,
        };
        let result = config.validate(true);
        assert!(matches!(result, Err(ConfigError::Validation(_))));
    }
}

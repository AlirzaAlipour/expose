use crate::server::AppState;
use axum::{extract::Extension, http::StatusCode, response::IntoResponse, Json};
use serde::Serialize;
use std::sync::Arc;

#[derive(Debug, Serialize)]
pub struct HealthResponse {
    pub status: HealthStatus,
    pub version: &'static str,
    pub uptime_secs: u64,
    pub tunnels: TunnelHealth,
    pub requests: RequestHealth,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum HealthStatus {
    Healthy,
    Degraded,
    Unhealthy,
}

#[derive(Debug, Serialize)]
pub struct TunnelHealth {
    pub active: usize,
    pub limit: usize,
    pub utilization_percent: f32,
}

#[derive(Debug, Serialize)]
pub struct RequestHealth {
    pub pending: usize,
    pub limit: usize,
    pub utilization_percent: f32,
}

impl HealthResponse {
    pub fn compute(state: &AppState) -> Self {
        let active_tunnels = state.manager.active_tunnel_count();
        let tunnel_limit = state.config.limits.max_tunnels;
        let pending_requests = state.manager.pending_request_count();
        let pending_limit = state.config.limits.max_pending_requests;

        let tunnel_util = if tunnel_limit > 0 {
            (active_tunnels as f32 / tunnel_limit as f32) * 100.0
        } else {
            0.0
        };

        let request_util = if pending_limit > 0 {
            (pending_requests as f32 / pending_limit as f32) * 100.0
        } else {
            0.0
        };

        let status = if tunnel_util > 90.0 || request_util > 90.0 {
            HealthStatus::Unhealthy
        } else if tunnel_util > 70.0 || request_util > 70.0 {
            HealthStatus::Degraded
        } else {
            HealthStatus::Healthy
        };

        Self {
            status,
            version: env!("CARGO_PKG_VERSION"),
            uptime_secs: state.started_at.elapsed().as_secs(),
            tunnels: TunnelHealth {
                active: active_tunnels,
                limit: tunnel_limit,
                utilization_percent: tunnel_util,
            },
            requests: RequestHealth {
                pending: pending_requests,
                limit: pending_limit,
                utilization_percent: request_util,
            },
        }
    }
}

pub async fn health_check(Extension(state): Extension<Arc<AppState>>) -> impl IntoResponse {
    let health = HealthResponse::compute(&state);
    let status_code = match health.status {
        HealthStatus::Healthy | HealthStatus::Degraded => StatusCode::OK,
        HealthStatus::Unhealthy => StatusCode::SERVICE_UNAVAILABLE,
    };
    (status_code, Json(health))
}

pub async fn readiness_check(Extension(state): Extension<Arc<AppState>>) -> impl IntoResponse {
    let active = state.manager.active_tunnel_count();
    let limit = state.config.limits.max_tunnels;

    if limit > 0 && active >= limit {
        (StatusCode::SERVICE_UNAVAILABLE, "at capacity")
    } else {
        (StatusCode::OK, "ready")
    }
}

pub async fn liveness_check() -> impl IntoResponse {
    (StatusCode::OK, "alive")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ServerConfig;
    use crate::platform;
    use crate::server::AppState;
    use crate::tcp_proxy::TcpTunnelRegistry;
    use crate::tunnel_manager::{ActiveTunnel, OutgoingFrame, TunnelManager};
    use axum::extract::Extension;
    use axum::response::IntoResponse;
    use expose_common::types::TunnelProtocol;
    use serde_json::Value;
    use std::sync::Arc;
    use std::time::Instant;
    use tokio::sync::mpsc;
    use uuid::Uuid;

    fn build_state(config: ServerConfig) -> Arc<AppState> {
        let (per_minute, burst) = config.rate_limit_config();
        let domain = config.domain.clone();
        let limits = config.limits();
        let pending = config.pending_requests.clone();
        let max_tunnels = config.limits.max_tunnels;
        let max_per_key = config.limits.max_tunnels_per_key;
        let manager = Arc::new(TunnelManager::new(
            domain,
            limits,
            per_minute,
            burst,
            pending,
            max_tunnels,
            max_per_key,
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

    fn register_tunnel(state: &Arc<AppState>, name: &str) -> Arc<ActiveTunnel> {
        let (tx, _rx): (mpsc::Sender<OutgoingFrame>, _) = mpsc::channel(1);
        state
            .manager
            .register_tunnel(Uuid::new_v4(), name.into(), TunnelProtocol::Http, tx, None)
            .expect("tunnel registered")
    }

    #[tokio::test]
    async fn compute_reports_healthy_with_capacity_available() {
        let state = build_state(ServerConfig::default());
        let health = HealthResponse::compute(&state);
        assert!(matches!(health.status, HealthStatus::Healthy));
        assert_eq!(health.tunnels.active, 0);
        assert_eq!(health.requests.pending, 0);
    }

    #[tokio::test]
    async fn compute_reports_degraded_when_pending_utilization_high() {
        let mut config = ServerConfig::default();
        config.limits.max_pending_requests = 10;
        config.limits.max_tunnels = 2;
        let state = build_state(config);
        let tunnel = register_tunnel(&state, "alpha");
        for _ in 0..8 {
            let _ = state
                .manager
                .register_pending_request(&tunnel.id, Uuid::new_v4(), None)
                .unwrap();
        }

        let health = HealthResponse::compute(&state);
        assert!(matches!(health.status, HealthStatus::Degraded));
        assert_eq!(health.requests.pending, 8);
        assert_eq!(
            health.requests.limit,
            state.config.limits.max_pending_requests
        );
    }

    #[tokio::test]
    async fn compute_reports_unhealthy_when_tunnels_saturated() {
        let mut config = ServerConfig::default();
        config.limits.max_tunnels = 1;
        let state = build_state(config);
        let _ = register_tunnel(&state, "alpha");
        let health = HealthResponse::compute(&state);
        assert!(matches!(health.status, HealthStatus::Unhealthy));
        assert_eq!(health.tunnels.active, 1);
        assert_eq!(health.tunnels.limit, 1);
    }

    #[tokio::test]
    async fn health_check_surface_status_code() {
        let mut config = ServerConfig::default();
        config.limits.max_tunnels = 1;
        let state = build_state(config);
        let _ = register_tunnel(&state, "alpha");
        let response = health_check(Extension(state.clone())).await.into_response();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let body_bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let body: Value = serde_json::from_slice(&body_bytes).unwrap();
        assert_eq!(body["status"], "unhealthy");
    }

    #[tokio::test]
    async fn readiness_check_reflects_capacity() {
        let mut config = ServerConfig::default();
        config.limits.max_tunnels = 1;
        let state = build_state(config);
        let ready = readiness_check(Extension(state.clone()))
            .await
            .into_response();
        assert_eq!(ready.status(), StatusCode::OK);

        let _ = register_tunnel(&state, "alpha");
        let not_ready = readiness_check(Extension(state.clone()))
            .await
            .into_response();
        assert_eq!(not_ready.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn liveness_check_always_ok() {
        let response = liveness_check().await.into_response();
        assert_eq!(response.status(), StatusCode::OK);
    }
}

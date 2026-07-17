//! Path-based routing handler.
//!
//! Routes requests like `GET /t/demo/api/users` to tunnel `demo` and forwards
//! the rewritten path (e.g., `/api/users`) to the tunnel client.

use crate::error::{ExposeError, Result};
use crate::metrics::ServerMetrics;
use crate::proxy::{
    maybe_inject_base_tag, rewrite_response_headers, BodyCollector, MessageEncoder, RequestLimiter,
    RequestStreamer, ResponseBuilder,
};
use crate::server::AppState;
use axum::body::{Body, HttpBody};
use axum::extract::{Extension, Path};
use axum::http::{header::CONTENT_LENGTH, Request};
use axum::response::Response;
use expose_common::protocol::Message;
use expose_common::utils::sanitize_subdomain;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time::timeout;
use tracing::{debug, field::display, instrument, Span};
use uuid::Uuid;

/// Path parameters extracted by Axum.
#[derive(Debug, serde::Deserialize)]
pub struct PathParams {
    /// Tunnel name extracted from the path prefix.
    pub tunnel_name: String,
    /// Remaining path after the tunnel name (may be empty).
    #[serde(default)]
    pub rest: String,
}

/// Handles path-based tunnel routing requests.
#[instrument(
    skip(state, request),
    fields(
        request_id = %Uuid::new_v4(),
        tunnel = tracing::field::Empty,
        method = tracing::field::Empty,
        forwarded_path = tracing::field::Empty,
    )
)]
pub async fn path_proxy_request(
    Extension(state): Extension<Arc<AppState>>,
    Path(params): Path<PathParams>,
    request: Request<Body>,
) -> Result<Response> {
    path_proxy_with_state(state, params, request).await
}

async fn path_proxy_with_state(
    state: Arc<AppState>,
    params: PathParams,
    request: Request<Body>,
) -> Result<Response> {
    let span = Span::current();
    span.record("method", display(request.method()));
    let trimmed = params.tunnel_name.trim();
    if trimmed
        .chars()
        .any(|c| !(c.is_ascii_alphanumeric() || c == '-'))
    {
        return Err(ExposeError::InvalidSubdomain {
            subdomain: params.tunnel_name.clone(),
            reason: "invalid tunnel name".into(),
        });
    }
    let tunnel_name = sanitize_subdomain(trimmed).ok_or_else(|| ExposeError::InvalidSubdomain {
        subdomain: params.tunnel_name.clone(),
        reason: "invalid tunnel name".into(),
    })?;
    span.record("tunnel", display(&tunnel_name));

    let query = request.uri().query().map(|q| q.to_string());
    let forwarded_path = build_forwarded_path(&params.rest, query.as_deref());
    span.record("forwarded_path", display(&forwarded_path));
    debug!(%tunnel_name, %forwarded_path, "routing via path prefix");

    let tunnel = state
        .manager
        .get(&tunnel_name)
        .ok_or_else(|| ExposeError::TunnelNotFound {
            identifier: tunnel_name.clone(),
        })?;

    RequestLimiter::check(&tunnel)?;
    ServerMetrics::request_started(&tunnel_name);

    let started = Instant::now();
    let (parts, body) = request.into_parts();
    let content_length = parts
        .headers
        .get(CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok());

    let body_collector = BodyCollector::new(state.config.request_body_limit_bytes);
    let streaming_cfg = &state.config.streaming;
    let size_hint = body.size_hint();
    let should_stream = streaming_cfg.enabled
        && content_length
            .map(|len| len as usize >= streaming_cfg.threshold_bytes)
            .or_else(|| {
                size_hint
                    .upper()
                    .map(|upper| (upper as usize) >= streaming_cfg.threshold_bytes)
            })
            .unwrap_or(false);

    let timeout_duration = Duration::from_secs(state.config.request_timeout_secs);
    let request_id = Uuid::new_v4();
    let response_rx =
        state
            .manager
            .register_pending_request(&tunnel.id, request_id, Some(timeout_duration))?;

    let result = async {
        if should_stream {
            RequestStreamer::new(&tunnel, request_id, streaming_cfg)
                .stream_with_path(&parts, body, content_length, Some(&forwarded_path))
                .await?;
        } else {
            let collected = body_collector.collect(body).await?;
            let len = collected.len();
            let message = MessageEncoder::encode_request_with_path(
                request_id,
                &parts,
                &forwarded_path,
                collected,
            )?;
            tunnel.send(message).await?;
            ServerMetrics::bytes_sent(&tunnel.subdomain, len);
        }

        let response = timeout(timeout_duration, response_rx)
            .await
            .map_err(|_| ExposeError::Timeout {
                operation: "tunnel response".into(),
                timeout_secs: timeout_duration.as_secs(),
            })?
            .map_err(|_| ExposeError::TunnelDisconnected { reason: None })?;

        let response = match response {
            Message::HttpResponse { .. } => response,
            other => {
                return Err(ExposeError::InvalidMessage {
                    context: format!("expected HttpResponse, got {other:?}"),
                })
            }
        };
        let response = rewrite_response_headers(response, &state.config.path_prefix, &tunnel_name);
        let response = maybe_inject_base_tag(response, &state.config.path_prefix, &tunnel_name);
        let response = ResponseBuilder::from_message(response)?;

        tracing::info!(
            status = response.status().as_u16(),
            duration_ms = started.elapsed().as_millis(),
            tunnel = %tunnel_name,
            "Path request completed"
        );

        Ok(response)
    }
    .await;

    match &result {
        Ok(response) => ServerMetrics::request_completed(
            &tunnel_name,
            response.status().as_u16(),
            started.elapsed(),
        ),
        Err(err) => ServerMetrics::request_failed(&tunnel_name, err),
    }

    result
}

fn build_forwarded_path(rest: &str, query: Option<&str>) -> String {
    let mut path = if rest.is_empty() {
        "/".to_string()
    } else {
        format!("/{}", rest)
    };
    if let Some(q) = query {
        path.push('?');
        path.push_str(q);
    }
    path
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_forwarded_path_empty_rest() {
        assert_eq!(build_forwarded_path("", None), "/");
    }

    #[test]
    fn test_forwarded_path_with_segments() {
        assert_eq!(build_forwarded_path("api/users", None), "/api/users");
    }

    #[test]
    fn test_forwarded_path_with_query() {
        assert_eq!(
            build_forwarded_path("api/users", Some("q=1")),
            "/api/users?q=1"
        );
    }
}

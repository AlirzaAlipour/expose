mod body;
mod encoder;
mod host;
mod limiter;
mod response;
mod streaming;

pub use body::BodyCollector;
pub use encoder::MessageEncoder;
pub use host::HostResolver;
pub use limiter::RequestLimiter;
pub use response::ResponseBuilder;
pub use streaming::RequestStreamer;

use crate::error::{ExposeError, Result};
use crate::metrics::ServerMetrics;
use crate::server::AppState;
use axum::body::{Body, HttpBody};
use axum::extract::Extension;
use axum::http::{header::CONTENT_LENGTH, Request};
use axum::response::Response;
use expose_common::protocol::Message;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;
use tokio::time::timeout;
use tracing::field::display;
use tracing::{debug, info, instrument, Span};
use uuid::Uuid;

/// Primary Axum handler for proxied HTTP requests.
#[instrument(
    skip(state, request),
    fields(
        request_id = %Uuid::new_v4(),
        subdomain = tracing::field::Empty,
        method = tracing::field::Empty,
        path = tracing::field::Empty,
    )
)]
pub async fn subdomain_proxy_request(
    Extension(state): Extension<Arc<AppState>>,
    request: Request<Body>,
) -> Result<Response> {
    proxy_with_state(state, request).await
}

pub async fn proxy_with_state(state: Arc<AppState>, request: Request<Body>) -> Result<Response> {
    let span = Span::current();
    span.record("method", display(request.method()));
    span.record("path", display(request.uri().path()));
    debug!("processing proxy request");
    let started = Instant::now();

    let resolver = HostResolver::new(&state.config.domain);
    let subdomain = resolver.resolve(&request)?;
    span.record("subdomain", display(&subdomain));

    let tunnel = state
        .manager
        .get(&subdomain)
        .ok_or_else(|| ExposeError::TunnelNotFound {
            identifier: subdomain.clone(),
        })?;

    RequestLimiter::check(&tunnel)?;
    ServerMetrics::request_started(&subdomain);

    let result = async {
        let request_id = Uuid::new_v4();
        span.record("request_id", display(&request_id));
        let (parts, body) = request.into_parts();
        let content_length = parts
            .headers
            .get(CONTENT_LENGTH)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<u64>().ok());

        let body_bytes = BodyCollector::new(state.config.request_body_limit_bytes);
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
        let response_rx = state.manager.register_pending_request(
            &tunnel.id,
            request_id,
            Some(timeout_duration),
        )?;

        if should_stream {
            RequestStreamer::new(&tunnel, request_id, streaming_cfg)
                .stream(&parts, body, content_length)
                .await?;
        } else {
            let collected = body_bytes.collect(body).await?;
            let len = collected.len();
            let message = MessageEncoder::encode_request(request_id, &parts, collected)?;
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
            Message::HttpResponse { .. } => ResponseBuilder::from_message(response),
            other => Err(ExposeError::InvalidMessage {
                context: format!("expected HttpResponse, got {other:?}"),
            }),
        }?;

        info!(
            status = response.status().as_u16(),
            duration_ms = started.elapsed().as_millis(),
            "Request completed"
        );

        Ok(response)
    }
    .await;

    match &result {
        Ok(response) => ServerMetrics::request_completed(
            &subdomain,
            response.status().as_u16(),
            started.elapsed(),
        ),
        Err(err) => ServerMetrics::request_failed(&subdomain, err),
    }

    result
}

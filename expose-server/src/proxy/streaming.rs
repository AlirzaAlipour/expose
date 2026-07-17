use axum::body::Body;
use axum::http::request::Parts;
use bytes::Bytes;
use http_body_util::BodyExt;

use crate::config::StreamingConfig;
use crate::error::ExposeError;
use crate::metrics::ServerMetrics;
use crate::tunnel_manager::ActiveTunnel;
use expose_common::protocol::Message;
use uuid::Uuid;

/// Streams HTTP request bodies to the tunnel client using chunked messages.
pub struct RequestStreamer<'a> {
    tunnel: &'a ActiveTunnel,
    request_id: Uuid,
    config: &'a StreamingConfig,
}

impl<'a> RequestStreamer<'a> {
    pub fn new(tunnel: &'a ActiveTunnel, request_id: Uuid, config: &'a StreamingConfig) -> Self {
        Self {
            tunnel,
            request_id,
            config,
        }
    }

    pub async fn stream(
        &self,
        parts: &Parts,
        body: Body,
        content_length: Option<u64>,
    ) -> Result<(), ExposeError> {
        self.stream_with_path(parts, body, content_length, None)
            .await
    }

    pub async fn stream_with_path(
        &self,
        parts: &Parts,
        mut body: Body,
        content_length: Option<u64>,
        forwarded_path: Option<&str>,
    ) -> Result<(), ExposeError> {
        let start = Message::HttpRequestStart {
            id: self.request_id,
            method: parts.method.to_string(),
            path: forwarded_path
                .map(|p| p.to_string())
                .or_else(|| parts.uri.path_and_query().map(|pq| pq.as_str().to_string()))
                .unwrap_or_else(|| parts.uri.path().to_string()),
            headers: parts
                .headers
                .iter()
                .filter_map(|(name, value)| {
                    value
                        .to_str()
                        .ok()
                        .map(|v| (name.to_string(), v.to_string()))
                })
                .collect(),
            content_length,
        };
        self.tunnel.send(start).await?;

        let mut total: usize = 0;
        let mut sequence = 0u32;
        while let Some(frame) = body.frame().await {
            let frame = frame.map_err(|err| ExposeError::Internal(err.to_string()))?;
            if let Some(data) = frame.data_ref() {
                total = total.saturating_add(data.len());
                if total > self.config.max_body_bytes {
                    return Err(ExposeError::PayloadTooLarge {
                        size: total,
                        limit: self.config.max_body_bytes,
                    });
                }

                for chunk in data.chunks(self.config.chunk_size_bytes) {
                    let msg = Message::HttpRequestChunk {
                        id: self.request_id,
                        data: Bytes::copy_from_slice(chunk),
                        sequence,
                    };
                    self.tunnel.send(msg).await?;
                    ServerMetrics::bytes_sent(&self.tunnel.subdomain, chunk.len());
                    sequence = sequence.wrapping_add(1);
                }
            }
        }

        self.tunnel
            .send(Message::HttpRequestEnd {
                id: self.request_id,
            })
            .await
    }
}

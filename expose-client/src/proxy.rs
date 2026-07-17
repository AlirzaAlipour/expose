//! Local HTTP proxy that forwards requests to the developer's machine.

use crate::error::{other_io_error, Result};
use bytes::{Bytes, BytesMut};
use dashmap::DashMap;
use expose_common::error::{ConfigError, ExposeError};
use expose_common::protocol::Message;
use hyper::body::to_bytes;
use hyper::client::HttpConnector;
use hyper::{Body, Client, Method, Request, Uri};
use std::sync::Arc;
use tracing::debug;
use uuid::Uuid;

/// HTTP proxy wrapper around `hyper::Client`.
#[derive(Clone)]
pub struct LocalProxy {
    client: Client<HttpConnector, Body>,
    upstream_host: String,
    upstream_port: u16,
    pending_streams: Arc<DashMap<Uuid, StreamingRequest>>,
}

const STREAM_BUFFER_LIMIT: usize = 50 * 1024 * 1024; // 50MB safeguard

struct StreamingRequest {
    method: String,
    path: String,
    headers: Vec<(String, String)>,
    body: BytesMut,
}

type PendingRequestParts = (String, String, Vec<(String, String)>, Bytes);

impl LocalProxy {
    /// Build a new proxy using the provided host and port.
    pub fn new(host: impl Into<String>, port: u16) -> Self {
        let mut connector = HttpConnector::new();
        connector.enforce_http(false);
        let client = Client::builder().build(connector);
        Self {
            client,
            upstream_host: host.into(),
            upstream_port: port,
            pending_streams: Arc::new(DashMap::new()),
        }
    }

    /// Set the Host header on outbound requests if missing.
    fn ensure_host_header(headers: &mut hyper::http::HeaderMap, host: &str) {
        use hyper::header::HOST;
        if !headers.contains_key(HOST) {
            if let Ok(value) = hyper::header::HeaderValue::from_str(host) {
                headers.insert(HOST, value);
            }
        }
    }

    pub fn begin_streaming_request(
        &self,
        id: Uuid,
        method: String,
        path: String,
        headers: Vec<(String, String)>,
    ) {
        self.pending_streams.insert(
            id,
            StreamingRequest {
                method,
                path,
                headers,
                body: BytesMut::new(),
            },
        );
    }

    pub fn push_streaming_chunk(&self, id: &Uuid, data: Bytes) -> Result<()> {
        let mut entry =
            self.pending_streams
                .get_mut(id)
                .ok_or_else(|| ExposeError::InvalidMessage {
                    context: format!("chunk received for unknown request {id}"),
                })?;
        if entry.body.len() + data.len() > STREAM_BUFFER_LIMIT {
            return Err(ExposeError::PayloadTooLarge {
                size: entry.body.len() + data.len(),
                limit: STREAM_BUFFER_LIMIT,
            });
        }
        entry.body.extend_from_slice(data.as_ref());
        Ok(())
    }

    pub fn finish_streaming_request(&self, id: &Uuid) -> Option<PendingRequestParts> {
        self.pending_streams.remove(id).map(|(_, request)| {
            (
                request.method,
                request.path,
                request.headers,
                request.body.freeze(),
            )
        })
    }

    /// Forward the HTTP request to the upstream server and return a protocol response message.
    pub async fn handle_http_request(
        &self,
        id: Uuid,
        method: String,
        path: String,
        headers: Vec<(String, String)>,
        body: Bytes,
    ) -> Result<Message> {
        let method = method.parse::<Method>().map_err(|err| {
            ExposeError::from(ConfigError::Validation(format!(
                "invalid HTTP method: {err}"
            )))
        })?;

        let uri_string = format!(
            "http://{}:{}{}",
            self.upstream_host, self.upstream_port, path
        );
        let uri: Uri = uri_string.parse().map_err(|err| {
            ExposeError::from(ConfigError::Validation(format!(
                "invalid request URI: {err}"
            )))
        })?;

        let mut builder = Request::builder();
        builder = builder.method(method).uri(uri);
        {
            let headers_map = builder
                .headers_mut()
                .ok_or_else(|| ExposeError::Network(other_io_error("unable to mutate headers")))?;
            for (name, value) in headers {
                if let (Ok(name), Ok(value)) = (
                    hyper::header::HeaderName::from_bytes(name.as_bytes()),
                    hyper::header::HeaderValue::from_str(&value),
                ) {
                    headers_map.append(name, value);
                }
            }
            Self::ensure_host_header(
                headers_map,
                &format!("{}:{}", self.upstream_host, self.upstream_port),
            );
        }

        let request = builder
            .body(Body::from(body))
            .map_err(|err| ExposeError::Network(other_io_error(err.to_string())))?;

        debug!("forwarding request to local service");
        let response = self.client.request(request).await?;
        let status = response.status().as_u16();
        let headers_vec = response
            .headers()
            .iter()
            .filter_map(|(name, value)| {
                Some((name.as_str().to_string(), value.to_str().ok()?.to_string()))
            })
            .collect::<Vec<_>>();
        let body_bytes = to_bytes(response.into_body()).await?;

        Ok(Message::HttpResponse {
            id,
            status,
            headers: headers_vec,
            body: body_bytes,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn streaming_request_buffering_roundtrip() {
        let proxy = LocalProxy::new("127.0.0.1", 8080);
        let id = Uuid::new_v4();
        proxy.begin_streaming_request(id, "POST".into(), "/data".into(), vec![]);
        proxy
            .push_streaming_chunk(&id, Bytes::from_static(b"hello"))
            .unwrap();
        proxy
            .push_streaming_chunk(&id, Bytes::from_static(b" world"))
            .unwrap();
        let parts = proxy.finish_streaming_request(&id).expect("finish");
        assert_eq!(parts.0, "POST");
        assert_eq!(parts.1, "/data");
        assert_eq!(parts.3.as_ref(), b"hello world");
    }
}

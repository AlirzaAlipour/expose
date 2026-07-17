use crate::error::ExposeError;
use axum::http::request::Parts;
use bytes::Bytes;
use expose_common::protocol::Message;
use uuid::Uuid;

/// Encodes HTTP requests into protocol messages.
pub struct MessageEncoder;

impl MessageEncoder {
    pub fn encode_request(id: Uuid, parts: &Parts, body: Bytes) -> Result<Message, ExposeError> {
        let path = parts
            .uri
            .path_and_query()
            .map(|pq| pq.as_str().to_string())
            .unwrap_or_else(|| parts.uri.path().to_string());

        let headers = parts
            .headers
            .iter()
            .filter_map(|(name, value)| Some((name.to_string(), value.to_str().ok()?.to_string())))
            .collect::<Vec<_>>();

        Ok(Message::HttpRequest {
            id,
            method: parts.method.to_string(),
            path,
            headers,
            body,
        })
    }
}

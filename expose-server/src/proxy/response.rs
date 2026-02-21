use axum::body::Body;
use axum::http::{header::HeaderName, header::HeaderValue, StatusCode};
use axum::response::Response;
use expose_common::protocol::Message;

use crate::error::ExposeError;

pub struct ResponseBuilder;

impl ResponseBuilder {
    pub fn from_message(message: Message) -> Result<Response, ExposeError> {
        match message {
            Message::HttpResponse {
                status,
                headers,
                body,
                ..
            } => {
                let mut builder = Response::builder()
                    .status(StatusCode::from_u16(status).unwrap_or(StatusCode::OK));
                if let Some(map) = builder.headers_mut() {
                    for (name, value) in headers {
                        if let (Ok(name), Ok(value)) =
                            (name.parse::<HeaderName>(), value.parse::<HeaderValue>())
                        {
                            map.append(name, value);
                        }
                    }
                }
                builder
                    .body(Body::from(body))
                    .map_err(|err| ExposeError::Internal(err.to_string()))
            }
            other => Err(ExposeError::InvalidMessage {
                context: format!("expected HttpResponse, got {other:?}"),
            }),
        }
    }
}

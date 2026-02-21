use axum::body::{Body, Bytes, HttpBody};
use http_body_util::BodyExt;

use crate::error::ExposeError;

/// Collects request bodies with streaming limit enforcement.
pub struct BodyCollector {
    limit: usize,
}

impl BodyCollector {
    pub fn new(limit: usize) -> Self {
        Self { limit }
    }

    pub async fn collect(&self, mut body: Body) -> Result<Bytes, ExposeError> {
        if let Some(upper) = body.size_hint().upper() {
            if (upper as usize) > self.limit {
                return Err(ExposeError::PayloadTooLarge {
                    size: upper as usize,
                    limit: self.limit,
                });
            }
        }

        let mut collected = Vec::new();
        while let Some(frame) = body.frame().await {
            let frame = frame.map_err(|err| ExposeError::Internal(err.to_string()))?;
            if let Some(data) = frame.data_ref() {
                if collected.len() + data.len() > self.limit {
                    return Err(ExposeError::PayloadTooLarge {
                        size: collected.len() + data.len(),
                        limit: self.limit,
                    });
                }
                collected.extend_from_slice(data);
            }
        }

        Ok(Bytes::from(collected))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn collect_body_within_limit_succeeds() {
        let collector = BodyCollector::new(1024);
        let bytes = collector.collect(Body::from("hello world")).await.unwrap();
        assert_eq!(bytes.as_ref(), b"hello world");
    }

    #[tokio::test]
    async fn collect_body_exceeds_limit_returns_error() {
        let collector = BodyCollector::new(5);
        let err = collector.collect(Body::from("too long")).await.unwrap_err();
        match err {
            ExposeError::PayloadTooLarge { limit, .. } => assert_eq!(limit, 5),
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[tokio::test]
    async fn collect_body_empty_succeeds() {
        let collector = BodyCollector::new(10);
        let bytes = collector.collect(Body::empty()).await.unwrap();
        assert!(bytes.is_empty());
    }
}

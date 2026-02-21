use crate::error::ExposeError;
use axum::extract::Request;
use axum::http::header::HOST;

/// Resolves subdomains from Host headers.
pub struct HostResolver<'a> {
    domain: &'a str,
}

impl<'a> HostResolver<'a> {
    pub fn new(domain: &'a str) -> Self {
        Self { domain }
    }

    pub fn resolve(&self, request: &Request) -> Result<String, ExposeError> {
        let host = request
            .headers()
            .get(HOST)
            .and_then(|h| h.to_str().ok())
            .ok_or_else(|| ExposeError::InvalidMessage {
                context: "missing Host header".into(),
            })?;

        let host = host.split(':').next().unwrap_or(host);
        let suffix = format!(".{}", self.domain);
        if !host.ends_with(&suffix) {
            return Err(ExposeError::TunnelNotFound {
                identifier: format!("host '{host}' doesn't match domain '{}'", self.domain),
            });
        }

        let subdomain = &host[..host.len() - suffix.len()];
        if subdomain.is_empty() {
            return Err(ExposeError::InvalidSubdomain {
                subdomain: subdomain.to_string(),
                reason: "empty subdomain".into(),
            });
        }

        Ok(subdomain.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;

    #[test]
    fn resolves_valid_host() {
        let resolver = HostResolver::new("example.com");
        let request = Request::builder()
            .header("Host", "demo.example.com")
            .body(Body::empty())
            .unwrap();
        assert_eq!(resolver.resolve(&request).unwrap(), "demo");
    }

    #[test]
    fn rejects_missing_host() {
        let resolver = HostResolver::new("example.com");
        let request = Request::builder().body(Body::empty()).unwrap();
        assert!(matches!(
            resolver.resolve(&request),
            Err(ExposeError::InvalidMessage { .. })
        ));
    }
}

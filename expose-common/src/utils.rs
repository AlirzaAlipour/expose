//! Miscellaneous helpers shared by multiple crates.

use crate::error::{ExposeError, Result};

/// Ensure the payload size does not exceed the allowed limit.
pub fn validate_body_size(size: usize, limit: usize) -> Result<()> {
    if size > limit {
        Err(ExposeError::PayloadTooLarge { size, limit })
    } else {
        Ok(())
    }
}

/// Basic sanitization for requested subdomains, ensuring we only keep
/// alphanumeric characters and dashes.
pub fn sanitize_subdomain(raw: &str) -> Option<String> {
    let normalized = raw
        .trim()
        .to_ascii_lowercase()
        .chars()
        .filter(|c| c.is_ascii_alphanumeric() || *c == '-')
        .collect::<String>();

    if normalized.is_empty() || normalized.len() > 63 {
        return None;
    }

    // Subdomains cannot start or end with dashes.
    if normalized.starts_with('-') || normalized.ends_with('-') {
        return None;
    }

    Some(normalized)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_size() {
        assert!(validate_body_size(100, 200).is_ok());
        assert!(validate_body_size(200, 100).is_err());
    }

    #[test]
    fn sanitizes_subdomain() {
        assert_eq!(sanitize_subdomain(" My-App "), Some("my-app".into()));
        assert!(sanitize_subdomain("---").is_none());
    }
}

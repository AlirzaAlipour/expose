//! Helpers for loading TLS configuration from PEM files.

use crate::config::ServerConfig;
use crate::error::{ExposeError, Result};
use axum_server::tls_rustls::RustlsConfig;
use expose_common::error::ConfigError;
use std::path::Path;
use tokio::fs;

/// Load a `RustlsConfig` from the certificate and key paths provided in [`ServerConfig`].
pub async fn load_rustls_config(config: &ServerConfig) -> Result<RustlsConfig> {
    let (cert_path, key_path) = config.tls_paths()?;
    ensure_exists(cert_path, "certificate").await?;
    ensure_exists(key_path, "private key").await?;

    RustlsConfig::from_pem_file(cert_path, key_path)
        .await
        .map_err(|err| {
            ExposeError::Config(ConfigError::Validation(format!(
                "failed to parse TLS materials: {err}"
            )))
        })
}

async fn ensure_exists(path: &Path, label: &str) -> Result<()> {
    fs::metadata(path).await.map_err(|err| {
        ExposeError::Config(ConfigError::Validation(format!(
            "failed to read {label} file ({}): {err}",
            path.display()
        )))
    })?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[tokio::test]
    async fn tls_paths_must_exist() {
        let mut config = ServerConfig::default();
        config.tls_enabled = true;
        config.https_bind_address = Some("127.0.0.1:8443".into());
        config.tls_cert_path = Some(PathBuf::from("/tmp/missing-cert.pem"));
        config.tls_key_path = Some(PathBuf::from("/tmp/missing-key.pem"));

        let err = load_rustls_config(&config).await.unwrap_err();
        match err {
            ExposeError::Config(inner) => {
                assert!(inner.to_string().contains("failed to read certificate"));
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }
}

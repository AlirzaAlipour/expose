use anyhow::Result;
use expose_client::cli::{Cli, Command};
use expose_client::config::build_runtime_config;
use tempfile::NamedTempFile;

fn base_cli() -> Cli {
    Cli {
        config: None,
        server: Some("ws://127.0.0.1:8080".into()),
        api_key: None,
        subdomain: None,
        reconnect_attempts: None,
        reconnect_base_delay_ms: None,
        command: Command::Http {
            port: 8080,
            host: "127.0.0.1".into(),
        },
    }
}

#[test]
fn rejects_invalid_subdomain_characters() {
    let mut cli = base_cli();
    cli.subdomain = Some("---".into());

    let result = build_runtime_config(&cli);
    assert!(result.is_err());
}

#[test]
fn merges_cli_and_file_config_correctly() -> Result<()> {
    let mut temp = NamedTempFile::new()?;
    std::io::Write::write_all(
        &mut temp,
        br#"
        server_url = "wss://production.example.com"
        api_key = "file-key"
        local_port = 3000
    "#,
    )?;

    let mut cli = base_cli();
    cli.config = Some(temp.path().to_path_buf());
    cli.server = None; // use file value
    cli.api_key = Some("cli-key".into());
    cli.command = Command::Http {
        port: 8080,
        host: "127.0.0.1".into(),
    };

    let merged = build_runtime_config(&cli)?;
    assert_eq!(merged.local_port, 8080);
    assert_eq!(merged.server_url, "wss://production.example.com");
    assert_eq!(merged.api_key.as_deref(), Some("cli-key"));
    Ok(())
}

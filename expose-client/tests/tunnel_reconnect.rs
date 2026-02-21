use anyhow::{anyhow, Result};
use expose_client::tunnel::run_with_token;
use expose_common::protocol::Message;
use helpers::fixtures::config_for;
use helpers::mock_server::{MockTunnelServer, ResponseBehavior};
use helpers::{init_tracing, with_timeout, DEFAULT_TEST_TIMEOUT, SLOW_TIMEOUT};
use tokio::time::Duration;
use tokio_util::sync::CancellationToken;

mod helpers;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn client_reconnects_after_disconnect() -> Result<()> {
    init_tracing();

    let mock_server = MockTunnelServer::start(ResponseBehavior::Normal).await?;
    let addr = mock_server.addr();

    let mut config = config_for(&format!("ws://{}", addr), 0);
    config.reconnect_max_attempts = 0; // infinite reconnects for the test
    config.reconnect_base_delay_ms = 100;

    let shutdown = CancellationToken::new();
    let client_handle = tokio::spawn(run_with_token(config, shutdown.clone()));

    with_timeout(
        "initial connect",
        DEFAULT_TEST_TIMEOUT,
        mock_server.wait_for_message(DEFAULT_TEST_TIMEOUT, |m| matches!(m, Message::Connect(_))),
    )
    .await?;

    mock_server.shutdown().await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(
        !client_handle.is_finished(),
        "client should still be attempting to reconnect after a disconnect"
    );

    shutdown.cancel();
    client_handle.abort();
    let _ = client_handle.await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn client_times_out_on_unresponsive_server() -> Result<()> {
    init_tracing();
    let mock_server = MockTunnelServer::start(ResponseBehavior::SilentDuringHandshake).await?;

    let mut config = config_for(&format!("ws://{}", mock_server.addr()), 0);
    config.reconnect_max_attempts = 1;
    config.reconnect_base_delay_ms = 100;

    let shutdown = CancellationToken::new();
    let client_handle = tokio::spawn(run_with_token(config, shutdown));

    let result = with_timeout(
        "client fails fast when handshake silent",
        SLOW_TIMEOUT,
        async {
            match client_handle.await {
                Ok(inner) => inner.map_err(|err| anyhow!(err.to_string())),
                Err(err) => Err(anyhow!(format!("client task panicked: {err}"))),
            }
        },
    )
    .await;

    assert!(result.is_err(), "client should error when server is silent");

    mock_server.shutdown().await;
    Ok(())
}

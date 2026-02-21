use anyhow::{bail, Result};
use bytes::Bytes;
use expose_client::tunnel::run_with_token;
use expose_common::protocol::Message;
use helpers::fixtures::{config_for, start_test_http_server};
use helpers::mock_server::{MockTunnelServer, ResponseBehavior};
use helpers::{init_tracing, with_timeout, DEFAULT_TEST_TIMEOUT, FAST_TIMEOUT};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

mod helpers;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn client_forwards_http_requests() -> Result<()> {
    init_tracing();

    let http_server = start_test_http_server().await?;
    let mock_server = MockTunnelServer::start(ResponseBehavior::Normal).await?;

    let mut config = config_for(
        &format!("ws://{}", mock_server.addr()),
        http_server.addr().port(),
    );
    config.reconnect_max_attempts = 1;

    let shutdown = CancellationToken::new();
    let client_handle = tokio::spawn(run_with_token(config, shutdown.clone()));

    with_timeout(
        "wait for connect message",
        FAST_TIMEOUT,
        mock_server.wait_for_message(DEFAULT_TEST_TIMEOUT, |m| matches!(m, Message::Connect(_))),
    )
    .await?;

    let request_id = Uuid::new_v4();
    mock_server
        .send(Message::HttpRequest {
            id: request_id,
            method: "GET".into(),
            path: "/".into(),
            headers: vec![("host".into(), format!("{}", http_server.addr()))],
            body: Bytes::new(),
        })
        .await?;

    let recorded = http_server.wait_for_request(DEFAULT_TEST_TIMEOUT).await?;
    assert_eq!(recorded.method, "GET");
    assert_eq!(recorded.path, "/");
    assert!(recorded.body.is_empty());

    let response = mock_server
        .wait_for_message(
            DEFAULT_TEST_TIMEOUT,
            |m| matches!(m, Message::HttpResponse { id, .. } if *id == request_id),
        )
        .await?;

    match response {
        Message::HttpResponse { status, body, .. } => {
            assert_eq!(status, 200);
            assert_eq!(body, Bytes::from_static(b"ok"));
        }
        other => bail!("expected HttpResponse, got {other:?}"),
    }

    shutdown.cancel();
    mock_server.shutdown().await;
    client_handle.abort();
    let _ = client_handle.await;

    http_server.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn client_handles_shutdown_token() -> Result<()> {
    init_tracing();
    let mock_server = MockTunnelServer::start(ResponseBehavior::Normal).await?;

    let mut config = config_for(&format!("ws://{}", mock_server.addr()), 0);
    config.reconnect_max_attempts = 1;

    let shutdown = CancellationToken::new();
    let client_handle = tokio::spawn(run_with_token(config, shutdown.clone()));

    with_timeout(
        "wait for connect message",
        DEFAULT_TEST_TIMEOUT,
        mock_server.wait_for_message(DEFAULT_TEST_TIMEOUT, |m| matches!(m, Message::Connect(_))),
    )
    .await?;

    shutdown.cancel();
    mock_server.shutdown().await;
    client_handle.abort();
    let _ = client_handle.await;
    Ok(())
}

use bytes::Bytes;
use expose_common::protocol::{ConnectRequest, Message, PROTOCOL_VERSION};
use expose_common::types::TcpTuningConfig;
use expose_common::types::TunnelProtocol;
use expose_server::config::SecureApiKey;
use futures_util::{SinkExt, StreamExt};
use helpers::fixtures;
use helpers::mock_client::MockTunnelClient;
use helpers::test_server::TestServer;
use hyper::body::to_bytes;
use hyper::{Body, Client, Request, StatusCode};
use serial_test::serial;
use tokio::time::{Duration, Instant};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message as WsMessage;

mod helpers;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn websocket_handshake_accepts_valid_api_key() {
    let mut config = fixtures::base_config();
    config.api_keys = vec![SecureApiKey::new("top-secret-secret-123456").unwrap()];

    let server = TestServer::start(config).await;
    let client = MockTunnelClient::connect(
        &server.websocket_url(),
        Some("demo"),
        Some("top-secret-secret-123456"),
    )
    .await;

    assert_eq!(client.assignment.assigned_subdomain, "demo");
    assert_eq!(client.assignment.domain, "test.localhost");

    client.shutdown().await;
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn websocket_handshake_rejects_invalid_api_key() {
    let mut config = fixtures::base_config();
    config.api_keys = vec![SecureApiKey::new("correct-key-secret-123456").unwrap()];

    let server = TestServer::start(config).await;
    let (mut ws, _) = connect_async(server.websocket_url())
        .await
        .expect("connect websocket");

    let request = ConnectRequest {
        protocol_version: PROTOCOL_VERSION,
        api_key: Some("wrong".into()),
        desired_subdomain: Some("ignored".into()),
        tunnel_protocol: TunnelProtocol::Http,
        client_version: "test-suite".into(),
        metadata: None,
    };

    ws.send(WsMessage::Binary(
        Message::Connect(request).encode().unwrap(),
    ))
    .await
    .expect("send connect");

    match ws.next().await {
        Some(Ok(WsMessage::Binary(frame))) => {
            match expose_common::protocol::Message::decode(&frame) {
                Ok(expose_common::protocol::Message::Error { code, .. }) => {
                    assert_eq!(
                        code,
                        expose_common::protocol::ErrorCode::AuthenticationFailed
                    );
                }
                other => panic!("expected error message, got {other:?}"),
            }
        }
        other => panic!("expected error frame, got {other:?}"),
    }

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn http_request_is_proxied_and_response_returned() {
    let config = fixtures::base_config();
    let server = TestServer::start(config).await;

    let mut tunnel = MockTunnelClient::connect(&server.websocket_url(), Some("proxy"), None).await;
    let host = format!(
        "{}.{}",
        tunnel.assignment.assigned_subdomain, tunnel.assignment.domain
    );

    let client = Client::new();
    let request = Request::builder()
        .method("POST")
        .uri(server.http_url("/echo"))
        .header("Host", host)
        .body(Body::from("payload"))
        .expect("http request");
    let response_task =
        tokio::spawn(async move { client.request(request).await.expect("response") });

    let inbound = tunnel.expect_http_request().await;
    assert_eq!(inbound.path, "/echo");
    assert_eq!(inbound.method, "POST");
    assert_eq!(inbound.body, Bytes::from_static(b"payload"));
    assert!(inbound
        .headers
        .iter()
        .any(|(name, _)| name.eq_ignore_ascii_case("host")));

    tunnel
        .send_http_response(
            inbound.id,
            201,
            vec![("x-expose".into(), "ok".into())],
            Bytes::from_static(b"from tunnel"),
        )
        .await;

    let response = response_task.await.expect("task");
    assert_eq!(response.status(), StatusCode::CREATED);
    let body = to_bytes(response.into_body()).await.expect("body");
    assert_eq!(body.as_ref(), b"from tunnel");

    tunnel.shutdown().await;
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn timeout_for_unresponsive_tunnel_returns_504() {
    let mut config = fixtures::base_config();
    config.request_timeout_secs = 1;
    let server = TestServer::start(config).await;

    let mut tunnel = MockTunnelClient::connect(&server.websocket_url(), Some("slow"), None).await;
    let host = format!(
        "{}.{}",
        tunnel.assignment.assigned_subdomain, tunnel.assignment.domain
    );

    let client = Client::new();
    let request = Request::builder()
        .method("GET")
        .uri(server.http_url("/slow"))
        .header("Host", host)
        .body(Body::empty())
        .expect("http request");
    let response_task =
        tokio::spawn(async move { client.request(request).await.expect("response") });

    let inbound = tunnel.expect_http_request().await;
    assert_eq!(inbound.path, "/slow");

    let started = Instant::now();
    let response = response_task.await.expect("task");
    assert_eq!(response.status(), StatusCode::GATEWAY_TIMEOUT);
    assert!(started.elapsed() >= Duration::from_secs(1));

    tunnel.shutdown().await;
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn admin_api_lists_active_tunnels() {
    let mut config = fixtures::base_config();
    config.admin.token =
        Some(expose_server::config::SecureApiKey::new("admin-token-1234567890").unwrap());
    let server = TestServer::start(config.clone()).await;

    let tunnel = MockTunnelClient::connect(&server.websocket_url(), Some("admin"), None).await;

    let client = Client::new();
    let request = Request::builder()
        .uri(server.http_url("/admin/tunnels"))
        .header("Authorization", "Bearer admin-token-1234567890")
        .body(Body::empty())
        .expect("admin request");

    let response = client.request(request).await.expect("admin response");
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body()).await.expect("body");
    let tunnels: serde_json::Value = serde_json::from_slice(&body).expect("json");
    let list = tunnels.as_array().expect("array from admin endpoint");
    assert!(list
        .iter()
        .any(|entry| entry["subdomain"] == tunnel.assignment.assigned_subdomain));

    tunnel.shutdown().await;
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn tcp_tuning_config_does_not_break_startup() {
    let mut config = fixtures::base_config();
    config.tcp_tuning = TcpTuningConfig {
        nodelay: true,
        keepalive_enabled: true,
        keepalive_time_secs: 30,
        keepalive_interval_secs: 5,
        send_buffer_size: Some(128 * 1024),
        recv_buffer_size: Some(128 * 1024),
    };

    let server = TestServer::start(config).await;
    server.shutdown().await;
}

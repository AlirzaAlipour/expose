//! Axum server bootstrap and websocket tunnel coordination.

use crate::admin;
use crate::config::ServerConfig;
use crate::error::{other_io_error, ExposeError, Result};
use crate::health;
use crate::metrics::ServerMetrics;
use crate::middleware;
use crate::platform;
use crate::proxy;
use crate::proxy::path_router::path_proxy_request;
use crate::tcp_proxy::TcpTunnelRegistry;
use crate::tcp_tuning;
use crate::tls;
use crate::tunnel_manager::{OutgoingFrame, TunnelManager};
use axum::body::Body;
use axum::extract::ws::{Message as WsMessage, WebSocket, WebSocketUpgrade};
use axum::extract::Extension;
use axum::http::{Request, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{any, get};
use axum::{Json, Router};
use expose_common::error::ConfigError;
use expose_common::protocol::{
    ConnectResponse, ErrorCode, Message, VersionCheckResult, PROTOCOL_VERSION,
};
use futures_util::{SinkExt, StreamExt};
use serde_json::json;
use socket2::SockRef;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tracing::{info, warn};
use uuid::Uuid;

/// Shared application state.
#[derive(Clone)]
pub struct AppState {
    pub config: Arc<ServerConfig>,
    pub manager: Arc<TunnelManager>,
    pub tcp_registry: Arc<TcpTunnelRegistry>,
    pub platform_caps: platform::PlatformCapabilities,
    pub started_at: Instant,
}

/// Primary server entry point.
pub struct Server {
    state: Arc<AppState>,
}

impl Server {
    /// Instantiate the server with the provided configuration.
    pub fn new(config: ServerConfig) -> Self {
        let (per_minute, burst) = config.rate_limit_config();
        let manager = Arc::new(TunnelManager::new(
            config.domain.clone(),
            config.limits(),
            per_minute,
            burst,
            config.pending_requests.clone(),
            config.limits.max_tunnels,
            config.limits.max_tunnels_per_key,
        ));
        ServerMetrics::tunnels_active(manager.active_tunnel_count());
        let tcp_registry = Arc::new(TcpTunnelRegistry::new(&config.tcp_forward));
        let platform_caps = platform::detect_capabilities();
        let state = Arc::new(AppState {
            config: Arc::new(config),
            manager,
            tcp_registry,
            platform_caps,
            started_at: Instant::now(),
        });
        Self { state }
    }

    /// Run HTTP and/or HTTPS listeners based on configuration.
    pub async fn run(self) -> Result<()> {
        let app = self.build_router();
        let config = self.state.config.clone();
        let mut servers = Vec::new();

        if let Some(http_addr) = config.http_bind_address() {
            let addr: SocketAddr = http_addr.parse().map_err(|err| {
                ExposeError::Config(ConfigError::Validation(format!(
                    "invalid bind_address '{http_addr}': {err}"
                )))
            })?;
            let router = app.clone();
            let config = config.clone();
            servers.push(tokio::spawn(async move {
                let listener = TcpListener::bind(addr).await.map_err(|err| {
                    ExposeError::Network(other_io_error(format!(
                        "failed to bind HTTP listener on {addr}: {err}"
                    )))
                })?;
                if let Err(err) =
                    tcp_tuning::apply_sockref_tuning(SockRef::from(&listener), &config.tcp_tuning)
                {
                    warn!(?err, %addr, "failed to apply TCP tuning to HTTP listener");
                }
                info!(%addr, "HTTP server listening");
                axum::serve(listener, router.into_make_service())
                    .tcp_nodelay(config.tcp_tuning.nodelay)
                    .await
                    .map_err(|err| {
                        ExposeError::Network(other_io_error(format!("HTTP server error: {err}")))
                    })
            }));
        }

        if config.tls_enabled {
            let https_addr = config
                .https_bind_address()
                .ok_or_else(|| {
                    ExposeError::Config(ConfigError::Validation(
                        "https_bind_address must be set when tls_enabled = true".into(),
                    ))
                })?
                .parse::<SocketAddr>()
                .map_err(|err| {
                    ExposeError::Config(ConfigError::Validation(format!(
                        "invalid https_bind_address: {err}"
                    )))
                })?;
            let tls_config = tls::load_rustls_config(config.as_ref()).await?;
            let router = app.clone();
            let config = config.clone();
            servers.push(tokio::spawn(async move {
                info!(%https_addr, "HTTPS server listening");
                let listener = TcpListener::bind(https_addr).await.map_err(|err| {
                    ExposeError::Network(other_io_error(format!(
                        "failed to bind HTTPS listener on {https_addr}: {err}"
                    )))
                })?;
                if let Err(err) =
                    tcp_tuning::apply_sockref_tuning(SockRef::from(&listener), &config.tcp_tuning)
                {
                    warn!(?err, %https_addr, "failed to apply TCP tuning to HTTPS listener");
                }
                let std_listener = listener.into_std().map_err(|err| {
                    ExposeError::Network(other_io_error(format!(
                        "failed to convert HTTPS listener for rustls: {err}"
                    )))
                })?;
                axum_server::from_tcp_rustls(std_listener, tls_config)
                    .map_err(|err| {
                        ExposeError::Network(other_io_error(format!(
                            "failed to configure HTTPS server: {err}"
                        )))
                    })?
                    .serve(router.into_make_service())
                    .await
                    .map_err(|err| {
                        ExposeError::Network(other_io_error(format!("HTTPS server error: {err}")))
                    })
            }));
        }

        if servers.is_empty() {
            return Err(ExposeError::Config(ConfigError::Validation(
                "no HTTP or HTTPS listener configured".into(),
            )));
        }

        for server in servers {
            let result = server.await.map_err(|err| {
                ExposeError::Network(other_io_error(format!("server task failed: {err}")))
            })?;
            result?;
        }

        Ok(())
    }

    fn build_router(&self) -> Router {
        let config = self.state.config.clone();
        let mut public = Router::new()
            .route("/healthz", get(health::health_check))
            .route("/readyz", get(health::readiness_check))
            .route("/livez", get(health::liveness_check))
            .route("/connect", get(tunnel_entry));

        if config.routing_mode.supports_path() {
            let prefix = config.path_prefix.clone();
            info!(%prefix, "Enabling path-based tunnel routing");
            public = public
                .route(&format!("{}/:tunnel_name", prefix), any(path_proxy_request))
                .route(
                    &format!("{}/:tunnel_name/*rest", prefix),
                    any(path_proxy_request),
                );
        }

        if config.routing_mode.supports_subdomain() {
            info!("Enabling subdomain-based tunnel routing");
            public = public.fallback(proxy::subdomain_proxy_request);
        } else {
            public = public.fallback(path_routing_fallback);
        }

        public
            .nest("/admin", admin::router(self.state.clone()))
            .layer(middleware::rate_limit::layer({
                let limit = self.state.config.limits.max_tunnels;
                if limit == 0 {
                    1024
                } else {
                    limit * 4
                }
            }))
            .layer(tower_http::trace::TraceLayer::new_for_http())
            .layer(Extension(self.state.clone()))
    }
}

async fn tunnel_entry(
    Extension(state): Extension<Arc<AppState>>,
    ws: WebSocketUpgrade,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| async move {
        if let Err(err) = handle_socket(socket, state).await {
            warn!(?err, "tunnel connection terminated with error");
        }
    })
}

async fn handle_socket(socket: WebSocket, state: Arc<AppState>) -> Result<()> {
    let (sender, mut receiver) = socket.split();
    let (tx, mut rx) = mpsc::channel::<OutgoingFrame>(64);

    let writer = tokio::spawn(async move {
        let mut sender = sender;
        while let Some(frame) = rx.recv().await {
            match frame {
                OutgoingFrame::Protocol(message) => {
                    let bytes = message.encode()?;
                    if sender.send(WsMessage::Binary(bytes)).await.is_err() {
                        break;
                    }
                }
                OutgoingFrame::Control(control) => {
                    if sender.send(control).await.is_err() {
                        break;
                    }
                }
            }
        }
        let _ = sender.send(WsMessage::Close(None)).await;
        Ok::<_, ExposeError>(())
    });

    let connect_request = wait_for_connect(&mut receiver, &tx).await?;
    let version_check =
        VersionCheckResult::check(connect_request.protocol_version, PROTOCOL_VERSION);
    if !version_check.is_compatible() {
        let message = version_check
            .error_message()
            .unwrap_or_else(|| "unsupported protocol version".into());
        let _ = tx
            .send(OutgoingFrame::Protocol(Message::Error {
                code: ErrorCode::ProtocolMismatch,
                message: message.clone(),
            }))
            .await;
        return Err(ExposeError::VersionMismatch {
            client_version: connect_request.protocol_version,
            server_version: PROTOCOL_VERSION,
        });
    }

    if !state
        .config
        .validate_api_key(connect_request.api_key.as_deref())
    {
        let _ = tx
            .send(OutgoingFrame::Protocol(Message::Error {
                code: ErrorCode::AuthenticationFailed,
                message: "invalid API key".into(),
            }))
            .await;
        return Err(ExposeError::Authentication {
            reason: "invalid API key".into(),
        });
    }

    let subdomain = state
        .manager
        .allocate_subdomain(connect_request.desired_subdomain.clone())?;
    let tunnel_id = Uuid::new_v4();
    let handle = match state.manager.register_tunnel(
        tunnel_id,
        subdomain.clone(),
        connect_request.tunnel_protocol,
        tx.clone(),
        connect_request.api_key.as_deref(),
    ) {
        Ok(handle) => handle,
        Err(err @ ExposeError::CapacityExceeded { .. }) => {
            let _ = tx
                .send(OutgoingFrame::Protocol(Message::Error {
                    code: ErrorCode::RateLimitExceeded,
                    message: err.to_string(),
                }))
                .await;
            return Err(err);
        }
        Err(err @ ExposeError::SubdomainTaken { .. }) => {
            let _ = tx
                .send(OutgoingFrame::Protocol(Message::Error {
                    code: ErrorCode::SubdomainUnavailable,
                    message: err.to_string(),
                }))
                .await;
            return Err(err);
        }
        Err(err) => return Err(err),
    };

    let tcp_public_port =
        if connect_request.tunnel_protocol == expose_common::types::TunnelProtocol::Tcp {
            Some(
                state
                    .tcp_registry
                    .register_tunnel(handle.clone())
                    .await
                    .map_err(|err| {
                        warn!(?err, "failed to bind tcp listener");
                        err
                    })?,
            )
        } else {
            None
        };

    let public_port = match connect_request.tunnel_protocol {
        expose_common::types::TunnelProtocol::Tcp => tcp_public_port,
        _ => state.config.effective_public_port(),
    };

    let mut ack = ConnectResponse::build(
        tunnel_id,
        subdomain.clone(),
        state.manager.domain().to_string(),
        connect_request.tunnel_protocol,
        state.config.tls_enabled,
        public_port,
        &state.config.routing_mode,
        &state.config.path_prefix,
        state.manager.limits(),
    );
    ack.message = Some("tunnel connected".into());

    tx.send(OutgoingFrame::Protocol(Message::ConnectAck(ack)))
        .await
        .map_err(|_| ExposeError::Network(other_io_error("websocket closed")))?;

    while let Some(frame) = receiver.next().await {
        match frame {
            Ok(WsMessage::Binary(bytes)) => {
                let message = Message::decode(&bytes)?;
                if let Message::HttpResponse { body, .. } = &message {
                    ServerMetrics::bytes_received(&handle.subdomain, body.len());
                }

                match message {
                    response @ Message::HttpResponse { .. } => handle.fulfill(response),
                    Message::Disconnect { reason } => {
                        warn!(?reason, "client requested disconnect");
                        break;
                    }
                    Message::Error { code, message } => {
                        warn!(?code, %message, "client reported protocol error");
                        break;
                    }
                    Message::TcpConnectAck {
                        connection_id,
                        success,
                        error,
                    } => {
                        state.tcp_registry.handle_client_ack(
                            &handle.id,
                            &connection_id,
                            success,
                            error,
                        );
                    }
                    Message::TcpData {
                        connection_id,
                        data,
                        ..
                    } => {
                        state
                            .tcp_registry
                            .handle_client_data(&handle.id, &connection_id, data)
                            .await?;
                    }
                    Message::TcpClose { connection_id, .. } => {
                        state
                            .tcp_registry
                            .handle_client_close(&handle.id, &connection_id);
                    }
                    other => warn!(?other, "ignoring unsupported message from client"),
                }
            }
            Ok(WsMessage::Ping(payload)) => {
                let _ = tx
                    .send(OutgoingFrame::Control(WsMessage::Pong(payload)))
                    .await;
            }
            Ok(WsMessage::Close(_)) => break,
            Ok(_) => {}
            Err(err) => return Err(ExposeError::Network(other_io_error(err.to_string()))),
        }
    }

    handle.close();
    state.tcp_registry.remove_tunnel(&tunnel_id);
    state.manager.remove(&subdomain);
    drop(tx);
    let writer_result = writer
        .await
        .map_err(|err| ExposeError::Network(other_io_error(err.to_string())))?;
    writer_result?;
    info!(subdomain, "tunnel disconnected");
    Ok(())
}

async fn wait_for_connect(
    receiver: &mut futures_util::stream::SplitStream<WebSocket>,
    tx: &mpsc::Sender<OutgoingFrame>,
) -> Result<expose_common::protocol::ConnectRequest> {
    while let Some(frame) = receiver.next().await {
        match frame {
            Ok(WsMessage::Binary(bytes)) => match Message::decode(&bytes)? {
                Message::Connect(request) => return Ok(request),
                other => warn!(?other, "unexpected message during handshake"),
            },
            Ok(WsMessage::Ping(payload)) => {
                let _ = tx
                    .send(OutgoingFrame::Control(WsMessage::Pong(payload)))
                    .await;
            }
            Ok(WsMessage::Close(frame)) => {
                return Err(ExposeError::Network(other_io_error(format!(
                    "client closed during handshake: {:?}",
                    frame
                ))))
            }
            Ok(_) => {}
            Err(err) => return Err(ExposeError::Network(other_io_error(err.to_string()))),
        }
    }
    Err(ExposeError::Network(other_io_error(
        "client disconnected before sending Connect",
    )))
}

async fn path_routing_fallback(
    Extension(state): Extension<Arc<AppState>>,
    _request: Request<Body>,
) -> impl IntoResponse {
    path_routing_hint_response(&state.config)
}

/// Builds a helpful 404 response describing path-based routing.
pub(crate) fn path_routing_hint_response(config: &ServerConfig) -> Response {
    let scheme = if config.tls_enabled { "https" } else { "http" };
    let port_suffix = match config.effective_public_port() {
        Some(port)
            if (config.tls_enabled && port != 443) || (!config.tls_enabled && port != 80) =>
        {
            format!(":{port}")
        }
        _ => String::new(),
    };
    let hint = format!(
        "Tunnels are accessed via path routing: {}://{}{}{}/{{tunnel_name}}/...",
        scheme, config.domain, port_suffix, config.path_prefix
    );

    (
        StatusCode::NOT_FOUND,
        Json(json!({
            "error": "not found",
            "hint": hint,
            "routing_mode": config.routing_mode.to_string()
        })),
    )
        .into_response()
}

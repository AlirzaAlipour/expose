use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use expose_common::protocol::{
    ConnectRequest, ConnectResponse, ErrorCode, Message, PROTOCOL_VERSION,
};
use expose_common::types::{TunnelConfig, TunnelProtocol};
use futures_util::{SinkExt, StreamExt};
use tokio::sync::mpsc;
use tokio::time::{self, Duration};
use tokio_tungstenite::connect_async_with_config;
use tokio_tungstenite::tungstenite::Message as WsMessage;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};
use url::Url;
use uuid::Uuid;

use crate::config::MultiRuntimeConfig;
use crate::error::{other_io_error, ClientError, Result};
use crate::proxy::LocalProxy;
use crate::tcp_proxy::TcpLocalProxy;
use crate::tunnel::{print_tunnel_banner, Outgoing, ReconnectBackoff};

/// Public API invoked from client entry point.
pub async fn run(config: MultiRuntimeConfig) -> Result<()> {
    let shutdown = CancellationToken::new();
    let ctrl = shutdown.clone();
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        ctrl.cancel();
    });
    run_with_token(config, shutdown).await
}

async fn run_with_token(config: MultiRuntimeConfig, shutdown: CancellationToken) -> Result<()> {
    let mut backoff = ReconnectBackoff::new(
        config.reconnect_base_delay_ms,
        config.reconnect_max_attempts,
    );
    let mut has_connected = false;

    loop {
        if shutdown.is_cancelled() {
            return Ok(());
        }

        let reconnecting = has_connected;
        match run_single_session(&config, shutdown.clone(), reconnecting).await {
            SessionResult::Shutdown { had_connection } => {
                if had_connection {
                    backoff.reset();
                }
                return Ok(());
            }
            SessionResult::Reconnect {
                error,
                had_connection,
            } => {
                if shutdown.is_cancelled() {
                    return Ok(());
                }
                if had_connection {
                    has_connected = true;
                    backoff.reset();
                }
                if let Some(delay) = backoff.next_delay() {
                    let seconds = delay.as_secs_f32();
                    warn!(%error, retry_in_secs = seconds, "multi tunnel connection lost, retrying");
                    println!(
                        "Connection lost ({}). Retrying multi-tunnel session in {:.1}s...",
                        error, seconds
                    );
                    time::sleep(delay).await;
                    continue;
                }
                error!(%error, "Maximum reconnect attempts reached for multi command");
                return Err(error);
            }
        }
    }
}

#[derive(Debug)]
enum SessionResult {
    Shutdown {
        had_connection: bool,
    },
    Reconnect {
        error: ClientError,
        had_connection: bool,
    },
}

type WebSocket = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;
type WsReader = futures_util::stream::SplitStream<WebSocket>;

async fn run_single_session(
    config: &MultiRuntimeConfig,
    shutdown: CancellationToken,
    reconnecting: bool,
) -> SessionResult {
    let url = match Url::parse(&config.server_url) {
        Ok(url) => url,
        Err(err) => {
            return SessionResult::Reconnect {
                error: ClientError::from(other_io_error(err.to_string())),
                had_connection: false,
            }
        }
    };

    let tuning = config
        .tunnels
        .first()
        .map(|cfg| cfg.tcp_tuning.nodelay)
        .unwrap_or(true);

    let (socket, _response) = match connect_async_with_config(url.clone(), None, tuning).await {
        Ok(pair) => pair,
        Err(err) => {
            return SessionResult::Reconnect {
                error: ClientError::from(err),
                had_connection: false,
            }
        }
    };

    let (mut writer, mut reader) = socket.split();
    let (tx, mut rx) = mpsc::channel::<Outgoing>(64);

    let writer_task = tokio::spawn(async move {
        while let Some(frame) = rx.recv().await {
            match frame {
                Outgoing::Protocol(message) => {
                    let bytes = message.encode()?;
                    if writer.send(WsMessage::Binary(bytes)).await.is_err() {
                        break;
                    }
                }
                Outgoing::Raw(frame) => {
                    if writer.send(frame).await.is_err() {
                        break;
                    }
                }
            }
        }
        let _ = writer.send(WsMessage::Close(None)).await;
        Ok::<_, ClientError>(())
    });

    let ping_tx = tx.clone();
    let ping_shutdown = shutdown.clone();
    tokio::spawn(async move {
        let mut interval = time::interval(Duration::from_secs(20));
        loop {
            tokio::select! {
                _ = ping_shutdown.cancelled() => break,
                _ = interval.tick() => {}
            }
            if ping_tx
                .send(Outgoing::Raw(WsMessage::Ping(Vec::new())))
                .await
                .is_err()
            {
                break;
            }
        }
    });

    let mut manager = MultiTunnelManager::new();
    for tunnel_cfg in &config.tunnels {
        if let Err(err) = send_connect(tunnel_cfg, &tx).await {
            let _ = tx
                .send(Outgoing::Protocol(Message::Disconnect {
                    reason: Some(err.to_string()),
                }))
                .await;
            return SessionResult::Reconnect {
                error: err,
                had_connection: reconnecting,
            };
        }
        match wait_for_connect_ack(&mut reader, &tx).await {
            Ok(ack) => {
                manager.register_context(&ack, tunnel_cfg);
                print_tunnel_banner(&ack, tunnel_cfg);
            }
            Err(err) => {
                return SessionResult::Reconnect {
                    error: err,
                    had_connection: reconnecting,
                };
            }
        }
    }

    if reconnecting {
        println!("Reconnected multi-tunnel session successfully!");
    }

    enum Termination {
        Shutdown,
        Error(ClientError),
    }

    let mut shutdown_signal = Box::pin(shutdown.cancelled());

    let termination = loop {
        tokio::select! {
            _ = &mut shutdown_signal => {
                info!("multi tunnel shutdown requested");
                let _ = tx
                    .send(Outgoing::Protocol(Message::Disconnect {
                        reason: Some("client shutdown".into()),
                    }))
                    .await;
                break Termination::Shutdown;
            }
            Some(frame) = reader.next() => {
                match frame {
                    Ok(frame) => {
                        if let Err(err) = manager.handle_frame(frame, &tx).await {
                            break Termination::Error(err);
                        }
                    }
                    Err(err) => {
                        break Termination::Error(ClientError::from(err));
                    }
                }
            }
            else => {
                break Termination::Error(ClientError::from(other_io_error("connection closed")));
            }
        }
    };

    drop(tx);
    let writer_result = match writer_task.await {
        Ok(result) => result,
        Err(err) => {
            return SessionResult::Reconnect {
                error: ClientError::from(other_io_error(format!("writer task failed: {err}"))),
                had_connection: true,
            }
        }
    };

    if let Err(err) = writer_result {
        return SessionResult::Reconnect {
            error: err,
            had_connection: true,
        };
    }

    match termination {
        Termination::Shutdown => SessionResult::Shutdown {
            had_connection: true,
        },
        Termination::Error(error) => SessionResult::Reconnect {
            error,
            had_connection: true,
        },
    }
}

async fn send_connect(config: &TunnelConfig, tx: &mpsc::Sender<Outgoing>) -> Result<()> {
    let request = ConnectRequest {
        protocol_version: PROTOCOL_VERSION,
        api_key: config.api_key.clone(),
        desired_subdomain: config.subdomain.clone(),
        tunnel_protocol: config.protocol,
        client_version: env!("CARGO_PKG_VERSION").to_string(),
        metadata: Some(format!("local={}:{}", config.local_host, config.local_port)),
    };
    tx.send(Outgoing::Protocol(Message::Connect(request)))
        .await
        .map_err(|err| ClientError::from(other_io_error(err.to_string())))
}

async fn wait_for_connect_ack(
    reader: &mut WsReader,
    tx: &mpsc::Sender<Outgoing>,
) -> Result<ConnectResponse> {
    while let Some(frame) = reader.next().await {
        match frame {
            Ok(WsMessage::Binary(bytes)) => match Message::decode(&bytes)? {
                Message::ConnectAck(response) => return Ok(response),
                Message::Error { code, message } => {
                    return Err(protocol_error_from_message(code, message));
                }
                other => {
                    warn!(?other, "unexpected message while waiting for ConnectAck");
                }
            },
            Ok(WsMessage::Ping(payload)) => {
                let _ = tx.send(Outgoing::Raw(WsMessage::Pong(payload))).await;
            }
            Ok(WsMessage::Close(frame)) => {
                return Err(ClientError::from(other_io_error(format!(
                    "server closed during handshake: {:?}",
                    frame
                ))));
            }
            Ok(_) => {}
            Err(err) => return Err(ClientError::from(err)),
        }
    }
    Err(ClientError::from(other_io_error(
        "server closed before sending ConnectAck",
    )))
}

struct TunnelContext {
    tunnel_id: Uuid,
    protocol: TunnelProtocol,
    http_proxy: Option<Arc<LocalProxy>>,
    tcp_proxy: Option<Arc<TcpLocalProxy>>,
}

struct MultiTunnelManager {
    contexts: HashMap<Uuid, Arc<TunnelContext>>,
    subdomains: HashMap<String, Uuid>,
    http_requests: HashMap<Uuid, Uuid>,
    tcp_connections: HashMap<Uuid, Uuid>,
}

impl MultiTunnelManager {
    fn new() -> Self {
        Self {
            contexts: HashMap::new(),
            subdomains: HashMap::new(),
            http_requests: HashMap::new(),
            tcp_connections: HashMap::new(),
        }
    }

    fn register_context(
        &mut self,
        ack: &ConnectResponse,
        cfg: &TunnelConfig,
    ) -> Arc<TunnelContext> {
        let http_proxy = if ack.tunnel_protocol == TunnelProtocol::Http {
            Some(Arc::new(LocalProxy::new(
                cfg.local_host.clone(),
                cfg.local_port,
            )))
        } else {
            None
        };
        let tcp_proxy = if ack.tunnel_protocol == TunnelProtocol::Tcp {
            Some(Arc::new(TcpLocalProxy::new(
                cfg.local_host.clone(),
                cfg.local_port,
            )))
        } else {
            None
        };

        let context = Arc::new(TunnelContext {
            tunnel_id: ack.tunnel_id,
            protocol: ack.tunnel_protocol,
            http_proxy,
            tcp_proxy,
        });
        self.subdomains
            .insert(ack.assigned_subdomain.to_lowercase(), ack.tunnel_id);
        self.contexts.insert(ack.tunnel_id, context.clone());
        context
    }

    fn http_context_by_headers(&self, headers: &[(String, String)]) -> Option<Arc<TunnelContext>> {
        let host = headers
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case("host"))
            .map(|(_, value)| value.to_lowercase());
        if let Some(host) = host {
            let host_without_port = host.split(':').next().unwrap_or(&host);
            let subdomain = host_without_port
                .split('.')
                .next()
                .unwrap_or(host_without_port);
            if let Some(tunnel_id) = self.subdomains.get(subdomain) {
                return self.contexts.get(tunnel_id).cloned();
            }
        }
        self.contexts
            .values()
            .find(|ctx| ctx.protocol == TunnelProtocol::Http)
            .cloned()
    }

    fn http_context_for_request(&self, request_id: &Uuid) -> Option<Arc<TunnelContext>> {
        self.http_requests
            .get(request_id)
            .and_then(|id| self.contexts.get(id))
            .cloned()
    }

    fn track_http_request(&mut self, request_id: Uuid, tunnel_id: Uuid) {
        self.http_requests.insert(request_id, tunnel_id);
    }

    fn clear_http_request(&mut self, request_id: &Uuid) {
        self.http_requests.remove(request_id);
    }

    fn pick_tcp_context(&self) -> Option<Arc<TunnelContext>> {
        self.contexts
            .values()
            .find(|ctx| ctx.protocol == TunnelProtocol::Tcp)
            .cloned()
    }

    fn track_tcp_connection(&mut self, connection_id: Uuid, tunnel_id: Uuid) {
        self.tcp_connections.insert(connection_id, tunnel_id);
    }

    fn tcp_context(&self, connection_id: &Uuid) -> Option<Arc<TunnelContext>> {
        self.tcp_connections
            .get(connection_id)
            .and_then(|id| self.contexts.get(id))
            .cloned()
    }

    fn remove_tcp_connection(&mut self, connection_id: &Uuid) {
        self.tcp_connections.remove(connection_id);
    }

    async fn handle_frame(&mut self, frame: WsMessage, tx: &mpsc::Sender<Outgoing>) -> Result<()> {
        match frame {
            WsMessage::Binary(data) => match Message::decode(&data)? {
                Message::HttpRequest {
                    id,
                    method,
                    path,
                    headers,
                    body,
                } => {
                    if let Some(context) = self.http_context_by_headers(&headers) {
                        if let Some(proxy) = context.http_proxy.clone() {
                            spawn_http_task(proxy, tx.clone(), id, method, path, headers, body);
                        }
                    } else {
                        warn!("received HttpRequest without matching tunnel context");
                    }
                }
                Message::HttpRequestStart {
                    id,
                    method,
                    path,
                    headers,
                    ..
                } => {
                    if let Some(context) = self.http_context_by_headers(&headers) {
                        if let Some(proxy) = context.http_proxy.as_ref() {
                            proxy.begin_streaming_request(id, method, path, headers);
                            self.track_http_request(id, context.tunnel_id);
                        }
                    }
                }
                Message::HttpRequestChunk { id, data, .. } => {
                    if let Some(context) = self.http_context_for_request(&id) {
                        if let Some(proxy) = context.http_proxy.as_ref() {
                            proxy.push_streaming_chunk(&id, data)?;
                        }
                    }
                }
                Message::HttpRequestEnd { id } => {
                    if let Some(context) = self.http_context_for_request(&id) {
                        if let Some(proxy) = context.http_proxy.as_ref() {
                            if let Some((method, path, headers, body)) =
                                proxy.finish_streaming_request(&id)
                            {
                                spawn_http_task(
                                    proxy.clone(),
                                    tx.clone(),
                                    id,
                                    method,
                                    path,
                                    headers,
                                    body,
                                );
                            }
                        }
                    }
                    self.clear_http_request(&id);
                }
                Message::TcpConnect {
                    connection_id,
                    remote_addr,
                } => {
                    if let Some(context) = self.pick_tcp_context() {
                        if let Some(proxy) = context.tcp_proxy.as_ref() {
                            self.track_tcp_connection(connection_id, context.tunnel_id);
                            let proxy = proxy.clone();
                            let tx_clone = tx.clone();
                            tokio::spawn(async move {
                                if let Err(err) = proxy
                                    .handle_connect(connection_id, remote_addr, tx_clone)
                                    .await
                                {
                                    warn!(%connection_id, %err, "failed to open local TCP connection");
                                }
                            });
                        }
                    } else {
                        warn!("received TcpConnect but no TCP tunnels configured");
                    }
                }
                Message::TcpData {
                    connection_id,
                    data,
                    ..
                } => {
                    if let Some(context) = self.tcp_context(&connection_id) {
                        if let Some(proxy) = context.tcp_proxy.as_ref() {
                            proxy.handle_data(&connection_id, data).await?;
                        }
                    }
                }
                Message::TcpClose { connection_id, .. } => {
                    if let Some(context) = self.tcp_context(&connection_id) {
                        if let Some(proxy) = context.tcp_proxy.as_ref() {
                            proxy.handle_close(&connection_id);
                        }
                    }
                    self.remove_tcp_connection(&connection_id);
                }
                Message::Disconnect { reason } => {
                    return Err(ClientError::from(other_io_error(format!(
                        "Server requested disconnect: {:?}",
                        reason
                    ))));
                }
                Message::Error { code, message } => {
                    return Err(protocol_error_from_message(code, message));
                }
                other => {
                    warn!(?other, "ignoring unsupported message in multi tunnel loop");
                }
            },
            WsMessage::Ping(payload) => {
                let _ = tx.send(Outgoing::Raw(WsMessage::Pong(payload))).await;
            }
            WsMessage::Close(_frame) => {
                return Err(ClientError::from(other_io_error(
                    "server closed the websocket",
                )));
            }
            _ => {}
        }
        Ok(())
    }
}

fn spawn_http_task(
    proxy: Arc<LocalProxy>,
    tx: mpsc::Sender<Outgoing>,
    id: Uuid,
    method: String,
    path: String,
    headers: Vec<(String, String)>,
    body: Bytes,
) {
    tokio::spawn(async move {
        let response = match proxy
            .handle_http_request(id, method, path, headers, body)
            .await
        {
            Ok(resp) => resp,
            Err(err) => Message::HttpResponse {
                id,
                status: 502,
                headers: vec![("x-expose-error".into(), err.to_string())],
                body: Bytes::new(),
            },
        };

        if tx.send(Outgoing::Protocol(response)).await.is_err() {
            warn!("unable to send response to writer task");
        }
    });
}

fn protocol_error_from_message(code: ErrorCode, message: String) -> ClientError {
    match code {
        ErrorCode::AuthenticationFailed => {
            ClientError::from(other_io_error(format!("authentication failed: {message}")))
        }
        ErrorCode::SubdomainUnavailable => {
            ClientError::from(other_io_error(format!("subdomain unavailable: {message}")))
        }
        ErrorCode::RateLimitExceeded => {
            ClientError::from(other_io_error(format!("rate limited: {message}")))
        }
        ErrorCode::ProtocolMismatch => ClientError::from(other_io_error(message)),
        _ => ClientError::from(other_io_error(message)),
    }
}

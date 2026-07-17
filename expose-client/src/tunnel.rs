//! Tunnel management and websocket handling logic.

use crate::error::{other_io_error, Result};
use crate::proxy::LocalProxy;
use crate::tcp_proxy::TcpLocalProxy;
use crate::tcp_tuning;
use bytes::Bytes;
use expose_common::error::{ConfigError, ExposeError};
use expose_common::protocol::{self, ConnectRequest, ConnectResponse, ErrorCode, Message};
use expose_common::types::{TunnelConfig, TunnelProtocol};
use futures_util::stream::{SplitSink, SplitStream, StreamExt};
use futures_util::SinkExt;
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::time::{self, Duration};
use tokio_tungstenite::connect_async_with_config;
use tokio_tungstenite::tungstenite::Message as WsMessage;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, instrument, warn};
use url::Url;
use uuid::Uuid;

/// Convenience alias for the websocket type we operate on.
type WebSocket = WebSocketStream<MaybeTlsStream<TcpStream>>;
type WsWriter = SplitSink<WebSocket, WsMessage>;
type WsReader = SplitStream<WebSocket>;

#[derive(Debug)]
pub enum Outgoing {
    Protocol(Message),
    Raw(WsMessage),
}

#[derive(Debug)]
enum SessionResult {
    Shutdown {
        had_connection: bool,
    },
    Reconnect {
        error: ExposeError,
        had_connection: bool,
    },
}

#[derive(Debug)]
pub(crate) struct ReconnectBackoff {
    attempt: u32,
    max_attempts: Option<u32>,
    base_delay: Duration,
}

impl ReconnectBackoff {
    pub fn new(base_delay_ms: u64, max_attempts: u32) -> Self {
        Self {
            attempt: 0,
            max_attempts: if max_attempts == 0 {
                None
            } else {
                Some(max_attempts)
            },
            base_delay: Duration::from_millis(base_delay_ms.max(100)),
        }
    }

    pub fn reset(&mut self) {
        self.attempt = 0;
    }

    /// Record a failure and return the duration to wait before retrying.
    pub fn next_delay(&mut self) -> Option<Duration> {
        if let Some(max) = self.max_attempts {
            if self.attempt >= max {
                return None;
            }
        }
        let exponent = self.attempt.min(5); // clamp to 32s
        let multiplier: u32 = 1 << exponent;
        self.attempt = self.attempt.saturating_add(1);
        let mut delay = self
            .base_delay
            .checked_mul(multiplier)
            .unwrap_or(Duration::from_secs(32));
        let cap = Duration::from_secs(32);
        if delay > cap {
            delay = cap;
        }
        Some(delay)
    }
}

pub(crate) fn print_tunnel_banner(response: &ConnectResponse, config: &TunnelConfig) {
    let local_addr = match response.tunnel_protocol {
        TunnelProtocol::Http => format!("http://{}:{}", config.local_host, config.local_port),
        TunnelProtocol::Tcp => format!("tcp://{}:{}", config.local_host, config.local_port),
    };
    display_tunnel_banner(response, &local_addr);
}

fn display_tunnel_banner(response: &ConnectResponse, local_addr: &str) {
    let display_url = compute_display_url(response);
    println!();
    println!("  ╔══════════════════════════════════════════════════════════════╗");
    println!("  ║                    Tunnel Established!                       ║");
    println!("  ╠══════════════════════════════════════════════════════════════╣");
    println!("  ║  Public URL: {:<47} ║", display_url);
    println!("  ║  Subdomain:  {:<47} ║", response.assigned_subdomain);
    println!(
        "  ║  Protocol:   {:<47} ║",
        format!("{:?}", response.tunnel_protocol)
    );
    println!("  ║  Forwarding: {:<47} ║", local_addr);
    println!("  ║  Tunnel ID:  {:<47} ║", response.tunnel_id);
    println!("  ╚══════════════════════════════════════════════════════════════╝");
    println!();

    if let Some(msg) = &response.message {
        println!("  note: {msg}");
        println!();
    }

    info!(
        public_url = %display_url,
        tunnel_id = %response.tunnel_id,
        subdomain = %response.assigned_subdomain,
        "Tunnel ready"
    );
}

fn compute_display_url(response: &ConnectResponse) -> String {
    if !response.public_url.is_empty() {
        return response.public_url.clone();
    }

    let scheme = if response.public_scheme.is_empty() {
        match response.tunnel_protocol {
            TunnelProtocol::Http => "http",
            TunnelProtocol::Tcp => "tcp",
        }
    } else {
        response.public_scheme.as_str()
    };

    let port_suffix = match (scheme, response.public_port) {
        ("https", Some(port)) if port != 443 => format!(":{port}"),
        ("http", Some(port)) if port != 80 => format!(":{port}"),
        ("tcp", Some(port)) => format!(":{port}"),
        _ => String::new(),
    };

    format!(
        "{}://{}.{}{}",
        scheme, response.assigned_subdomain, response.domain, port_suffix
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_compute_display_url_prefers_server_value() {
        let resp = ConnectResponse::build(
            Uuid::new_v4(),
            "demo".into(),
            "example.com".into(),
            TunnelProtocol::Http,
            false,
            Some(8080),
            expose_common::types::RequestLimits::default(),
        );
        assert_eq!(compute_display_url(&resp), resp.public_url);
    }

    #[test]
    fn test_compute_display_url_fallbacks_when_empty() {
        let mut resp = ConnectResponse::build(
            Uuid::new_v4(),
            "demo".into(),
            "example.com".into(),
            TunnelProtocol::Http,
            false,
            Some(8080),
            expose_common::types::RequestLimits::default(),
        );
        resp.public_url.clear();
        resp.public_scheme.clear();
        assert_eq!(compute_display_url(&resp), "http://demo.example.com:8080");
    }
}

/// Establish and manage the lifetime of a tunnel connection with Ctrl+C shutdown.
pub async fn run(config: TunnelConfig) -> Result<()> {
    let shutdown = CancellationToken::new();
    let ctrl = shutdown.clone();
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        ctrl.cancel();
    });
    run_with_token(config, shutdown).await
}

/// Run the client using a provided cancellation token (primarily for tests).
#[instrument(skip(config, shutdown))]
pub async fn run_with_token(config: TunnelConfig, shutdown: CancellationToken) -> Result<()> {
    info!(server = %config.server_url, subdomain = ?config.subdomain, "Starting tunnel runtime");
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
        match run_single_session(&config, reconnecting, shutdown.clone()).await {
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
                    warn!(%error, retry_in_secs = seconds, "Connection lost, retrying");
                    println!(
                        "Connection lost ({}). Retrying in {:.1}s...",
                        error, seconds
                    );
                    tokio::select! {
                        _ = shutdown.cancelled() => {
                            return Ok(());
                        }
                        _ = time::sleep(delay) => {
                            // backoff delay elapsed, try again
                        }
                    }
                    continue;
                }
                error!(%error, "Maximum reconnect attempts reached");
                println!(
                    "Maximum reconnect attempts reached. Please verify your network connectivity or server status."
                );
                return Err(error);
            }
        }
    }
}

#[instrument(skip(config, shutdown))]
async fn run_single_session(
    config: &TunnelConfig,
    reconnecting: bool,
    shutdown: CancellationToken,
) -> SessionResult {
    let url = match Url::parse(&config.server_url) {
        Ok(url) => url,
        Err(err) => {
            return SessionResult::Reconnect {
                error: ExposeError::Config(ConfigError::Validation(format!(
                    "Invalid server URL: {err}"
                ))),
                had_connection: false,
            }
        }
    };

    let (socket, _response) =
        match connect_async_with_config(url.clone(), None, config.tcp_tuning.nodelay).await {
            Ok(ok) => ok,
            Err(err) => {
                return SessionResult::Reconnect {
                    error: ExposeError::Network(other_io_error(format!(
                        "Failed to connect to {}: {err}. Is the server reachable?",
                        url
                    ))),
                    had_connection: false,
                }
            }
        };
    if let Err(err) = tcp_tuning::apply_ws_stream_tuning(socket.get_ref(), &config.tcp_tuning) {
        warn!(?err, "failed to apply TCP tuning to tunnel connection");
    }
    let (mut writer, mut reader) = socket.split();

    let connect_request = ConnectRequest {
        protocol_version: protocol::PROTOCOL_VERSION,
        api_key: config.api_key.clone(),
        desired_subdomain: config.subdomain.clone(),
        tunnel_protocol: config.protocol,
        client_version: env!("CARGO_PKG_VERSION").to_string(),
        metadata: Some(format!("local={}", config.local_endpoint())),
    };

    let connect_ack = match perform_handshake(&mut writer, &mut reader, connect_request).await {
        Ok(ack) => ack,
        Err(err) => {
            return SessionResult::Reconnect {
                error: err,
                had_connection: false,
            }
        }
    };

    if reconnecting {
        println!("Reconnected successfully!");
    }
    print_tunnel_banner(&connect_ack, config);

    let http_proxy = if connect_ack.tunnel_protocol == TunnelProtocol::Http {
        Some(Arc::new(LocalProxy::new(
            config.local_host.clone(),
            config.local_port,
        )))
    } else {
        None
    };
    let tcp_proxy = if connect_ack.tunnel_protocol == TunnelProtocol::Tcp {
        Some(Arc::new(TcpLocalProxy::new(
            config.local_host.clone(),
            config.local_port,
        )))
    } else {
        None
    };
    let (tx, mut rx) = mpsc::channel::<Outgoing>(64);

    let writer_task = tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            match msg {
                Outgoing::Protocol(message) => {
                    debug!(variant = message.variant_name(), "sending protocol frame");
                    let bytes = message.encode()?;
                    if let Err(err) = writer.send(WsMessage::Binary(bytes)).await {
                        return Err(ExposeError::Network(other_io_error(format!(
                            "WebSocket send error: {err}"
                        ))));
                    }
                }
                Outgoing::Raw(frame) => {
                    if let Err(err) = writer.send(frame).await {
                        return Err(ExposeError::Network(other_io_error(format!(
                            "WebSocket control send error: {err}"
                        ))));
                    }
                }
            }
        }
        let _ = writer.send(WsMessage::Close(None)).await;
        Ok::<_, ExposeError>(())
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

    enum Termination {
        Shutdown,
        Error(ExposeError),
    }

    let mut shutdown_signal = Box::pin(shutdown.cancelled());

    let termination = loop {
        tokio::select! {
            _ = &mut shutdown_signal => {
                info!("received shutdown signal");
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
                        match handle_frame(frame, &http_proxy, &tcp_proxy, &tx).await {
                            Ok(FrameControl::Continue) => {}
                            Ok(FrameControl::Close) => {
                                break Termination::Error(ExposeError::TunnelDisconnected {
                                    reason: Some("Server closed tunnel".into()),
                                });
                            }
                            Err(err) => {
                                break Termination::Error(err);
                            }
                        }
                    }
                    Err(err) => {
                        break Termination::Error(ExposeError::Network(other_io_error(format!(
                            "WebSocket read error: {err}"
                        ))));
                    }
                }
            }
            else => {
                break Termination::Error(ExposeError::Network(other_io_error(
                    "Connection closed",
                )));
            }
        }
    };

    drop(tx);
    let writer_result = match writer_task.await {
        Ok(result) => result,
        Err(err) => {
            return SessionResult::Reconnect {
                error: ExposeError::Network(other_io_error(format!("Writer task failed: {err}"))),
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

#[derive(Debug)]
enum FrameControl {
    Continue,
    Close,
}

async fn handle_frame(
    frame: WsMessage,
    http_proxy: &Option<Arc<LocalProxy>>,
    tcp_proxy: &Option<Arc<TcpLocalProxy>>,
    tx: &mpsc::Sender<Outgoing>,
) -> Result<FrameControl> {
    match frame {
        WsMessage::Binary(data) => match Message::decode(&data)? {
            Message::HttpRequest {
                id,
                method,
                path,
                headers,
                body,
            } => {
                if let Some(proxy) = http_proxy {
                    spawn_proxy_task(proxy.clone(), tx.clone(), id, method, path, headers, body);
                } else {
                    warn!("received HttpRequest on non-HTTP tunnel");
                }
                Ok(FrameControl::Continue)
            }
            Message::HttpRequestStart {
                id,
                method,
                path,
                headers,
                ..
            } => {
                if let Some(proxy) = http_proxy {
                    proxy.begin_streaming_request(id, method, path, headers);
                }
                Ok(FrameControl::Continue)
            }
            Message::HttpRequestChunk { id, data, .. } => {
                if let Some(proxy) = http_proxy {
                    proxy.push_streaming_chunk(&id, data)?;
                }
                Ok(FrameControl::Continue)
            }
            Message::HttpRequestEnd { id } => {
                if let Some(proxy) = http_proxy {
                    if let Some((method, path, headers, body)) = proxy.finish_streaming_request(&id)
                    {
                        spawn_proxy_task(
                            proxy.clone(),
                            tx.clone(),
                            id,
                            method,
                            path,
                            headers,
                            body,
                        );
                    } else {
                        warn!(%id, "stream end without matching request start");
                    }
                } else {
                    warn!("received streaming HTTP body on TCP tunnel");
                }
                Ok(FrameControl::Continue)
            }
            Message::TcpConnect {
                connection_id,
                remote_addr,
            } => {
                if let Some(proxy) = tcp_proxy {
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
                Ok(FrameControl::Continue)
            }
            Message::TcpData {
                connection_id,
                data,
                ..
            } => {
                if let Some(proxy) = tcp_proxy {
                    proxy.handle_data(&connection_id, data).await?;
                }
                Ok(FrameControl::Continue)
            }
            Message::TcpClose { connection_id, .. } => {
                if let Some(proxy) = tcp_proxy {
                    proxy.handle_close(&connection_id);
                }
                Ok(FrameControl::Continue)
            }
            Message::TcpConnectAck { .. } => {
                warn!("unexpected TcpConnectAck from server");
                Ok(FrameControl::Continue)
            }
            Message::Disconnect { reason } => Err(ExposeError::TunnelDisconnected { reason }),
            Message::Error { code, message } => Err(protocol_error_from_message(code, message)),
            Message::HttpResponse { .. } => {
                warn!("unexpected HttpResponse from server");
                Ok(FrameControl::Continue)
            }
            msg => {
                warn!(?msg, "unsupported message from server");
                Ok(FrameControl::Continue)
            }
        },
        WsMessage::Ping(payload) => {
            let _ = tx.send(Outgoing::Raw(WsMessage::Pong(payload))).await;
            debug!("received websocket ping");
            Ok(FrameControl::Continue)
        }
        WsMessage::Pong(_) => Ok(FrameControl::Continue),
        WsMessage::Close(_) => Ok(FrameControl::Close),
        _ => Ok(FrameControl::Continue),
    }
}

fn spawn_proxy_task(
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

fn protocol_error_from_message(code: ErrorCode, message: String) -> ExposeError {
    match code {
        ErrorCode::AuthenticationFailed => ExposeError::Authentication { reason: message },
        ErrorCode::SubdomainUnavailable => ExposeError::SubdomainTaken { subdomain: message },
        ErrorCode::RateLimitExceeded => ExposeError::CapacityExceeded { resource: message },
        ErrorCode::ProtocolMismatch => version_mismatch_error(message),
        _ => ExposeError::Internal(message),
    }
}

fn version_mismatch_error(message: String) -> ExposeError {
    let server_version = parse_server_version(&message).unwrap_or(protocol::PROTOCOL_VERSION);
    ExposeError::VersionMismatch {
        client_version: protocol::PROTOCOL_VERSION,
        server_version,
    }
}

fn parse_server_version(message: &str) -> Option<u16> {
    let needle = "server version ";
    let start = message.find(needle)? + needle.len();
    let digits = message[start..]
        .chars()
        .take_while(|c| c.is_ascii_digit())
        .collect::<String>();
    if digits.is_empty() {
        None
    } else {
        digits.parse().ok()
    }
}

async fn perform_handshake(
    writer: &mut WsWriter,
    reader: &mut WsReader,
    request: ConnectRequest,
) -> Result<ConnectResponse> {
    writer
        .send(WsMessage::Binary(Message::Connect(request).encode()?))
        .await
        .map_err(|err| {
            ExposeError::Network(other_io_error(format!(
                "Failed to send handshake request: {err}"
            )))
        })?;

    loop {
        let frame = reader
            .next()
            .await
            .ok_or_else(|| {
                ExposeError::Network(other_io_error(
                    "Connection closed before the server acknowledged the tunnel",
                ))
            })?
            .map_err(|err| {
                ExposeError::Network(other_io_error(format!(
                    "Failed to read handshake response: {err}"
                )))
            })?;

        match frame {
            WsMessage::Binary(payload) => match Message::decode(&payload)? {
                Message::ConnectAck(ack) => return Ok(ack),
                Message::Error {
                    code: ErrorCode::ProtocolMismatch,
                    message,
                } => {
                    eprintln!();
                    eprintln!("  ╔══════════════════════════════════════════════════════════════╗");
                    eprintln!("  ║                   Version Mismatch Error                     ║");
                    eprintln!("  ╠══════════════════════════════════════════════════════════════╣");
                    eprintln!("  ║  {:<60} ║", message);
                    eprintln!("  ║                                                              ║");
                    eprintln!("  ║  Upgrade with: cargo install expose-client --force           ║");
                    eprintln!("  ╚══════════════════════════════════════════════════════════════╝");
                    eprintln!();
                    return Err(version_mismatch_error(message));
                }
                Message::Error { code, message } => {
                    return Err(protocol_error_from_message(code, message))
                }
                other => warn!(?other, "unexpected message during handshake"),
            },
            WsMessage::Ping(payload) => {
                writer.send(WsMessage::Pong(payload)).await.map_err(|err| {
                    ExposeError::Network(other_io_error(format!(
                        "Failed to respond to WebSocket ping: {err}"
                    )))
                })?;
            }
            WsMessage::Close(frame) => {
                let reason = frame
                    .and_then(|f| (!f.reason.is_empty()).then(|| f.reason.into_owned()))
                    .unwrap_or_else(|| "no reason provided".into());
                return Err(ExposeError::Network(other_io_error(format!(
                    "Server closed connection during handshake: {reason}. Verify your API key and requested subdomain."
                ))));
            }
            _ => {}
        }
    }
}

trait MessageExt {
    fn variant_name(&self) -> &'static str;
}

impl MessageExt for Message {
    fn variant_name(&self) -> &'static str {
        match self {
            Message::Connect(_) => "Connect",
            Message::ConnectAck(_) => "ConnectAck",
            Message::HttpRequest { .. } => "HttpRequest",
            Message::HttpResponse { .. } => "HttpResponse",
            Message::HttpRequestStart { .. } => "HttpRequestStart",
            Message::HttpRequestChunk { .. } => "HttpRequestChunk",
            Message::HttpRequestEnd { .. } => "HttpRequestEnd",
            Message::HttpResponseStart { .. } => "HttpResponseStart",
            Message::HttpResponseChunk { .. } => "HttpResponseChunk",
            Message::HttpResponseEnd { .. } => "HttpResponseEnd",
            Message::TcpConnect { .. } => "TcpConnect",
            Message::TcpConnectAck { .. } => "TcpConnectAck",
            Message::TcpData { .. } => "TcpData",
            Message::TcpClose { .. } => "TcpClose",
            Message::Disconnect { .. } => "Disconnect",
            Message::Error { .. } => "Error",
        }
    }
}

#[cfg(test)]
mod backoff_tests {
    use super::ReconnectBackoff;

    #[test]
    fn backoff_grows_exponentially_until_cap() {
        let mut backoff = ReconnectBackoff::new(1_000, 0);
        let expected = [1, 2, 4, 8, 16, 32, 32];
        for seconds in expected {
            let delay = backoff.next_delay().expect("delay");
            assert_eq!(delay.as_secs(), seconds);
        }
    }
}

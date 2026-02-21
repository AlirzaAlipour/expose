use std::net::SocketAddr;
use std::sync::Arc;

use axum::body::Bytes;
use dashmap::DashMap;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::config::TcpForwardConfig;
use crate::error::{ExposeError, Result};
use crate::metrics::ServerMetrics;
use crate::tunnel_manager::ActiveTunnel;
use expose_common::protocol::{Message, TcpCloseReason};

/// Manages per-tunnel TCP listeners and active connections.
#[derive(Debug)]
pub struct TcpTunnelRegistry {
    bind_host: String,
    tunnels: DashMap<Uuid, Arc<TcpTunnelHandle>>,
}

#[derive(Debug)]
struct TcpTunnelHandle {
    _tunnel_id: Uuid,
    tunnel: Arc<ActiveTunnel>,
    connections: Arc<DashMap<Uuid, TcpConnection>>,
    shutdown: CancellationToken,
    _listener_task: JoinHandle<()>,
}

#[derive(Debug)]
struct TcpConnection {
    writer: tokio::net::tcp::OwnedWriteHalf,
    reader_task: JoinHandle<()>,
}

impl TcpTunnelRegistry {
    pub fn new(config: &TcpForwardConfig) -> Self {
        Self {
            bind_host: config.bind_host.clone(),
            tunnels: DashMap::new(),
        }
    }

    pub async fn register_tunnel(&self, tunnel: Arc<ActiveTunnel>) -> Result<u16> {
        let listener = TcpListener::bind((self.bind_host.as_str(), 0))
            .await
            .map_err(|err| {
                ExposeError::Network(std::io::Error::new(std::io::ErrorKind::Other, err))
            })?;
        let port = listener
            .local_addr()
            .map_err(|err| {
                ExposeError::Network(std::io::Error::new(std::io::ErrorKind::Other, err))
            })?
            .port();

        let shutdown = CancellationToken::new();
        let connections = Arc::new(DashMap::new());
        let handle = Arc::new(TcpTunnelHandle {
            _tunnel_id: tunnel.id,
            tunnel: tunnel.clone(),
            connections: connections.clone(),
            shutdown: shutdown.clone(),
            _listener_task: tokio::spawn(Self::accept_loop(
                listener,
                tunnel.clone(),
                connections,
                shutdown,
            )),
        });
        self.tunnels.insert(tunnel.id, handle);
        Ok(port)
    }

    pub fn remove_tunnel(&self, tunnel_id: &Uuid) {
        if let Some((_, handle)) = self.tunnels.remove(tunnel_id) {
            handle.shutdown.cancel();
            handle.connections.clear();
        }
    }

    pub async fn handle_client_data(
        &self,
        tunnel_id: &Uuid,
        connection_id: &Uuid,
        data: Bytes,
    ) -> Result<()> {
        let handle = self
            .tunnels
            .get(tunnel_id)
            .ok_or_else(|| ExposeError::TunnelNotFound {
                identifier: tunnel_id.to_string(),
            })?;
        let mut conn = handle.connections.get_mut(connection_id).ok_or_else(|| {
            ExposeError::TunnelNotFound {
                identifier: format!("tcp-conn-{connection_id}"),
            }
        })?;
        conn.writer.write_all(&data).await.map_err(|err| {
            ExposeError::Network(std::io::Error::new(std::io::ErrorKind::Other, err))
        })?;
        ServerMetrics::bytes_received(&handle.tunnel.subdomain, data.len());
        Ok(())
    }

    pub fn handle_client_close(&self, tunnel_id: &Uuid, connection_id: &Uuid) {
        if let Some(handle) = self.tunnels.get(tunnel_id) {
            if let Some((_key, conn)) = handle.connections.remove(connection_id) {
                conn.reader_task.abort();
            }
        }
    }

    pub fn handle_client_ack(
        &self,
        tunnel_id: &Uuid,
        connection_id: &Uuid,
        success: bool,
        error: Option<String>,
    ) {
        if !success {
            warn!(
                %tunnel_id,
                %connection_id,
                err = error.as_deref().unwrap_or("unknown"),
                "client failed to open TCP connection"
            );
            self.handle_client_close(tunnel_id, connection_id);
        }
    }

    async fn accept_loop(
        listener: TcpListener,
        tunnel: Arc<ActiveTunnel>,
        connections: Arc<DashMap<Uuid, TcpConnection>>,
        shutdown: CancellationToken,
    ) {
        let tunnel_id = tunnel.id;
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => {
                    break;
                }
                accept_result = listener.accept() => {
                    match accept_result {
                        Ok((stream, addr)) => {
                            if let Err(err) = Self::handle_connection(&tunnel, &connections, stream, addr).await {
                                warn!(%err, "failed to handle tcp connection");
                            }
                        }
                        Err(err) => {
                            error!(error = %err, "tcp accept failed");
                            break;
                        }
                    }
                }
            }
        }
        for entry in connections.iter() {
            entry.value().reader_task.abort();
        }
        info!(%tunnel_id, "tcp accept loop ended");
    }

    async fn handle_connection(
        tunnel: &Arc<ActiveTunnel>,
        connections: &Arc<DashMap<Uuid, TcpConnection>>,
        stream: TcpStream,
        addr: SocketAddr,
    ) -> Result<()> {
        let (reader, writer) = stream.into_split();
        let connection_id = Uuid::new_v4();
        info!(
            tunnel = %tunnel.subdomain,
            %connection_id,
            remote = %addr,
            "tcp connection accepted"
        );
        tunnel
            .send(Message::TcpConnect {
                connection_id,
                remote_addr: addr.to_string(),
            })
            .await?;
        let tunnel_clone = tunnel.clone();
        let connections_clone = connections.clone();
        let read_task = tokio::spawn(async move {
            Self::read_loop(connection_id, reader, tunnel_clone, connections_clone).await;
        });
        connections.insert(
            connection_id,
            TcpConnection {
                writer,
                reader_task: read_task,
            },
        );
        Ok(())
    }

    async fn read_loop(
        connection_id: Uuid,
        mut reader: tokio::net::tcp::OwnedReadHalf,
        tunnel: Arc<ActiveTunnel>,
        connections: Arc<DashMap<Uuid, TcpConnection>>,
    ) {
        let mut buf = vec![0u8; 64 * 1024];
        let mut sequence = 0u64;
        loop {
            match reader.read(&mut buf).await {
                Ok(0) => {
                    let _ = tunnel
                        .send(Message::TcpClose {
                            connection_id,
                            reason: TcpCloseReason::Normal,
                        })
                        .await;
                    connections.remove(&connection_id);
                    break;
                }
                Ok(n) => {
                    let data = Bytes::copy_from_slice(&buf[..n]);
                    if tunnel
                        .send(Message::TcpData {
                            connection_id,
                            data,
                            sequence,
                        })
                        .await
                        .is_err()
                    {
                        break;
                    }
                    sequence = sequence.wrapping_add(1);
                }
                Err(err) => {
                    let _ = tunnel
                        .send(Message::TcpClose {
                            connection_id,
                            reason: TcpCloseReason::Error(err.to_string()),
                        })
                        .await;
                    connections.remove(&connection_id);
                    break;
                }
            }
        }
    }
}

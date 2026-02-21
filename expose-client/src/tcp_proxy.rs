use bytes::Bytes;
use dashmap::DashMap;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{info, warn};
use uuid::Uuid;

use crate::error::{other_io_error, Result};
use crate::tunnel::Outgoing;
use expose_common::error::ExposeError;
use expose_common::protocol::{Message, TcpCloseReason};

/// Manages local TCP connections for tunnel traffic.
pub struct TcpLocalProxy {
    host: String,
    port: u16,
    connections: DashMap<Uuid, TcpConnectionHandle>,
}

struct TcpConnectionHandle {
    writer: tokio::net::tcp::OwnedWriteHalf,
    reader_task: JoinHandle<()>,
}

impl TcpLocalProxy {
    pub fn new(host: impl Into<String>, port: u16) -> Self {
        Self {
            host: host.into(),
            port,
            connections: DashMap::new(),
        }
    }

    pub async fn handle_connect(
        &self,
        connection_id: Uuid,
        remote_addr: String,
        tx: mpsc::Sender<Outgoing>,
    ) -> Result<()> {
        info!(%connection_id, %remote_addr, "opening local TCP connection");
        match TcpStream::connect((self.host.as_str(), self.port)).await {
            Ok(stream) => {
                let (reader, writer) = stream.into_split();
                let read_tx = tx.clone();
                let read_task = tokio::spawn(async move {
                    if let Err(err) = Self::read_loop(connection_id, reader, read_tx).await {
                        warn!(%connection_id, %err, "TCP read loop ended with error");
                    }
                });
                self.connections.insert(
                    connection_id,
                    TcpConnectionHandle {
                        writer,
                        reader_task: read_task,
                    },
                );
                tx.send(Outgoing::Protocol(Message::TcpConnectAck {
                    connection_id,
                    success: true,
                    error: None,
                }))
                .await
                .map_err(|err| ExposeError::TunnelDisconnected {
                    reason: Some(err.to_string()),
                })?;
                Ok(())
            }
            Err(err) => {
                tx.send(Outgoing::Protocol(Message::TcpConnectAck {
                    connection_id,
                    success: false,
                    error: Some(err.to_string()),
                }))
                .await
                .ok();
                Err(ExposeError::Network(other_io_error(err.to_string())))
            }
        }
    }

    pub async fn handle_data(&self, id: &Uuid, data: Bytes) -> Result<()> {
        let mut entry =
            self.connections
                .get_mut(id)
                .ok_or_else(|| ExposeError::InvalidMessage {
                    context: format!("TCP data for unknown connection {id}"),
                })?;
        entry
            .writer
            .write_all(&data)
            .await
            .map_err(|err| ExposeError::Network(other_io_error(err.to_string())))
    }

    pub fn handle_close(&self, id: &Uuid) {
        if let Some((_key, conn)) = self.connections.remove(id) {
            conn.reader_task.abort();
        }
    }

    async fn read_loop(
        connection_id: Uuid,
        mut reader: tokio::net::tcp::OwnedReadHalf,
        tx: mpsc::Sender<Outgoing>,
    ) -> Result<()> {
        let mut buffer = vec![0u8; 64 * 1024];
        let mut sequence = 0u64;
        loop {
            let read = reader
                .read(&mut buffer)
                .await
                .map_err(|err| ExposeError::Network(other_io_error(err.to_string())))?;
            if read == 0 {
                let _ = tx
                    .send(Outgoing::Protocol(Message::TcpClose {
                        connection_id,
                        reason: TcpCloseReason::Normal,
                    }))
                    .await;
                break;
            }
            let payload = Bytes::copy_from_slice(&buffer[..read]);
            if tx
                .send(Outgoing::Protocol(Message::TcpData {
                    connection_id,
                    data: payload,
                    sequence,
                }))
                .await
                .is_err()
            {
                break;
            }
            sequence = sequence.wrapping_add(1);
        }
        Ok(())
    }
}

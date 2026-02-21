#![allow(dead_code)]

use anyhow::Result;
use expose_common::protocol::{ConnectResponse, Message};
use expose_common::types::TunnelProtocol;
use futures_util::{SinkExt, StreamExt};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot, Mutex, Notify};
use tokio::time::{Duration, Instant};
use tokio_tungstenite::tungstenite::Message as WsMessage;
use uuid::Uuid;

use super::{FAST_TIMEOUT, SLOW_TIMEOUT};

#[derive(Clone)]
pub enum ResponseBehavior {
    /// Normal server that acknowledges the handshake and waits for commands.
    Normal,
    /// Rejects the handshake by closing the connection immediately.
    RejectHandshake { reason: String },
    /// Accepts the handshake but disconnects after the provided duration.
    DisconnectAfter(Duration),
    /// Accepts the TCP connection but never sends a ConnectAck (forces client timeout).
    SilentDuringHandshake,
}

pub struct MockTunnelServer {
    addr: SocketAddr,
    command_sender: Arc<Mutex<Option<mpsc::Sender<ServerCommand>>>>,
    ready_notify: Arc<Notify>,
    recorded_messages: Arc<Mutex<Vec<Message>>>,
    shutdown_tx: Option<oneshot::Sender<()>>,
}

enum ServerCommand {
    Send(Message),
    Disconnect,
}

impl MockTunnelServer {
    pub async fn start(behavior: ResponseBehavior) -> Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        Self::from_listener(listener, behavior).await
    }

    pub async fn start_on(addr: SocketAddr, behavior: ResponseBehavior) -> Result<Self> {
        let listener = TcpListener::bind(addr).await?;
        Self::from_listener(listener, behavior).await
    }

    async fn from_listener(listener: TcpListener, behavior: ResponseBehavior) -> Result<Self> {
        let addr = listener.local_addr()?;
        let recorded_messages = Arc::new(Mutex::new(Vec::new()));
        let command_sender = Arc::new(Mutex::new(None));
        let ready_notify = Arc::new(Notify::new());
        let (shutdown_tx, shutdown_rx) = oneshot::channel();

        let server = Self {
            addr,
            command_sender: command_sender.clone(),
            ready_notify: ready_notify.clone(),
            recorded_messages: recorded_messages.clone(),
            shutdown_tx: Some(shutdown_tx),
        };

        tokio::spawn(async move {
            if let Err(err) = run_server(
                listener,
                behavior,
                recorded_messages,
                command_sender,
                ready_notify,
                shutdown_rx,
            )
            .await
            {
                eprintln!("mock server error: {err}");
            }
        });

        // provide a brief delay to ensure the listener is ready
        tokio::time::sleep(Duration::from_millis(50)).await;
        Ok(server)
    }

    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    pub async fn send(&self, message: Message) -> Result<()> {
        let deadline = Instant::now() + FAST_TIMEOUT;
        loop {
            if let Some(tx) = self.command_sender.lock().await.clone() {
                tx.send(ServerCommand::Send(message.clone()))
                    .await
                    .map_err(|err| anyhow::anyhow!("failed to send mock command: {err}"))?;
                return Ok(());
            }

            if Instant::now() > deadline {
                return Err(anyhow::anyhow!("mock server connection not ready"));
            }

            self.ready_notify.notified().await;
        }
    }

    pub async fn wait_for_message<F>(&self, duration: Duration, predicate: F) -> Result<Message>
    where
        F: Fn(&Message) -> bool,
    {
        let deadline = Instant::now() + duration;
        loop {
            {
                let messages = self.recorded_messages.lock().await;
                if let Some(found) = messages.iter().find(|m| predicate(m)).cloned() {
                    return Ok(found);
                }
            }

            if Instant::now() > deadline {
                let snapshot = self.recorded_messages.lock().await.clone();
                return Err(anyhow::anyhow!(
                    "timeout waiting for message. recorded: {:?}",
                    snapshot
                ));
            }

            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    pub async fn received_messages(&self) -> Vec<Message> {
        self.recorded_messages.lock().await.clone()
    }

    pub async fn shutdown(mut self) {
        if let Some(tx) = self.command_sender.lock().await.clone() {
            let _ = tx.send(ServerCommand::Disconnect).await;
        }
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn run_server(
    listener: TcpListener,
    behavior: ResponseBehavior,
    recorded_messages: Arc<Mutex<Vec<Message>>>,
    command_sender: Arc<Mutex<Option<mpsc::Sender<ServerCommand>>>>,
    ready_notify: Arc<Notify>,
    mut shutdown_rx: oneshot::Receiver<()>,
) -> Result<()> {
    loop {
        tokio::select! {
            biased;
            _ = &mut shutdown_rx => break,
            accept = listener.accept() => {
                let (stream, _) = accept?;
                let recorded = recorded_messages.clone();
                let command_slot = command_sender.clone();
                let notify = ready_notify.clone();
                let behavior = behavior.clone();
                tokio::spawn(async move {
                    if let Err(err) = handle_connection(stream, behavior, recorded, command_slot, notify).await {
                        eprintln!("mock connection error: {err}");
                    }
                });
            }
        }
    }
    Ok(())
}

async fn handle_connection(
    stream: TcpStream,
    behavior: ResponseBehavior,
    recorded_messages: Arc<Mutex<Vec<Message>>>,
    command_slot: Arc<Mutex<Option<mpsc::Sender<ServerCommand>>>>,
    ready_notify: Arc<Notify>,
) -> Result<()> {
    let ws_stream = tokio_tungstenite::accept_async(stream).await?;
    let (mut writer, mut reader) = ws_stream.split();
    let (cmd_tx, mut cmd_rx) = mpsc::channel(32);
    *command_slot.lock().await = Some(cmd_tx.clone());
    ready_notify.notify_waiters();

    let connect_msg = tokio::time::timeout(SLOW_TIMEOUT, reader.next())
        .await
        .map_err(|_| anyhow::anyhow!("did not receive Connect message"))?;

    let Some(Ok(frame)) = connect_msg else {
        return Err(anyhow::anyhow!("connection closed before handshake"));
    };

    let WsMessage::Binary(payload) = frame else {
        return Err(anyhow::anyhow!(
            "unexpected websocket frame during handshake"
        ));
    };

    let connect = Message::decode(&payload)?;
    recorded_messages.lock().await.push(connect.clone());

    match behavior.clone() {
        ResponseBehavior::RejectHandshake { .. } => {
            writer
                .send(WsMessage::Close(None))
                .await
                .map_err(|err| anyhow::anyhow!("failed to close websocket: {err}"))?;
            return Ok(());
        }
        ResponseBehavior::SilentDuringHandshake => {
            // never send ConnectAck - client should timeout
            return Ok(());
        }
        _ => {
            if let Message::Connect(request) = connect {
                let mut ack = ConnectResponse::new(
                    Uuid::new_v4(),
                    request
                        .desired_subdomain
                        .clone()
                        .unwrap_or_else(|| "test-client".into()),
                    "example.com".into(),
                    TunnelProtocol::Http,
                    false,
                    Some(8080),
                    Default::default(),
                );
                ack.message = Some("connected".into());
                writer
                    .send(WsMessage::Binary(Message::ConnectAck(ack).encode()?))
                    .await?;
            }
        }
    }

    if let ResponseBehavior::DisconnectAfter(delay) = behavior.clone() {
        let tx = cmd_tx.clone();
        tokio::spawn(async move {
            tokio::time::sleep(delay).await;
            let _ = tx.send(ServerCommand::Disconnect).await;
        });
    }

    loop {
        tokio::select! {
            Some(cmd) = cmd_rx.recv() => {
                match cmd {
                    ServerCommand::Send(message) => {
                        writer.send(WsMessage::Binary(message.encode()?)).await?;
                    }
                    ServerCommand::Disconnect => {
                        writer.send(WsMessage::Close(None)).await.ok();
                        break;
                    }
                }
            }
            maybe_frame = reader.next() => {
                match maybe_frame {
                    Some(Ok(WsMessage::Binary(bytes))) => {
                        let message = Message::decode(&bytes)?;
                        recorded_messages.lock().await.push(message);
                    }
                    Some(Ok(WsMessage::Close(_))) | None => break,
                    Some(Ok(_)) => {}
                    Some(Err(err)) => return Err(anyhow::anyhow!("websocket error: {err}")),
                }
            }
        }
    }

    *command_slot.lock().await = None;
    ready_notify.notify_waiters();
    Ok(())
}

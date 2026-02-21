use bytes::Bytes;
use expose_common::protocol::{ConnectRequest, ConnectResponse, Message, PROTOCOL_VERSION};
use expose_common::types::TunnelProtocol;
use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message as WsMessage;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};
use uuid::Uuid;

pub struct MockTunnelClient {
    ws: WebSocketStream<MaybeTlsStream<TcpStream>>,
    pub assignment: ConnectResponse,
}

pub struct HttpRequestFrame {
    pub id: Uuid,
    pub method: String,
    pub path: String,
    pub headers: Vec<(String, String)>,
    pub body: Bytes,
}

impl MockTunnelClient {
    pub async fn connect(
        ws_url: &str,
        desired_subdomain: Option<&str>,
        api_key: Option<&str>,
    ) -> Self {
        let (mut ws, _) = connect_async(ws_url).await.expect("connect to server");
        let connect = ConnectRequest {
            protocol_version: PROTOCOL_VERSION,
            api_key: api_key.map(|key| key.to_string()),
            desired_subdomain: desired_subdomain.map(|s| s.to_string()),
            tunnel_protocol: TunnelProtocol::Http,
            client_version: "test-suite".into(),
            metadata: None,
        };
        ws.send(WsMessage::Binary(
            Message::Connect(connect).encode().unwrap(),
        ))
        .await
        .expect("send connect frame");

        let mut assignment = None;
        while let Some(frame) = ws.next().await {
            match frame {
                Ok(WsMessage::Binary(bytes)) => match Message::decode(&bytes).unwrap() {
                    Message::ConnectAck(ack) => {
                        assignment = Some(ack);
                        break;
                    }
                    other => panic!("unexpected message during handshake: {other:?}"),
                },
                Ok(WsMessage::Ping(payload)) => {
                    ws.send(WsMessage::Pong(payload))
                        .await
                        .expect("respond to websocket ping");
                }
                Ok(WsMessage::Close(frame)) => {
                    panic!("server closed connection during handshake: {frame:?}");
                }
                Ok(other) => panic!("unexpected websocket frame: {other:?}"),
                Err(err) => panic!("websocket error: {err}"),
            }
        }

        let assignment = assignment.expect("connect ack");
        Self { ws, assignment }
    }

    pub async fn expect_http_request(&mut self) -> HttpRequestFrame {
        while let Some(frame) = self.ws.next().await {
            match frame {
                Ok(WsMessage::Binary(bytes)) => match Message::decode(&bytes).unwrap() {
                    Message::HttpRequest {
                        id,
                        method,
                        path,
                        headers,
                        body,
                    } => {
                        return HttpRequestFrame {
                            id,
                            method,
                            path,
                            headers,
                            body,
                        };
                    }
                    other => panic!("unexpected message from server: {other:?}"),
                },
                Ok(WsMessage::Ping(payload)) => {
                    self.ws.send(WsMessage::Pong(payload)).await.expect("pong");
                }
                Ok(WsMessage::Close(frame)) => {
                    panic!("tunnel closed unexpectedly: {frame:?}");
                }
                Ok(other) => panic!("unexpected websocket frame: {other:?}"),
                Err(err) => panic!("websocket error: {err}"),
            }
        }
        panic!("websocket closed while waiting for request");
    }

    pub async fn send_http_response(
        &mut self,
        id: Uuid,
        status: u16,
        headers: Vec<(String, String)>,
        body: Bytes,
    ) {
        let message = Message::HttpResponse {
            id,
            status,
            headers,
            body,
        };
        self.ws
            .send(WsMessage::Binary(message.encode().unwrap()))
            .await
            .expect("send http response");
    }

    pub async fn shutdown(mut self) {
        let _ = self.ws.close(None).await;
    }
}

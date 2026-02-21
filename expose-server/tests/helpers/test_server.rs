use crate::helpers::fixtures;
use expose_server::config::ServerConfig;
use expose_server::server::Server;
use std::net::SocketAddr;
use tokio::net::TcpStream;
use tokio::task::JoinHandle;
use tokio::time::{sleep, Duration};

pub struct TestServer {
    pub http_addr: SocketAddr,
    handle: JoinHandle<expose_server::error::Result<()>>,
}

impl TestServer {
    pub async fn start(mut config: ServerConfig) -> Self {
        let bind_address = config
            .http_bind_address()
            .map(|addr| addr.to_string())
            .unwrap_or_else(|| format!("127.0.0.1:{}", fixtures::unused_port()));
        config.bind_address = Some(bind_address.clone());

        let http_addr: SocketAddr = bind_address.parse().expect("valid bind address");
        let server = Server::new(config);
        let handle = tokio::spawn(async move { server.run().await });
        wait_for_port(http_addr).await;

        Self { http_addr, handle }
    }

    pub fn http_url(&self, path: &str) -> String {
        format!("http://{}{}", self.http_addr, path)
    }

    pub fn websocket_url(&self) -> String {
        format!("ws://{}/connect", self.http_addr)
    }

    pub async fn shutdown(self) {
        self.handle.abort();
        let _ = self.handle.await;
    }
}

async fn wait_for_port(addr: SocketAddr) {
    for _ in 0..50 {
        match TcpStream::connect(addr).await {
            Ok(stream) => {
                drop(stream);
                return;
            }
            Err(_) => sleep(Duration::from_millis(20)).await,
        }
    }
    panic!("server failed to start on {addr}");
}

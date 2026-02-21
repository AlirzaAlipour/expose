#![allow(dead_code)]

use anyhow::Result;
use bytes::Bytes;
use expose_common::types::{TcpTuningConfig, TunnelConfig, TunnelProtocol};
use hyper::body::to_bytes;
use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Request, Response, Server};
use std::collections::VecDeque;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::{oneshot, Mutex, Notify};
use tokio::task::JoinHandle;
use tokio::time::Duration;

use super::with_timeout;

pub fn config_for(server_url: &str, local_port: u16) -> TunnelConfig {
    TunnelConfig {
        protocol: TunnelProtocol::Http,
        local_host: "127.0.0.1".into(),
        local_port,
        subdomain: Some("test-client".into()),
        server_url: server_url.to_string(),
        api_key: None,
        reconnect_max_attempts: 3,
        reconnect_base_delay_ms: 100,
        tcp_tuning: TcpTuningConfig::default(),
    }
}

#[derive(Debug, Clone)]
pub struct RecordedRequest {
    pub method: String,
    pub path: String,
    pub body: Bytes,
}

pub struct TestHttpServer {
    addr: SocketAddr,
    shutdown_tx: Option<oneshot::Sender<()>>,
    requests: Arc<Mutex<VecDeque<RecordedRequest>>>,
    notify: Arc<Notify>,
    _task: JoinHandle<()>,
}

impl TestHttpServer {
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    pub async fn wait_for_request(&self, duration: Duration) -> Result<RecordedRequest> {
        with_timeout("wait for http request", duration, async {
            loop {
                if let Some(request) = self.requests.lock().await.pop_front() {
                    return Ok(request);
                }
                self.notify.notified().await;
            }
        })
        .await
    }

    pub async fn shutdown(mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        let _ = self._task.await;
    }
}

pub async fn start_test_http_server() -> Result<TestHttpServer> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let requests = Arc::new(Mutex::new(VecDeque::new()));
    let notify = Arc::new(Notify::new());
    let (shutdown_tx, mut shutdown_rx) = oneshot::channel();

    let requests_clone = requests.clone();
    let notify_clone = notify.clone();

    let server = Server::from_tcp(listener.into_std()?)?.serve(make_service_fn(move |_| {
        let requests = requests_clone.clone();
        let notify = notify_clone.clone();
        async move {
            Ok::<_, hyper::Error>(service_fn(move |req: Request<Body>| {
                let requests = requests.clone();
                let notify = notify.clone();
                async move {
                    let (parts, body) = req.into_parts();
                    let bytes = to_bytes(body).await.unwrap_or_default();
                    let record = RecordedRequest {
                        method: parts.method.as_str().to_string(),
                        path: parts
                            .uri
                            .path_and_query()
                            .map(|pq| pq.as_str().to_string())
                            .unwrap_or_else(|| parts.uri.path().to_string()),
                        body: bytes,
                    };
                    requests.lock().await.push_back(record);
                    notify.notify_waiters();
                    Ok::<_, hyper::Error>(Response::new(Body::from("ok")))
                }
            }))
        }
    }));

    let task = tokio::spawn(async move {
        tokio::select! {
            result = server => {
                if let Err(err) = result {
                    eprintln!("test http server error: {err}");
                }
            }
            _ = &mut shutdown_rx => {}
        }
    });

    Ok(TestHttpServer {
        addr,
        shutdown_tx: Some(shutdown_tx),
        requests,
        notify,
        _task: task,
    })
}

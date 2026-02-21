//! Linux io_uring accelerated proxy helpers.

use bytes::Bytes;
use tokio_uring::net::TcpStream;

use axum::body::Body;
use axum::extract::Host;
use axum::http::{Request, Response};
use std::sync::Arc;

use crate::error::{other_io_error, ExposeError, Result};
use crate::proxy;
use crate::server::AppState;

/// Read data from a TCP stream using io_uring.
pub async fn read_request_uring(stream: &TcpStream, buf: Vec<u8>) -> Result<(usize, Vec<u8>)> {
    let (res, buf) = stream.read(buf).await;
    let n = res.map_err(|err| ExposeError::Network(other_io_error(err.to_string())))?;
    Ok((n, buf))
}

/// Write a response using io_uring.
pub async fn write_response_uring(stream: &TcpStream, data: Bytes) -> Result<usize> {
    let buf = data.to_vec();
    let (res, _buf) = stream.write(buf).await;
    let n = res.map_err(|err| ExposeError::Network(other_io_error(err.to_string())))?;
    Ok(n)
}

/// Proxy request using io_uring operations when available.
///
/// Currently falls back to the standard proxy path on error.
pub async fn proxy_with_io_uring(
    state: Arc<AppState>,
    Host(_host): Host,
    request: Request<Body>,
) -> Result<Response<Body>> {
    // TODO: replace with io_uring-accelerated proxying. For now, use the standard path.
    crate::proxy::proxy_with_state(state, request).await
}

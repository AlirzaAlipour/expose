//! TCP socket tuning helpers for tunnel connections.

use expose_common::types::TcpTuningConfig;
use socket2::{SockRef, TcpKeepalive};
use std::io;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio_tungstenite::MaybeTlsStream;

/// Apply TCP tuning to a Tokio TCP stream.
pub fn apply_stream_tuning(stream: &TcpStream, config: &TcpTuningConfig) -> io::Result<()> {
    let sock_ref = SockRef::from(stream);
    apply_sockref_tuning(sock_ref, config)
}

/// Apply TCP tuning to a websocket transport (plain or TLS).
pub fn apply_ws_stream_tuning(
    stream: &MaybeTlsStream<TcpStream>,
    config: &TcpTuningConfig,
) -> io::Result<()> {
    match stream {
        MaybeTlsStream::Plain(tcp_stream) => apply_stream_tuning(tcp_stream, config),
        #[cfg(feature = "rustls-tls")]
        MaybeTlsStream::Rustls(tls_stream) => apply_stream_tuning(tls_stream.get_ref().0, config),
        #[cfg(feature = "native-tls")]
        MaybeTlsStream::NativeTls(tls_stream) => apply_stream_tuning(tls_stream.get_ref(), config),
        #[allow(unreachable_patterns)]
        _ => Ok(()),
    }
}

fn apply_sockref_tuning(socket: SockRef<'_>, config: &TcpTuningConfig) -> io::Result<()> {
    if config.nodelay {
        socket.set_nodelay(true)?;
    }

    if config.keepalive_enabled {
        let keepalive = TcpKeepalive::new()
            .with_time(Duration::from_secs(config.keepalive_time_secs))
            .with_interval(Duration::from_secs(config.keepalive_interval_secs));
        socket.set_tcp_keepalive(&keepalive)?;
    }

    if let Some(size) = config.send_buffer_size {
        socket.set_send_buffer_size(size)?;
    }
    if let Some(size) = config.recv_buffer_size {
        socket.set_recv_buffer_size(size)?;
    }

    Ok(())
}

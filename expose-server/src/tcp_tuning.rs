//! TCP socket tuning helpers for tunnel performance.

use expose_common::types::TcpTuningConfig;
use socket2::{SockRef, Socket, TcpKeepalive};
use std::io;
use std::time::Duration;

/// Apply TCP tuning options to a socket.
///
/// # Examples
/// ```no_run
/// use expose_common::types::TcpTuningConfig;
/// use expose_server::tcp_tuning::apply_tcp_tuning;
/// use socket2::Socket;
///
/// let socket = Socket::new(socket2::Domain::IPV4, socket2::Type::STREAM, None)?;
/// let config = TcpTuningConfig::default();
/// apply_tcp_tuning(&socket, &config)?;
/// # Ok::<(), std::io::Error>(())
/// ```
pub fn apply_tcp_tuning(socket: &Socket, config: &TcpTuningConfig) -> io::Result<()> {
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

/// Apply TCP tuning to any socket-like type via `SockRef`.
///
/// This is useful for `tokio::net::TcpListener` or `tokio::net::TcpStream`,
/// where the underlying socket is borrowed rather than owned.
pub fn apply_sockref_tuning(socket: SockRef<'_>, config: &TcpTuningConfig) -> io::Result<()> {
    apply_tcp_tuning(&socket, config)
}

#[cfg(test)]
mod tests {
    use super::*;
    use socket2::{Domain, Protocol, Type};

    #[test]
    fn apply_tcp_tuning_on_socket() {
        let socket =
            Socket::new(Domain::IPV4, Type::STREAM, Some(Protocol::TCP)).expect("create socket");
        let config = TcpTuningConfig::default();
        apply_tcp_tuning(&socket, &config).expect("apply tuning");
    }
}

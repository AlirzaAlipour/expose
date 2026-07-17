use expose_server::config::ServerConfig;
use std::net::TcpListener;

pub fn base_config() -> ServerConfig {
    let mut config = ServerConfig::default();
    config.domain = "test.localhost".into();
    config.bind_address = None;
    config.https_bind_address = None;
    config.tls_enabled = false;
    config.limits.max_tunnels = 32;
    config
}

pub fn unused_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .expect("bind ephemeral port")
        .local_addr()
        .expect("local addr")
        .port()
}

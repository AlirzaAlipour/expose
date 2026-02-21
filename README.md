# Expose

Expose is a Rust-native, ngrok-like tunnel service that lets you securely share local HTTP servers with the outside world. It consists of three crates in a single workspace:

- `expose-server`: Axum-based relay that terminates TLS, manages tunnels, and proxies public requests into active tunnels.
- `expose-client`: CLI that opens a persistent websocket tunnel to the relay and forwards HTTP requests to your localhost service.
- `expose-common`: Shared protocol definitions, message types, and configuration structs.

## Building

```bash
# Fetch dependencies and compile everything
cargo build --workspace

# Run the entire test suite
cargo test --workspace
```

## Quick Start

1. **Start the relay (HTTP only):**
   ```bash
   cargo run -p expose-server -- --config server.toml
   ```
2. **Open a tunnel from your development machine:**
   ```bash
   cargo run -p expose-client -- http 8080 --subdomain demo
   ```
3. Share the printed public URL (e.g., `http://demo.tunnel.example.com` or `https://demo.tunnel.example.com`) with collaborators.

### TLS / HTTPS Configuration

Enable HTTPS by supplying certificate paths in the server config (TOML):

```toml
bind_address = "0.0.0.0:8080"        # Optional plain HTTP listener
https_bind_address = "0.0.0.0:8443"  # Required when tls_enabled = true
tls_enabled = true
tls_cert_path = "/etc/expose/cert.pem"
tls_key_path = "/etc/expose/key.pem"

# Other common settings
domain = "tunnel.example.com"
public_port = 443
rate_limit_requests_per_minute = 60
rate_limit_burst_size = 10

[pending_requests]
max_per_tunnel = 100
max_global = 10000
sweep_interval_ms = 1000
default_timeout_secs = 30

[limits]
max_tunnels = 1000
max_tunnels_per_key = 10
max_pending_requests = 10000

[admin]
token = "generate-a-random-hex-string"
insecure_admin = false
rate_limit_per_minute = 60

[streaming]
enabled = true
threshold_bytes = 1048576
chunk_size_bytes = 65536
max_body_bytes = 104857600

[metrics]
enabled = false
bind_address = "127.0.0.1:9090"

[tcp_forward]
bind_host = "0.0.0.0"
```

The server validates that both files exist before starting HTTPS. You can still keep the HTTP listener enabled for health checks if desired. Use `[streaming]` to control when bodies are streamed instead of buffered, `[metrics]` to expose a Prometheus `/metrics` endpoint, and `[tcp_forward]` to set the bind host for per-tunnel TCP listeners.

### Per-Tunnel Rate Limiting

Every tunnel has a dedicated token bucket so a single client cannot overwhelm the relay. Use `rate_limit_requests_per_minute` and `rate_limit_burst_size` in `server.toml` to tune how many requests per minute and how large short bursts may be.

### Client Reconnection Settings

The client can automatically reconnect using exponential backoff. Tune these in the CLI flags or client TOML:

```toml
server_url = "wss://relay.example.com"
local_port = 8080
reconnect_max_attempts = 0        # 0 = infinite
reconnect_base_delay_ms = 1000    # starting delay (1s, doubles up to 32s)
```

### TCP Tunnels

Use the `tcp` subcommand when you need to forward raw TCP sockets instead of HTTP:

```bash
expose-client --server wss://relay.example.com --api-key $EXPOSE_KEY \
    --subdomain my-db \
    tcp --port 5432 --host 127.0.0.1
```

Each TCP tunnel receives a dedicated listener on the relay. Incoming connections are multiplexed through the websocket using the `TcpConnect`/`TcpData`/`TcpClose` frames and mirrored to your local host/port.

### Multi-Tunnel Configurations

To bring up several services over a single websocket, create a TOML file:

```toml
server_url = "wss://relay.example.com/connect"
api_key = "my-api-key"

[[tunnels]]
name = "web"
protocol = "http"
local_host = "127.0.0.1"
local_port = 3000
subdomain = "web"

[[tunnels]]
name = "db"
protocol = "tcp"
local_host = "127.0.0.1"
local_port = 5432
subdomain = "pg"
```

Then run `expose-client multi --config tunnels.toml` to establish all tunnels in one session.

## Performance Optimizations

Expose includes several zero-copy and network optimizations to maximize tunnel throughput.

### Zero-Copy Memory Management
Uses `bytes::Bytes` instead of `Vec<u8>` for HTTP payloads to minimize redundant allocations.

### TCP Tuning
Automatic socket tuning for low latency:
- `TCP_NODELAY` enabled (Nagle disabled)
- Configurable send/receive buffer sizes
- TCP keepalive probes with configurable intervals

### Linux-Specific Optimizations (Optional)
On Linux with kernel 5.1+, enable io_uring support:

```bash
cargo build --release --features io_uring
```

Benefits:
- Lower latency under load
- Reduced CPU usage
- Improved throughput for large payloads

### Configuration
```toml
# server.toml
[tcp_tuning]
nodelay = true
send_buffer_size = 262144  # 256KB
recv_buffer_size = 262144
```

## Troubleshooting

- **"Failed to read TLS certificate"** – double-check the paths in `server.toml` and that the process has read permissions.
- **"Server rejected connection: invalid API key"** – ensure the client `--api-key` matches one of the entries in the server config.
- **"Connection lost... retrying"** – the client will automatically back off and reconnect; verify the relay is reachable and TLS settings are correct.
- **HTTP 429 / Too Many Requests** – your tunnel exceeded the configured rate limit; either reduce test traffic or raise `rate_limit_requests_per_minute`/`rate_limit_burst_size` in the server config.

For more details on crate internals, consult each crate's `AGENT.md` file.

## Testing

Each crate carries its own async integration tests plus protocol/unit tests. Run everything locally with:

```bash
cargo test --workspace
```

Notable suites (see `TESTING.md` for details):

- `expose-client/tests` – spins up a mock WebSocket relay and Hyper-backed local server to validate handshake, proxying, reconnection, and shutdown flows.
- `expose-server/tests` – boots an in-process Axum server plus mock tunnel client to exercise API-key auth, HTTP proxying, timeout handling, and the admin API.
- `expose-common/tests` – ensures every protocol message round-trips, large payloads decode successfully, corrupt frames error out, and subdomain sanitization covers edge cases.

If you have GNU `timeout` (or `gtimeout` on macOS) available you can wrap long test runs, e.g. `timeout 60s cargo test --workspace`, to prevent forgotten commands from hanging.

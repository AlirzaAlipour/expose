# Testing Guide

## Philosophy

1. **Never hang** – every async operation is wrapped in a timeout helper with descriptive errors.
2. **Fail fast** – tests bail out immediately with context when an expectation is not met.
3. **Isolated** – each suite spins up its own mock servers/clients so tests can run in any order.
4. **Deterministic** – helper fixtures provide consistent configs, ports, and behaviors.

## Running Tests

```bash
# Entire workspace
cargo test --workspace

# Client crate only (with live logs)
RUST_LOG=debug cargo test -p expose-client -- --nocapture

# Single integration test
cargo test -p expose-client client_forwards_http_requests -- --nocapture

# Run tests sequentially for easier debugging
cargo test -p expose-client -- --test-threads=1

# (Optional) wrap with timeout if available
# timeout 60s cargo test --workspace
```

> Note: macOS users may need `gtimeout` from coreutils; the bare `timeout` command may not exist.

## Test Structure

```
expose-client/tests/
  helpers/        # shared fixtures, mock tunnel server, timeout utilities
  proxy_integration.rs
  tunnel_reconnect.rs
  config_validation.rs

expose-server/tests/
  helpers/
  integration_test.rs

expose-common/tests/
  protocol_test.rs
```

## Writing New Tests

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn example() -> anyhow::Result<()> {
    helpers::init_tracing();
    let server = MockTunnelServer::start(ResponseBehavior::Normal).await?;

    let result = helpers::with_timeout(
        "operation description",
        helpers::DEFAULT_TEST_TIMEOUT,
        async {
            // ... test operation ...
            Ok(())
        },
    )
    .await;

    server.shutdown().await;
    result
}
```

### Best Practices
- Use `anyhow::Result<()>` returns so `?` works everywhere.
- Wrap every `.await` that talks to the network with `with_timeout`.
- Always clean up spawned tasks/servers (use `CancellationToken` + `abort`).
- Prefer helper fixtures for configs, HTTP servers, and mock tunnels.
- Log progress via `helpers::init_tracing()` when debugging.

### Debugging Hangs
1. Run the test with `-- --nocapture` to see stdout/stderr.
2. Drop additional `with_timeout` wrappers around suspicious futures.
3. Use `eprintln!` or tracing logs to mark progress between steps.
4. Abort spawned tasks if a timeout triggers so tests can proceed.

## Common Issues

| Symptom | Fix |
| --- | --- |
| Test waits forever on `.await` | Wrap with `with_timeout("description", duration, async { ... })`. |
| Mock server never sees a message | Ensure you called `wait_for_message` with adequate timeout and that the client connected. |
| Port already in use | Bind helpers with `127.0.0.1:0` to let the OS pick an open port. |
| `timeout` command not found | Install GNU coreutils and use `gtimeout`, or rely on the built-in test timeouts. |

Keep the suites lean (<5 s per test) and explicit so regressions surface immediately.

## Performance Testing

Expose ships with Criterion benchmarks for zero-copy paths. Run them with:

```bash
cargo bench -p expose-server
cargo bench -p expose-client
```

Use `scripts/benchmark.sh` to execute standard and io_uring (Linux) runs.

## Platform-Specific Tests (Linux)

When building on Linux with the `io_uring` feature enabled, run:

```bash
cargo test -p expose-server --features io_uring
```

The io_uring tests are guarded by `cfg` so non-Linux platforms skip them.

## Benchmark Suite Usage

Benchmark suites live under:

- `expose-server/benches`
- `expose-client/benches`

Criterion outputs per-run statistics and change detection when previous results exist in `target/criterion`.

## CI/CD Performance Checks

CI should include a dedicated benchmark job and a Linux-only `io_uring` test job:

- `cargo bench --all-features`
- `cargo test --all --features io_uring`

# syntax=docker/dockerfile:1

## Build stage: compile the expose-server binary
FROM rust:1.83 AS builder
WORKDIR /workspace

# Copy the workspace manifest first to leverage Docker layer caching
COPY Cargo.toml Cargo.lock ./
COPY expose-common/Cargo.toml expose-common/
COPY expose-client/Cargo.toml expose-client/
COPY expose-server/Cargo.toml expose-server/

# Copy the full workspace sources
COPY expose-common expose-common
COPY expose-client expose-client
COPY expose-server expose-server

# Build only the expose-server crate in release mode
RUN cargo build --release -p expose-server

## Runtime stage: minimal image with the compiled binary
FROM debian:bookworm-slim AS runtime
WORKDIR /app

# Install CA certificates for outbound HTTPS (e.g., when proxying)
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Copy the compiled binary
COPY --from=builder /workspace/target/release/expose-server /app/expose-server

# Copy default configs and entrypoint helper
COPY server.dev.toml /app/server.dev.toml
COPY server.container.toml /app/server.container.toml
COPY expose-server/docker-entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

EXPOSE 6200 6201 9900

ENV EXPOSE_CONFIG=/app/server.dev.toml
ENTRYPOINT ["/app/entrypoint.sh"]

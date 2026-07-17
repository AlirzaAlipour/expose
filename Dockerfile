# Simple Dockerfile for running expose-server with the Darkube config.
FROM rust:1.90-bookworm AS builder
WORKDIR /workspace
COPY . .
RUN cargo build --release -p expose-server

FROM debian:bookworm-slim
RUN apt-get update \ 
    && apt-get install -y --no-install-recommends ca-certificates \ 
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=builder /workspace/target/release/expose-server /usr/local/bin/expose-server
COPY darkube.toml /app/darkube.toml
EXPOSE 8080
ENV RUST_LOG=info
CMD ["expose-server", "--config", "/app/darkube.toml"]
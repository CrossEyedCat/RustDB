# Multi-stage build for RustDB
FROM rust:1.90-slim AS builder

# Install build dependencies
RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

ENV CARGO_NET_GIT_FETCH_WITH_CLI=true

WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY benches ./benches

RUN cargo build --release --bin rustdb

# Final image — must match builder glibc (rust:1.90-slim uses newer Debian than bookworm).
FROM debian:trixie-slim

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl3 \
    && rm -rf /var/lib/apt/lists/*

# Create unprivileged user
RUN groupadd -r rustdb && useradd -r -g rustdb rustdb

# Create directories
RUN mkdir -p /app/data /app/logs /app/config && \
    chown -R rustdb:rustdb /app

# Copy binary
COPY --from=builder /app/target/release/rustdb /usr/local/bin/rustdb

# Copy configuration files
COPY config.toml /app/config/

# Run as rustdb user
USER rustdb

# Working directory
WORKDIR /app

# Exposed ports
EXPOSE 8080 8081

# Environment variables
ENV RUST_LOG=info
ENV RUSTDB_DATA_DIR=/app/data
ENV RUSTDB_LOG_DIR=/app/logs
ENV RUSTDB_CONFIG_DIR=/app/config

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD rustdb --version || exit 1

# Default command
CMD ["rustdb", "--config", "/app/config/config.toml"]

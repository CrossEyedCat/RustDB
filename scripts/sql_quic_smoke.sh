#!/usr/bin/env bash
# QUIC network SQL smoke test: run rustdb server in Docker, send SQL via rustdb_quic_client.
#
# Requirements: docker, cargo (Rust toolchain) on the host running this script.
#
# Usage:
#   ./scripts/sql_quic_smoke.sh
#   RUSTDB_IMAGE=ghcr.io/crosseyedcat/rustdb:main-type-sha ./scripts/sql_quic_smoke.sh
#
# Notes:
# - Server listens on UDP; we map host UDP port 15432 -> container port 5432/udp.
# - We export a dev leaf TLS cert (DER) and pin it in the client.
set -euo pipefail

RUSTDB_IMAGE="${RUSTDB_IMAGE:-ghcr.io/crosseyedcat/rustdb:main-type-sha}"
HOST_PORT="${HOST_PORT:-15432}"
SERVER_NAME="${SERVER_NAME:-127.0.0.1}"

name="rustdb-quic-smoke-$$"
vol="rustdb-quic-smoke-vol-$$"
cert_dir="$(mktemp -d)"
cert_path="${cert_dir}/server.der"

cleanup() {
  docker rm -f "$name" >/dev/null 2>&1 || true
  docker volume rm -f "$vol" >/dev/null 2>&1 || true
  rm -rf "$cert_dir" >/dev/null 2>&1 || true
}
trap cleanup EXIT

echo "==> pull ${RUSTDB_IMAGE}"
docker pull "${RUSTDB_IMAGE}" >/dev/null

echo "==> create volume ${vol}"
docker volume rm -f "$vol" >/dev/null 2>&1 || true
docker volume create "$vol" >/dev/null

echo "==> start server container (${name})"
docker rm -f "$name" >/dev/null 2>&1 || true
docker run -d --name "$name" \
  -p "${HOST_PORT}:5432/udp" \
  -v "${vol}:/app/data" \
  "${RUSTDB_IMAGE}" \
  sh -c "rustdb --config /app/config/config.toml server --host 0.0.0.0 --port 5432 --cert-out /tmp/server.der" \
  >/dev/null

echo "==> extract TLS leaf certificate"
for i in $(seq 1 30); do
  if docker exec "$name" test -s /tmp/server.der >/dev/null 2>&1; then
    break
  fi
  sleep 0.2
done
docker cp "${name}:/tmp/server.der" "$cert_path"
test -s "$cert_path"

echo "==> build QUIC client (host)"
cargo build -q --bin rustdb_quic_client

client() {
  local sql="$1"
  echo ""
  echo ">>> ${sql}"
  ./target/debug/rustdb_quic_client \
    --addr "${SERVER_NAME}:${HOST_PORT}" \
    --cert "$cert_path" \
    --server-name "$SERVER_NAME" \
    "$sql"
}

echo "==> network queries"
client "SELECT 1"
client "INSERT INTO net_t (a) VALUES (10)"
client "SELECT a FROM net_t ORDER BY a"
client "UPDATE net_t SET a = 11 WHERE a = 10"
client "SELECT a FROM net_t ORDER BY a"
client "DELETE FROM net_t WHERE a = 11"
client "SELECT a FROM net_t"

echo ""
echo "==> OK"


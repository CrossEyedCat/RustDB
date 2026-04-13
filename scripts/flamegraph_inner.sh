#!/usr/bin/env bash
# Runs INSIDE the rustdb-prof container.
set -euo pipefail

OUT_SVG="${OUT_SVG:-/app/profile-out/flame_rustdb_load_select1_docker.svg}"
CERT_PATH="${CERT_PATH:-/app/profile-out/flame_server.der}"
SERVER_NAME="${SERVER_NAME:-localhost}"
PORT="${PORT:-15432}"

CONCURRENCY="${CONCURRENCY:-128}"
QUERIES="${QUERIES:-20000}"
SQL="${SQL:-SELECT 1}"
MODE="${MODE:-shared}"
STREAM_BATCH="${STREAM_BATCH:-1}"

mkdir -p "$(dirname "$OUT_SVG")"

HOSTIP="$(getent hosts host.docker.internal 2>/dev/null | awk '$1 ~ /\\./ {print $1; exit}')"
if [[ -z "$HOSTIP" ]]; then
  # Fallback: try default gateway (may or may not be host depending on environment).
  HOSTIP="$(ip route show default 2>/dev/null | awk '{print $3; exit}')"
fi
if [[ -z "$HOSTIP" ]]; then
  echo "ERROR: could not resolve host IP (host.docker.internal)" >&2
  exit 2
fi

echo "host ip: $HOSTIP"

ADDR_HOST="$HOSTIP"
if [[ "$ADDR_HOST" == *:* ]]; then
  # IPv6 literal needs brackets for SocketAddr parsing.
  ADDR_HOST="[$ADDR_HOST]"
fi

exec /usr/local/cargo/bin/cargo flamegraph \
  --output "$OUT_SVG" \
  --bin rustdb_load -- \
    --addr "${ADDR_HOST}:${PORT}" \
    --cert "$CERT_PATH" \
    --server-name "$SERVER_NAME" \
    --concurrency "$CONCURRENCY" \
    --queries "$QUERIES" \
    --sql "$SQL" \
    --connection-mode "$MODE" \
    --stream-batch "$STREAM_BATCH"


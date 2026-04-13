#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

IMAGE="${IMAGE:-rustdb-prof}"
OUT_SVG="${OUT_SVG:-$ROOT/profile-out/flame_rustdb_load_select1.svg}"
OUT_SVG_REL="${OUT_SVG_REL:-profile-out/flame_rustdb_load_select1.svg}"
ADDR="${ADDR:-127.0.0.1:15432}"
SERVER_NAME="${SERVER_NAME:-localhost}"
CERT="${CERT:-$ROOT/profile-out/flame_server.der}"
CERT_REL="${CERT_REL:-profile-out/flame_server.der}"

CONCURRENCY="${CONCURRENCY:-128}"
QUERIES="${QUERIES:-20000}"
SQL="${SQL:-SELECT 1}"
MODE="${MODE:-shared}"
STREAM_BATCH="${STREAM_BATCH:-1}"

mkdir -p "$ROOT/profile-out"

echo "==> build profiler image: $IMAGE"
docker build -t "$IMAGE" --target profiler .

echo "==> ensure server cert exists at: $CERT"
if [[ ! -f "$CERT" ]]; then
  echo "Missing cert. Start the server locally to generate it, e.g.:"
  echo "  cargo run -q --bin rustdb -- server --host 127.0.0.1 --port 15432 --cert-out profile-out/flame_server.der --exit-after-secs 60"
  exit 2
fi

echo "==> run cargo flamegraph (needs --privileged for perf)"
docker run --rm --privileged \
  -v "$ROOT:/app" \
  -w /app \
  "$IMAGE" \
  cargo flamegraph --output "/app/$OUT_SVG_REL" --bin rustdb_load -- \
    --addr "$ADDR" \
    --cert "/app/$CERT_REL" \
    --server-name "$SERVER_NAME" \
    --concurrency "$CONCURRENCY" \
    --queries "$QUERIES" \
    --sql "$SQL" \
    --connection-mode "$MODE" \
    --stream-batch "$STREAM_BATCH"

echo "==> wrote: $ROOT/$OUT_SVG_REL"


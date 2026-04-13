#!/usr/bin/env bash
# Re-run cookbook checks from docs/cookbook.md against GHCR (default: main).
# Usage: RUSTDB_IMAGE=ghcr.io/.../rustdb:main-abc1234 ./scripts/verify-cookbook-docker.sh
set -euo pipefail

RUSTDB_IMAGE="${RUSTDB_IMAGE:-ghcr.io/crosseyedcat/rustdb:main}"

echo "==> pull $RUSTDB_IMAGE"
docker pull "$RUSTDB_IMAGE"

echo "==> version"
docker run --rm "$RUSTDB_IMAGE" rustdb --version

echo "==> info"
docker run --rm "$RUSTDB_IMAGE" rustdb info

echo "==> language list"
docker run --rm "$RUSTDB_IMAGE" rustdb language list

echo "==> query (local SqlEngine)"
docker run --rm "$RUSTDB_IMAGE" rustdb query "SELECT 1"

name="rustdb-cookbook-verify-$$"
echo "==> server in background ($name) — UDP/QUIC, default port from image config (5432)"
docker rm -f "$name" 2>/dev/null || true
docker run -d --name "$name" -p 15432:5432/udp "$RUSTDB_IMAGE" \
  rustdb --config /app/config/config.toml server --host 0.0.0.0
sleep 2
docker logs "$name" | head -n 20
docker rm -f "$name"

echo "==> OK"

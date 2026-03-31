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

echo "==> query (stub)"
docker run --rm "$RUSTDB_IMAGE" rustdb query "SELECT 1"

name="rustdb-cookbook-verify-$$"
echo "==> server in background ($name)"
docker rm -f "$name" 2>/dev/null || true
docker run -d --name "$name" -p 18080:8080/udp "$RUSTDB_IMAGE" \
  rustdb server --host 0.0.0.0 --port 8080
sleep 2
docker logs "$name" | head -n 20
docker rm -f "$name"

echo "==> OK"

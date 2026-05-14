#!/usr/bin/env bash
#
# Short local TPC-C-ish run against a RustDB QUIC server in Docker, with optional SQL phase logs.
#
# Usage (from repo root, Linux / Git Bash / WSL):
#   ./scripts/profile_tpcc_local_docker.sh
#   RUSTDB_IMAGE=ghcr.io/crosseyedcat/rustdb:main ./scripts/profile_tpcc_local_docker.sh
#
# Prerequisites: Docker, `cargo`, Python 3 (same as tpcc_throughput_ci.sh).
#
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

# Default: build a local image if RUSTDB_IMAGE unset (slow first time).
RUSTDB_IMAGE="${RUSTDB_IMAGE:-rustdb-profile:local}"
OUT_DIR="${OUT_DIR:-$ROOT/tpcc-profile-out}"
UDP_PORT="${UDP_PORT:-15433}"
CONTAINER_NAME="${CONTAINER_NAME:-rustdb-profile-tpcc}"
VOL_NAME="${VOL_NAME:-rustdb_profile_tpcc_data}"

CONCURRENCY="${CONCURRENCY:-8}"
DURATION_SECS="${DURATION_SECS:-15}"
MIX="${MIX:-payment=1.0}"

mkdir -p "$OUT_DIR"
OUT_DIR_ABS="$(cd "$OUT_DIR" && pwd)"

if [[ "${SKIP_BUILD_IMAGE:-0}" != "1" ]]; then
  echo "==> docker build -t $RUSTDB_IMAGE (set SKIP_BUILD_IMAGE=1 to skip)"
  docker build -t "$RUSTDB_IMAGE" -f Dockerfile .
fi

export RUSTDB_IMAGE
export OUT_DIR_ABS
export UDP_PORT
export CONTAINER_NAME
export VOL_NAME
export CONCURRENCY
export DURATION_SECS
export MIX
export DURATION_SECS

# Reuse CI script shape: pass env through tpcc_throughput_ci by inlining minimal subset
echo "==> seed + server (SQL phase logs on server when RUSTDB_SQL_PHASE_LOG set in docker run)"
docker volume rm -f "$VOL_NAME" >/dev/null 2>&1 || true
docker volume create "$VOL_NAME" >/dev/null

SEED_IN="$ROOT/scripts/tpcc_seed.sql"
SEED_FILTERED="$OUT_DIR_ABS/tpcc_seed.filtered.sql"
python3 - <<PY
import pathlib
src = pathlib.Path(r"${SEED_IN}")
out = pathlib.Path(r"${SEED_FILTERED}")
lines = []
for line in src.read_text(encoding="utf-8").splitlines():
    s = line.strip()
    if not s or s.startswith("--"):
        continue
    lines.append(line)
out.write_text("\n".join(lines) + "\n", encoding="utf-8")
print(f"filtered seed: {out} (lines={len(lines)})")
PY

docker run --rm -i \
  -v "$VOL_NAME:/app/data" \
  -v "$SEED_FILTERED:/tmp/tpcc_seed.sql:ro" \
  "$RUSTDB_IMAGE" \
  sh -c 'rustdb query --batch-file /tmp/tpcc_seed.sql' >/dev/null

docker rm -f "$CONTAINER_NAME" >/dev/null 2>&1 || true
docker run -d --name "$CONTAINER_NAME" \
  -p "${UDP_PORT}:5432/udp" \
  -v "$VOL_NAME:/app/data" \
  -v "$ROOT/config.toml:/app/config/config.toml:ro" \
  -e "RUST_LOG=${RUST_LOG:-info}" \
  -e "RUSTDB_SQL_PHASE_LOG=${RUSTDB_SQL_PHASE_LOG:-1}" \
  ${RUSTDB_DEFER_HEAP_FLUSH_AFTER_DML:+-e "RUSTDB_DEFER_HEAP_FLUSH_AFTER_DML=$RUSTDB_DEFER_HEAP_FLUSH_AFTER_DML"} \
  "$RUSTDB_IMAGE" \
  sh -c 'rustdb --config /app/config/config.toml server --host 0.0.0.0 --port 5432 --cert-out /tmp/server.der' >/dev/null

for i in $(seq 1 120); do
  if docker exec "$CONTAINER_NAME" sh -c "test -s /tmp/server.der" >/dev/null 2>&1; then
    break
  fi
  sleep 0.25
done

CERT="$OUT_DIR_ABS/server.der"
docker cp "$CONTAINER_NAME:/tmp/server.der" "$CERT"
test -s "$CERT"

echo "==> cargo build rustdb_tpcc"
cargo build -q --bin rustdb_tpcc

echo "==> run rustdb_tpcc (${CONCURRENCY} clients, ${DURATION_SECS}s, mix=$MIX)"
set +e
RUST_LOG="${RUST_LOG:-info}" \
./target/debug/rustdb_tpcc \
  --addr "127.0.0.1:${UDP_PORT}" \
  --cert "$CERT" \
  --server-name localhost \
  --concurrency "$CONCURRENCY" \
  --duration-seconds "$DURATION_SECS" \
  --mix "$MIX" \
  --txn-log "$OUT_DIR_ABS/profile_txn.log" \
  --json > "$OUT_DIR_ABS/profile.json"
rc=$?
set -e

echo "==> server logs (tail) -> $OUT_DIR_ABS/server_tail.log"
docker logs "$CONTAINER_NAME" 2>&1 | tail -n 400 > "$OUT_DIR_ABS/server_tail.log" || true

docker rm -f "$CONTAINER_NAME" >/dev/null 2>&1 || true

if [[ "$rc" -ne 0 ]]; then
  echo "rustdb_tpcc failed (exit $rc); see $OUT_DIR_ABS/server_tail.log"
  exit "$rc"
fi

echo "==> wrote $OUT_DIR_ABS/profile.json ; grep sql_phases / rustdb::sql_phases in server_tail.log"
grep -E 'sql_phases|sql_parse|update|delete' "$OUT_DIR_ABS/server_tail.log" | tail -n 80 || true
echo "Done. Tip: RUST_LOG=rustdb::sql_phases=info for only phase lines."

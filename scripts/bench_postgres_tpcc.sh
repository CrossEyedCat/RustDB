#!/usr/bin/env bash
#
# PostgreSQL TPC-C-ish baseline (same workload as rustdb_tpcc / scripts/tpcc_seed.sql).
# Writes tpcc-out/postgres_tpcc.txt + postgres_tpcc.json (+ optional postgres_tpcc_txn.log).
#
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
export TPCC_ROOT="$ROOT"

# shellcheck source=scripts/tpcc_host_bins.sh
source "$ROOT/scripts/tpcc_host_bins.sh"

OUT_DIR="${OUT_DIR:-$ROOT/tpcc-out}"
mkdir -p "$OUT_DIR"
OUT_DIR_ABS="$(cd "$OUT_DIR" && pwd)"

CONCURRENCY="${CONCURRENCY:-64}"
DURATION_SECS="${DURATION_SECS:-300}"
MIX="${MIX:-new_order=0.45,payment=0.43,order_status=0.04,delivery=0.04,stock_level=0.04}"
TXNS="${TXNS:-5000}"

name="${POSTGRES_CONTAINER_NAME:-postgres-tpcc-bench}"
POSTGRES_USER="${POSTGRES_USER:-postgres}"
POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-postgres}"
POSTGRES_DB="${POSTGRES_DB:-tpcc_bench}"
HOST_PORT="${POSTGRES_HOST_PORT:-15440}"

SEED_IN="$ROOT/scripts/tpcc_seed.sql"
SEED_FILTERED="$OUT_DIR_ABS/tpcc_seed.postgres.filtered.sql"
python3 - "$SEED_IN" "$SEED_FILTERED" <<'PY'
import pathlib, sys
src = pathlib.Path(sys.argv[1])
out = pathlib.Path(sys.argv[2])
lines = []
for line in src.read_text(encoding="utf-8").splitlines():
    s = line.strip()
    if not s or s.startswith("--"):
        continue
    lines.append(line)
out.write_text("\n".join(lines) + "\n", encoding="utf-8")
print(f"filtered seed: {out} (lines={len(lines)})")
PY

# Bench-oriented server flags (same profile as CI; helps slow Docker hosts).
POSTGRES_BENCH_TUNING="${POSTGRES_BENCH_TUNING:-1}"
PG_IMAGE="${POSTGRES_IMAGE:-postgres:16-alpine}"
PG_DOCKER_ARGS=(--shm-size=512m)
if [[ "$POSTGRES_BENCH_TUNING" == "1" ]]; then
  PG_SERVER_ARGS=(
    -c max_connections=500
    -c shared_buffers=256MB
    -c effective_cache_size=512MB
    -c synchronous_commit=off
    -c checkpoint_completion_target=0.9
    -c wal_buffers=16MB
    -c random_page_cost=1.1
    -c effective_io_concurrency=200
    -c work_mem=8MB
    -c max_wal_size=2GB
  )
else
  PG_SERVER_ARGS=(-c max_connections=500)
fi

docker rm -f "$name" >/dev/null 2>&1 || true
docker run -d --name "$name" -p "${HOST_PORT}:5432" \
  "${PG_DOCKER_ARGS[@]}" \
  -e POSTGRES_PASSWORD="$POSTGRES_PASSWORD" \
  -e POSTGRES_USER="$POSTGRES_USER" \
  -e POSTGRES_DB="$POSTGRES_DB" \
  "$PG_IMAGE" \
  postgres "${PG_SERVER_ARGS[@]}" >/dev/null

ready=0
for _ in $(seq 1 240); do
  if docker exec "$name" pg_isready -h 127.0.0.1 -p 5432 -U "$POSTGRES_USER" >/dev/null 2>&1; then
    ready=1
    break
  fi
  sleep 0.5
done
if [[ "$ready" -ne 1 ]]; then
  echo "ERROR: Postgres did not become ready in time"
  docker logs "$name" 2>&1 | tail -n 200 || true
  exit 1
fi

docker exec -i "$name" psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -v ON_ERROR_STOP=1 \
  < "$SEED_FILTERED" >/dev/null

{
  echo "== postgres_tpcc =="
  echo "driver: tokio-postgres (same SQL mix as rustdb_tpcc)"
  echo "db: $POSTGRES_DB"
  echo "host: 127.0.0.1 port: $HOST_PORT -> container 5432"
  echo "concurrency: $CONCURRENCY"
  echo "duration_s: $DURATION_SECS"
  echo "mix: $MIX"
  echo "postgres_prepared: ${POSTGRES_TPCC_PREPARED:-0}"
  echo ""
} | tee "$OUT_DIR_ABS/postgres_tpcc.txt"

tpcc_build_bin postgres_tpcc
POSTGRES_TPCC_BIN="$(tpcc_bin_path postgres_tpcc)"

TXN_ARGS=()
if [[ -n "${DURATION_SECS:-}" ]]; then
  TXN_ARGS=(--duration-seconds "$DURATION_SECS")
else
  TXN_ARGS=(--transactions "$TXNS")
fi
PREPARED_ARGS=()
if [[ "${POSTGRES_TPCC_PREPARED:-0}" == "1" ]]; then
  PREPARED_ARGS=(--prepared)
fi

set +e
"$POSTGRES_TPCC_BIN" \
  --host 127.0.0.1 \
  --port "$HOST_PORT" \
  --user "$POSTGRES_USER" \
  --password "$POSTGRES_PASSWORD" \
  --database "$POSTGRES_DB" \
  --concurrency "$CONCURRENCY" \
  "${TXN_ARGS[@]}" \
  "${PREPARED_ARGS[@]}" \
  --mix "$MIX" \
  --txn-log "$OUT_DIR_ABS/postgres_tpcc_txn.log" \
  --json > "$OUT_DIR_ABS/postgres_tpcc.json"
rc=$?
set -e

if [[ "$rc" -ne 0 ]]; then
  echo "postgres_tpcc failed (exit $rc)"
  exit "$rc"
fi

python3 - "$OUT_DIR_ABS" <<'PY'
import json, pathlib, sys
out_dir = pathlib.Path(sys.argv[1])
p = out_dir / "postgres_tpcc.json"
data = json.loads(p.read_text())
txt_path = out_dir / "postgres_tpcc.txt"
header = txt_path.read_text(encoding="utf-8", errors="replace").rstrip() + "\n\n"
lines = []
lines.append("== postgres_tpcc throughput ==")
lines.append(f"concurrency: {data['concurrency']}")
lines.append(f"txn_attempts: {data.get('txn_attempts', data.get('transactions', 0))}")
lines.append(f"txn_successes: {data.get('txn_successes', 0)}")
lines.append(f"success_rate_pct: {data.get('success_rate_pct', 0.0):.2f}")
lines.append(f"elapsed_s: {data['elapsed_s']:.3f}")
lines.append(f"txns_per_s (successful only): {data['txns_per_s']:.1f}")
lines.append(f"attempts_per_s (all tries): {data.get('attempts_per_s', 0.0):.1f}")
lines.append(f"new_orders (successful only): {data['new_orders']}")
lines.append(f"tpmC: {data['tpmC']:.1f}")
lines.append(f"p50_ms: {data['p50_ms']:.2f}  p95_ms: {data['p95_ms']:.2f}  p99_ms: {data['p99_ms']:.2f}")
lines.append(f"failed_attempts: {data.get('err', 0)}")
if data.get("txn_log_path"):
    lines.append(f"txn_log: {data['txn_log_path']}")
if data.get("txn_log_truncated"):
    lines.append("txn_log_truncated: true")
txt_path.write_text(header + "\n".join(lines) + "\n", encoding="utf-8")
print("\n".join(lines))
PY

if [[ "${POSTGRES_TPCC_KEEP_CONTAINER:-}" != "1" ]]; then
  docker rm -f "$name" >/dev/null 2>&1 || true
fi
echo "==> wrote $OUT_DIR_ABS/postgres_tpcc.json, postgres_tpcc.txt, postgres_tpcc_txn.log"

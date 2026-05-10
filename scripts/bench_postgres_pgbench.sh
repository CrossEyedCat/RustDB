#!/usr/bin/env bash
#
# PostgreSQL pgbench baseline (TPC-B-like). Writes tpcc-out/pgbench.txt + pgbench.json
#
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

OUT_DIR="${OUT_DIR:-$ROOT/tpcc-out}"
mkdir -p "$OUT_DIR"
OUT_DIR_ABS="$(cd "$OUT_DIR" && pwd)"

PGBENCH_SCALE="${PGBENCH_SCALE:-10}"
PGBENCH_CLIENTS="${PGBENCH_CLIENTS:-64}"
PGBENCH_JOBS="${PGBENCH_JOBS:-16}"
PGBENCH_DURATION="${PGBENCH_DURATION:-300}"

name="${POSTGRES_CONTAINER_NAME:-postgres-pgbench}"
docker rm -f "$name" >/dev/null 2>&1 || true
docker run -d --name "$name" -p 15440:5432 \
  -e POSTGRES_PASSWORD=postgres \
  -e POSTGRES_DB=pgbench_rustdb_compare \
  postgres:16-alpine \
  -c max_connections=500 >/dev/null

ready=0
for _ in $(seq 1 240); do
  if docker exec "$name" pg_isready -h 127.0.0.1 -p 5432 -U postgres >/dev/null 2>&1; then
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

docker exec "$name" pgbench -h 127.0.0.1 -p 5432 -i -s "$PGBENCH_SCALE" -U postgres pgbench_rustdb_compare >/dev/null

{
  echo "== pgbench =="
  echo "transaction: builtin TPC-B-like (simple protocol)"
  echo "scale: $PGBENCH_SCALE"
  echo "clients: $PGBENCH_CLIENTS"
  echo "jobs: $PGBENCH_JOBS"
  echo "duration_s: $PGBENCH_DURATION"
  echo ""
  docker exec "$name" pgbench -h 127.0.0.1 -p 5432 \
    -c "$PGBENCH_CLIENTS" -j "$PGBENCH_JOBS" -T "$PGBENCH_DURATION" \
    -U postgres pgbench_rustdb_compare
} | tee "$OUT_DIR_ABS/pgbench.txt"

python3 - <<PY
import json, re, pathlib
out = pathlib.Path(r"$OUT_DIR_ABS")
text = (out / "pgbench.txt").read_text(encoding="utf-8", errors="replace")
# latency average (milliseconds), e.g.: latency average = 5.123 ms
lat_m = re.search(r"latency\s+average\s*=\s*([0-9]+(?:\.[0-9]+)?)\s*ms", text, re.I)
latency_avg_ms = float(lat_m.group(1)) if lat_m else 0.0
# tps line
tps_m = re.search(r"^\s*tps\s*=\s*([0-9]+(?:\.[0-9]+)?)\s*\(without initial connection time\)\s*$", text, re.M)
tps = float(tps_m.group(1)) if tps_m else 0.0
obj = {
  "tps": tps,
  "latency_avg_ms": latency_avg_ms,
  "duration_s": float("$PGBENCH_DURATION"),
  "scale": int("$PGBENCH_SCALE"),
  "clients": int("$PGBENCH_CLIENTS"),
  "jobs": int("$PGBENCH_JOBS"),
}
(out / "pgbench.json").write_text(json.dumps(obj, indent=2) + "\n", encoding="utf-8")
print(json.dumps(obj))
PY

docker rm -f "$name" >/dev/null 2>&1 || true
echo "==> wrote $OUT_DIR_ABS/pgbench.txt and pgbench.json"

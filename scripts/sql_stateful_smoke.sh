#!/usr/bin/env bash
# Stateful SQL smoke tests against a RustDB Docker image (persistent /app/data).
#
# Usage:
#   ./scripts/sql_stateful_smoke.sh
#   RUSTDB_IMAGE=ghcr.io/crosseyedcat/rustdb:local ./scripts/sql_stateful_smoke.sh
#   ./scripts/sql_stateful_smoke.sh --compare ghcr.io/crosseyedcat/rustdb:local ghcr.io/crosseyedcat/rustdb:main-type-sha
#
# Requires: docker, bash. Exits 0 if all hard assertions pass; prints full command output.
set -euo pipefail

CONFIG="--config /app/config/config.toml"

query() {
  local vol="$1"
  local sql="$2"
  docker run --rm \
    -e "RUSTDB_TEST_SQL=${sql}" \
    -v "${vol}:/app/data" \
    "${RUSTDB_IMAGE}" \
    sh -c 'rustdb --config /app/config/config.toml query "$RUSTDB_TEST_SQL"'
}

FAILS=0
fail() {
  echo "ASSERT FAIL: $*" >&2
  FAILS=$((FAILS + 1))
}

run_suite() {
  local name="$1"
  RUSTDB_IMAGE="$2"
  local vol="rustdb-ss-${name}-$$"

  echo ""
  echo "#####################################################################"
  echo "# Image: ${RUSTDB_IMAGE}"
  echo "#####################################################################"

  docker volume rm -f "$vol" >/dev/null 2>&1 || true
  docker volume create "$vol" >/dev/null

  cleanup() {
    docker volume rm -f "$vol" >/dev/null 2>&1 || true
  }
  trap cleanup EXIT

  echo ""
  echo "==> 1) INSERT (container A) -> SELECT (container B)"
  out1=$(query "$vol" "INSERT INTO ss_a (n) VALUES (10)" 2>&1) || true
  echo "$out1"
  echo "$out1" | grep -q "rows_affected: 1" || fail "1: INSERT rows_affected"
  out2=$(query "$vol" "SELECT n FROM ss_a" 2>&1) || true
  echo "$out2"
  echo "$out2" | grep -q "Integer(10)" || fail "1: persisted SELECT Integer(10)"

  echo ""
  echo "==> 2) Second INSERT + ORDER BY (new container)"
  out3=$(query "$vol" "INSERT INTO ss_a (n) VALUES (5)" 2>&1) || true
  echo "$out3"
  out4=$(query "$vol" "SELECT n FROM ss_a ORDER BY n" 2>&1) || true
  echo "$out4"
  echo "$out4" | grep -q "Integer(5)" || fail "2: ORDER BY row 5"
  echo "$out4" | grep -q "Integer(10)" || fail "2: ORDER BY row 10"

  echo ""
  echo "==> 3) CREATE TABLE (typed) + INSERT + SELECT"
  out5=$(query "$vol" "CREATE TABLE ss_ct (x INTEGER)" 2>&1) || true
  echo "$out5"
  echo "$out5" | grep -q "Success" || echo "WARN: CREATE TABLE may parse differently on older images"
  out6=$(query "$vol" "INSERT INTO ss_ct (x) VALUES (100)" 2>&1) || true
  echo "$out6"
  out7=$(query "$vol" "SELECT x FROM ss_ct" 2>&1) || true
  echo "$out7"
  echo "$out7" | grep -q "Integer(100)" || fail "3: CREATE/INSERT/SELECT x=100"

  echo ""
  echo "==> 4) UPDATE + readback"
  query "$vol" "INSERT INTO ss_u (k) VALUES (1)" >/dev/null
  out8=$(query "$vol" "UPDATE ss_u SET k = 2 WHERE k = 1" 2>&1) || true
  echo "$out8"
  echo "$out8" | grep -q "rows_affected: 1" || fail "4: UPDATE rows_affected"
  out9=$(query "$vol" "SELECT k FROM ss_u" 2>&1) || true
  echo "$out9"
  echo "$out9" | grep -q "Integer(2)" || fail "4: UPDATE readback"

  echo ""
  echo "==> 5) DELETE"
  query "$vol" "INSERT INTO ss_d (n) VALUES (1)" >/dev/null
  query "$vol" "INSERT INTO ss_d (n) VALUES (2)" >/dev/null
  out10=$(query "$vol" "DELETE FROM ss_d WHERE n = 1" 2>&1) || true
  echo "$out10"
  echo "$out10" | grep -q "rows_affected: 1" || fail "5: DELETE rows_affected"
  out11=$(query "$vol" "SELECT n FROM ss_d" 2>&1) || true
  echo "$out11"
  echo "$out11" | grep -q "Integer(2)" || fail "5: DELETE survivor"
  echo "$out11" | grep -q "Integer(1)" && fail "5: deleted row still visible" || true

  echo ""
  echo "==> 6) INSERT ... SELECT"
  query "$vol" "INSERT INTO ss_isrc (v) VALUES (7)" >/dev/null
  out12=$(query "$vol" "INSERT INTO ss_idst (v) SELECT v FROM ss_isrc WHERE v = 7" 2>&1) || true
  echo "$out12"
  echo "$out12" | grep -q "rows_affected: 1" || fail "6: INSERT SELECT rows_affected"
  out13=$(query "$vol" "SELECT v FROM ss_idst" 2>&1) || true
  echo "$out13"
  echo "$out13" | grep -q "Integer(7)" || fail "6: INSERT SELECT readback"

  echo ""
  echo "==> 7) INNER JOIN (soft check — known weak on some tags)"
  query "$vol" "CREATE TABLE ss_j1 (id INTEGER, v INTEGER)" >/dev/null 2>&1 || true
  query "$vol" "CREATE TABLE ss_j2 (id INTEGER, w INTEGER)" >/dev/null 2>&1 || true
  query "$vol" "INSERT INTO ss_j1 (id, v) VALUES (1, 10)" >/dev/null
  query "$vol" "INSERT INTO ss_j2 (id, w) VALUES (1, 20)" >/dev/null
  out14=$(query "$vol" "SELECT ss_j1.v, ss_j2.w FROM ss_j1 INNER JOIN ss_j2 ON ss_j1.id = ss_j2.id" 2>&1) || true
  echo "$out14"
  if echo "$out14" | grep -q "Integer(10)" && echo "$out14" | grep -q "Integer(20)"; then
    echo "JOIN OK"
  else
    echo "WARN: JOIN did not return expected Integers (known issue on some published images)"
  fi

  echo ""
  echo "==> 8) BEGIN / COMMIT (noop)"
  out15=$(query "$vol" "BEGIN TRANSACTION" 2>&1) || true
  echo "$out15"
  out16=$(query "$vol" "COMMIT" 2>&1) || true
  echo "$out16"

  trap - EXIT
  cleanup
}

if [[ "${1:-}" == "--compare" ]]; then
  [[ $# -ge 3 ]] || {
    echo "usage: $0 --compare <image-a> <image-b>" >&2
    exit 2
  }
  run_suite "a" "$2"
  run_suite "b" "$3"
else
  RUSTDB_IMAGE="${RUSTDB_IMAGE:-ghcr.io/crosseyedcat/rustdb:main-type-sha}"
  run_suite "single" "$RUSTDB_IMAGE"
fi

echo ""
if [[ "$FAILS" -gt 0 ]]; then
  echo "DONE: $FAILS assertion(s) failed"
  exit 1
fi
echo "DONE: all hard assertions passed"
exit 0

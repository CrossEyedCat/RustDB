#!/usr/bin/env bash
# Stateful SQL smoke tests against a RustDB Docker image (persistent /app/data).
#
# Steps 8+ use `rustdb query --batch-file -` so BEGIN/COMMIT/ROLLBACK share one session
# (each plain `query` invocation is a new process and a new SessionContext).
#
# Usage:
#   ./scripts/sql_stateful_smoke.sh
#   RUSTDB_IMAGE=ghcr.io/crosseyedcat/rustdb:local ./scripts/sql_stateful_smoke.sh
#   ./scripts/sql_stateful_smoke.sh --compare ghcr.io/crosseyedcat/rustdb:local ghcr.io/crosseyedcat/rustdb:main-type-sha
#
# Requires: docker, bash. Exits 0 if all hard assertions pass; prints full command output.
set -euo pipefail

query() {
  local vol="$1"
  local sql="$2"
  docker run --rm \
    -e "RUSTDB_TEST_SQL=${sql}" \
    -v "${vol}:/app/data" \
    "${RUSTDB_IMAGE}" \
    sh -c 'rustdb --config /app/config/config.toml query "$RUSTDB_TEST_SQL"'
}

# One SqlEngine + one SessionContext; one SQL statement per non-empty line (same as CLI --batch-file).
query_batch() {
  local vol="$1"
  local sql="$2"
  docker run --rm -i \
    -v "${vol}:/app/data" \
    "${RUSTDB_IMAGE}" \
    sh -c 'rustdb --config /app/config/config.toml query --batch-file -' <<<"$sql"
}

query_expect_fail() {
  local vol="$1"
  local sql="$2"
  local pat="$3"
  set +e
  local out ec
  out=$(docker run --rm \
    -e "RUSTDB_TEST_SQL=${sql}" \
    -v "${vol}:/app/data" \
    "${RUSTDB_IMAGE}" \
    sh -c 'rustdb --config /app/config/config.toml query "$RUSTDB_TEST_SQL"' 2>&1)
  ec=$?
  set -e
  echo "$out"
  [[ "$ec" -ne 0 ]] || fail "expected non-zero exit: $sql"
  echo "$out" | grep -qiE "$pat" || fail "expected /$pat/ in output for: $sql"
}

query_batch_expect_fail() {
  local vol="$1"
  local sql="$2"
  local pat="$3"
  set +e
  local out ec
  out=$(docker run --rm -i \
    -v "${vol}:/app/data" \
    "${RUSTDB_IMAGE}" \
    sh -c 'rustdb --config /app/config/config.toml query --batch-file -' <<<"$sql" 2>&1)
  ec=$?
  set -e
  echo "$out"
  [[ "$ec" -ne 0 ]] || fail "expected non-zero exit (batch): ${sql//$'\n'/ ; }"
  echo "$out" | grep -qiE "$pat" || fail "expected /$pat/ in batch output"
}

FAILS=0
fail() {
  echo "ASSERT FAIL: $*" >&2
  FAILS=$((FAILS + 1))
}

run_suite() {
  local name="$1"
  RUSTDB_IMAGE="$2"
  # NOTE: do NOT make this `local`: the EXIT trap runs outside this function scope on failure,
  # and `set -u` would treat a local-only variable as unbound.
  vol="rustdb-ss-${name}-$$"

  echo ""
  echo "#####################################################################"
  echo "# Image: ${RUSTDB_IMAGE}"
  echo "#####################################################################"

  docker volume rm -f "$vol" >/dev/null 2>&1 || true
  docker volume create "$vol" >/dev/null
  trap "docker volume rm -f \"$vol\" >/dev/null 2>&1 || true" EXIT

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
  echo "==> 8) Transaction batch: COMMIT persists INSERT"
  query "$vol" "CREATE TABLE ss92_tc (k INT PRIMARY KEY)" >/dev/null
  query_batch "$vol" "BEGIN TRANSACTION
INSERT INTO ss92_tc (k) VALUES (701)
COMMIT"
  out15=$(query "$vol" "SELECT k FROM ss92_tc WHERE k = 701" 2>&1) || true
  echo "$out15"
  echo "$out15" | grep -q "Integer(701)" || fail "8: COMMIT should persist k=701"

  echo ""
  echo "==> 9) Transaction batch: ROLLBACK discards INSERT"
  query_batch "$vol" "BEGIN TRANSACTION
INSERT INTO ss92_tc (k) VALUES (702)
ROLLBACK"
  out16=$(query "$vol" "SELECT k FROM ss92_tc WHERE k = 702" 2>&1) || true
  echo "$out16"
  echo "$out16" | grep -q "Integer(702)" && fail "9: ROLLBACK should remove k=702" || true

  echo ""
  echo "==> 10) COMMIT / ROLLBACK with no active transaction (errors)"
  query_expect_fail "$vol" "COMMIT" "no active transaction|code 2006"
  query_expect_fail "$vol" "ROLLBACK" "no active transaction|code 2006"

  echo ""
  echo "==> 11) Nested BEGIN in one session (error)"
  query_batch_expect_fail "$vol" "BEGIN TRANSACTION
BEGIN TRANSACTION" "already in a transaction|code 2007"

  echo ""
  echo "==> 12) DDL blocked inside explicit transaction"
  query_batch_expect_fail "$vol" "BEGIN TRANSACTION
CREATE TABLE ss92_bad (x INT)" "DDL is not supported|code 2008"

  echo ""
  echo "==> 13) PRIMARY KEY violation"
  # Keep setup + violation in one batch run (schemas may not persist across processes).
  query_batch_expect_fail "$vol" "CREATE TABLE ss92_pk (id INT PRIMARY KEY)
INSERT INTO ss92_pk (id) VALUES (1)
INSERT INTO ss92_pk (id) VALUES (1)" "PRIMARY KEY|violated|code 2005"

  echo ""
  echo "==> 14) FOREIGN KEY: missing parent row"
  query_batch_expect_fail "$vol" "CREATE TABLE ss92_par (id INT PRIMARY KEY)
CREATE TABLE ss92_ch (pid INT REFERENCES ss92_par(id))
INSERT INTO ss92_ch (pid) VALUES (99)" "missing parent row|foreign key|code 2005"

  echo ""
  echo "==> 15) FOREIGN KEY: parent DELETE blocked while child exists"
  query_batch_expect_fail "$vol" "CREATE TABLE ss92_par2 (id INT PRIMARY KEY)
CREATE TABLE ss92_ch2 (pid INT REFERENCES ss92_par2(id))
INSERT INTO ss92_par2 (id) VALUES (5)
INSERT INTO ss92_ch2 (pid) VALUES (5)
DELETE FROM ss92_par2 WHERE id = 5" "foreign key references exist|code 2005"

  echo ""
  echo "==> 16) DROP TABLE RESTRICT vs CASCADE (FK dependency)"
  query_batch_expect_fail "$vol" "CREATE TABLE ss92_dp (id INT PRIMARY KEY)
CREATE TABLE ss92_dc (pid INT REFERENCES ss92_dp(id))
DROP TABLE ss92_dp" "referenced by foreign key|CASCADE|code 2005"

  # CASCADE drops parent (and child) — verify by failing SELECT after drop.
  query_batch_expect_fail "$vol" "CREATE TABLE ss92_dp2 (id INT PRIMARY KEY)
CREATE TABLE ss92_dc2 (pid INT REFERENCES ss92_dp2(id))
DROP TABLE ss92_dp2 CASCADE
SELECT 1 FROM ss92_dc2" "does not exist|code 2005"

  echo ""
  echo "==> 17) ALTER TABLE ADD CONSTRAINT UNIQUE + violation"
  query_batch_expect_fail "$vol" "CREATE TABLE ss92_al (a INT)
INSERT INTO ss92_al (a) VALUES (1)
INSERT INTO ss92_al (a) VALUES (2)
ALTER TABLE ss92_al ADD CONSTRAINT ss92_uq UNIQUE (a)
INSERT INTO ss92_al (a) VALUES (1)" "UNIQUE constraint|violated|code 2005"

  echo ""
  echo "==> 18) NOT NULL violation"
  query_batch "$vol" "CREATE TABLE ss92_nn (a INT NOT NULL)"
  query_batch_expect_fail "$vol" "INSERT INTO ss92_nn (a) VALUES (NULL)" "NOT NULL|code 2005"

  echo ""
  echo "==> 19) CHECK violation"
  query_batch "$vol" "CREATE TABLE ss92_ck (b INT CHECK (b > 0))"
  query_batch_expect_fail "$vol" "INSERT INTO ss92_ck (b) VALUES (0)" "CHECK constraint|code 2005"

  echo ""
  echo "==> 20) ORDER BY + LIMIT + OFFSET"
  query "$vol" "CREATE TABLE ss92_lim (v INTEGER)" >/dev/null
  query "$vol" "INSERT INTO ss92_lim (v) VALUES (10)" >/dev/null
  query "$vol" "INSERT INTO ss92_lim (v) VALUES (20)" >/dev/null
  query "$vol" "INSERT INTO ss92_lim (v) VALUES (30)" >/dev/null
  out_lim=$(query "$vol" "SELECT v FROM ss92_lim ORDER BY v ASC LIMIT 2 OFFSET 1" 2>&1) || true
  echo "$out_lim"
  echo "$out_lim" | grep -q "Integer(20)" || fail "20: LIMIT/OFFSET expected 20"
  echo "$out_lim" | grep -q "Integer(30)" || fail "20: LIMIT/OFFSET expected 30"
  echo "$out_lim" | grep -q "Integer(10)" && fail "20: OFFSET should skip 10" || true

  echo ""
  echo "==> 21) GROUP BY + HAVING (light SQL92 aggregate check)"
  query "$vol" "CREATE TABLE ss92_gb (a INTEGER, t INTEGER)" >/dev/null
  query "$vol" "INSERT INTO ss92_gb (a, t) VALUES (1, 1)" >/dev/null
  query "$vol" "INSERT INTO ss92_gb (a, t) VALUES (1, 2)" >/dev/null
  query "$vol" "INSERT INTO ss92_gb (a, t) VALUES (2, 3)" >/dev/null
  out_gb=$(query "$vol" "SELECT a, COUNT(*) FROM ss92_gb GROUP BY a HAVING COUNT(*) > 1 ORDER BY a ASC" 2>&1) || true
  echo "$out_gb"
  echo "$out_gb" | grep -q "Integer(1)" || fail "21: GROUP BY/HAVING expected group a=1"

  trap - EXIT
  docker volume rm -f "$vol" >/dev/null 2>&1 || true
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

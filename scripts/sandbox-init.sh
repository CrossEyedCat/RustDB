#!/usr/bin/env bash
# Build RustDB and seed the ephemeral sandbox database (TPC-C minimal schema).
#
# Usage:
#   ./scripts/sandbox-init.sh
#   RUSTDB_SANDBOX_DATA=/tmp/sandbox-test ./scripts/sandbox-init.sh
#
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

SANDBOX_DATA="${RUSTDB_SANDBOX_DATA:-$ROOT/sandbox-data}"
BASE_CONFIG="${RUSTDB_CONFIG:-$ROOT/config/sandbox.toml}"
RUSTDB_BIN="${RUSTDB_BIN:-$ROOT/target/release/rustdb}"
SEED_IN="$ROOT/scripts/tpcc_seed.sql"
SEED_FILTERED="${SANDBOX_DATA}/.tpcc_seed.filtered.sql"
EFFECTIVE_CONFIG="${SANDBOX_DATA}/.sandbox-config.toml"

echo "==> RustDB sandbox init"
echo "    data:   $SANDBOX_DATA"
echo "    config: $EFFECTIVE_CONFIG"

rm -rf "$SANDBOX_DATA"
mkdir -p "$SANDBOX_DATA"

if [[ ! -f "$BASE_CONFIG" ]]; then
  echo "missing base config: $BASE_CONFIG" >&2
  exit 1
fi

# Resolve relative data_directory to an absolute path for stable CLI opens.
SANDBOX_DATA_ABS="$(cd "$(dirname "$SANDBOX_DATA")" && pwd)/$(basename "$SANDBOX_DATA")"
sed "s|^data_directory = .*|data_directory = \"${SANDBOX_DATA_ABS}\"|" "$BASE_CONFIG" > "$EFFECTIVE_CONFIG"

echo "==> cargo build --release --bin rustdb"
cargo build --release --bin rustdb

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

echo "==> seeding via batch SQL"
"$RUSTDB_BIN" --config "$EFFECTIVE_CONFIG" query --batch-file "$SEED_FILTERED"

echo "==> smoke query"
"$RUSTDB_BIN" --config "$EFFECTIVE_CONFIG" query "SELECT w_id, w_name FROM warehouse"

cat <<EOF

Sandbox ready.

  $RUSTDB_BIN --config $EFFECTIVE_CONFIG query "SELECT w_id FROM warehouse"
  ./scripts/sandbox-reset.sh

EOF

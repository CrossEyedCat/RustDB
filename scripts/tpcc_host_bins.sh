#!/usr/bin/env bash
# Resolve host load-generator binaries (rustdb_tpcc / postgres_tpcc).
#
# Env:
#   TPCC_CARGO_PROFILE — debug|release (default: release for throughput benches)
#
tpcc_host_bins_init() {
  TPCC_CARGO_PROFILE="${TPCC_CARGO_PROFILE:-release}"
  case "$TPCC_CARGO_PROFILE" in
    debug | release) ;;
    *)
      echo "ERROR: invalid TPCC_CARGO_PROFILE=$TPCC_CARGO_PROFILE (want debug|release)" >&2
      return 1
      ;;
  esac
}

tpcc_build_bin() {
  local bin="$1"
  tpcc_host_bins_init || return 1
  echo "==> build ${bin} (host, ${TPCC_CARGO_PROFILE})"
  cargo build -q --"${TPCC_CARGO_PROFILE}" --bin "$bin"
}

tpcc_bin_path() {
  local bin="$1"
  tpcc_host_bins_init || return 1
  local root="${TPCC_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
  local path="$root/target/${TPCC_CARGO_PROFILE}/${bin}"
  if [[ "$(uname -s 2>/dev/null || true)" == MINGW* || "$(uname -s 2>/dev/null || true)" == MSYS* ]]; then
    path="${path}.exe"
  fi
  printf '%s' "$path"
}

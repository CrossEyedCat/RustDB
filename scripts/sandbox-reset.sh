#!/usr/bin/env bash
# Wipe sandbox data and re-run seed (same as first-time Codespace setup).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
exec "$ROOT/scripts/sandbox-init.sh"

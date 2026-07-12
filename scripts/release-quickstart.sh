#!/usr/bin/env bash
# P2.2 fresh-machine gate: build a release binary (or accept PRAMEN_BIN),
# then run the measured quickstart against the ten-minute bar. This is the
# local stand-in for "download release binary → enriched rows" on a machine
# that has never seen the repo.
#
# Usage:
#   PRAMEN_POSTGRES_DSN=postgres://... ./scripts/release-quickstart.sh
#   PRAMEN_BIN=/path/to/pramen ./scripts/release-quickstart.sh  # skip build
set -euo pipefail
cd "$(dirname "$0")/.."

step() { printf '\n==> %s\n' "$*"; }

if [ -z "${PRAMEN_BIN:-}" ]; then
    step "build release binary"
    cargo build --release -p pramen --quiet
    PRAMEN_BIN=target/release/pramen
fi

if [ ! -x "$PRAMEN_BIN" ]; then
    echo "FAIL: PRAMEN_BIN is not executable: $PRAMEN_BIN" >&2
    exit 1
fi

step "verify binary"
"$PRAMEN_BIN" --version

step "measured quickstart (release profile)"
PRAMEN_BIN="$PRAMEN_BIN" ./scripts/quickstart.sh

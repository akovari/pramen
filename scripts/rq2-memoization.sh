#!/usr/bin/env bash
# Regenerate RQ2 (E2.2) memoization metrics under docs/research/.
# Offline-only: mock provider + temporary SQLite ledgers. No cloud, no secrets.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

JSON="${1:-docs/research/rq2-memoization-metrics.json}"
mkdir -p "$(dirname "$JSON")"

echo "Running offline RQ2 memoization suite…"
cargo run -p pramen-ai --example rq2_memoization -- --json "$JSON"

echo
echo "Done. Embed the printed markdown table in docs/research/rq2-memoization.md"
echo "if the numbers changed, and keep the JSON committed beside it."

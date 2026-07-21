#!/usr/bin/env bash
# E2.4: one-command regeneration (and --check) for published research figures.
#
#   ./scripts/reproduce-artifact.sh              # regenerate offline figures
#   ./scripts/reproduce-artifact.sh --check      # fail if committed artifacts drift
#   ./scripts/reproduce-artifact.sh --with-postgres   # also run e2e bench (needs DSN)
#
# See docs/research/artifact-manifest.json and docs/research/artifact-evaluation.md.
set -euo pipefail
cd "$(dirname "$0")/.."

CHECK=0
WITH_PG=0
for arg in "$@"; do
  case "$arg" in
    --check) CHECK=1 ;;
    --with-postgres) WITH_PG=1 ;;
    -h|--help)
      sed -n '2,10p' "$0"
      exit 0
      ;;
    *)
      echo "error: unknown argument: $arg" >&2
      exit 2
      ;;
  esac
done
if [ "${REPRODUCE_WITH_POSTGRES:-}" = "1" ]; then
  WITH_PG=1
fi

RQ2_JSON=docs/research/rq2-memoization-metrics.json
FRONTIER_MD=docs/research/e2-1-dispatch-frontier.md
MANIFEST=docs/research/artifact-manifest.json

if [ ! -f "$MANIFEST" ]; then
  echo "error: missing $MANIFEST" >&2
  exit 1
fi

tmpdir=
cleanup() {
  if [ -n "${tmpdir}" ] && [ -d "${tmpdir}" ]; then
    rm -rf "${tmpdir}"
  fi
}
trap cleanup EXIT

tmpdir="$(mktemp -d "${TMPDIR:-/tmp}/pramen-reproduce.XXXXXX")"

echo "==> pin: cargo test -p pramen-ai reuse"
cargo test -q -p pramen-ai reuse

if [ "$CHECK" -eq 1 ]; then
  echo "==> check: RQ2 metrics (scenarios; ignore generatedAtUnix)"
  tmp_rq2="$tmpdir/rq2.json"
  cargo run -q -p pramen-ai --example rq2_memoization -- --json "$tmp_rq2"
  python3 - "$RQ2_JSON" "$tmp_rq2" <<'PY'
import json, sys
committed = json.loads(open(sys.argv[1], encoding="utf-8").read())
fresh = json.loads(open(sys.argv[2], encoding="utf-8").read())
for key in ("task", "provider", "ledger", "scenarios"):
    if committed.get(key) != fresh.get(key):
        print(f"error: RQ2 drift on {key!r}; run: mise run reproduce", file=sys.stderr)
        sys.exit(1)
print("rq2-memoization: ok")
PY

  echo "==> check: E2.1 dispatch frontier"
  tmp_frontier="$tmpdir/frontier.md"
  cargo run -q -p pramen -- ai dispatch-plan --sweep --out "$tmp_frontier"
  if ! cmp -s "$FRONTIER_MD" "$tmp_frontier"; then
    echo "error: $FRONTIER_MD is stale; run: mise run reproduce" >&2
    diff -u "$FRONTIER_MD" "$tmp_frontier" || true
    exit 1
  fi
  echo "e2-1-dispatch-frontier: ok"

  echo "==> check: compare scoreboard"
  ./scripts/compare-scoreboard.sh --check

  echo
  echo "reproduce-artifact --check: all offline figures ok"
  exit 0
fi

echo "==> regenerate: RQ2 metrics"
./scripts/rq2-memoization.sh "$RQ2_JSON"

echo "==> regenerate: E2.1 dispatch frontier"
cargo run -q -p pramen -- ai dispatch-plan --sweep --out "$FRONTIER_MD"

echo "==> regenerate: compare scoreboard"
./scripts/compare-scoreboard.sh

if [ "$WITH_PG" -eq 1 ]; then
  if [ -z "${PRAMEN_POSTGRES_DSN:-}" ]; then
    echo "error: --with-postgres requires PRAMEN_POSTGRES_DSN" >&2
    exit 1
  fi
  echo "==> regenerate: e2e bench suite (postgres)"
  ./scripts/bench.sh
else
  echo "==> skip: e2e bench (pass --with-postgres + PRAMEN_POSTGRES_DSN to include)"
fi

echo
echo "reproduce-artifact: offline figures refreshed."
echo "If RQ2 numbers changed, update the table in docs/research/rq2-memoization.md."
echo "AE checklist: docs/research/artifact-evaluation.md"

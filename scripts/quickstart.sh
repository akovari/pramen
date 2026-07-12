#!/usr/bin/env bash
# The measured quickstart (P1.18): one binary + one YAML file -> governed,
# AI-enriched rows in PostgreSQL, timed end to end against the ten-minute
# bar. CI runs this script, so the documented steps cannot silently drift
# from what actually works.
#
# Requirements: a `pramen` binary (PRAMEN_BIN, default target/release/pramen),
# `psql`, and PRAMEN_POSTGRES_DSN pointing at a writable database.
#
# The pipeline is the shipped examples/local-tickets-ai-classify.yaml:
# NDJSON tickets -> SQL cleanup -> governed ai.classify (deterministic mock
# provider: offline, zero cost) -> transactional COPY into PostgreSQL.
set -euo pipefail
cd "$(dirname "$0")/.."
started=$(date +%s)

PRAMEN_BIN=${PRAMEN_BIN:-target/release/pramen}
: "${PRAMEN_POSTGRES_DSN:?set PRAMEN_POSTGRES_DSN, e.g. postgres://postgres:quickstart@localhost:5432/postgres}"
ROWS=${QUICKSTART_ROWS:-1000}

step() { printf '\n==> %s\n' "$*"; }

step "1/5 input: $ROWS synthetic support tickets (NDJSON)"
mkdir -p /tmp/pramen-ai-input
awk -v rows="$ROWS" 'BEGIN {
  for (i = 1; i <= rows; i++)
    printf("{\"id\": %d, \"description\": \"ticket %d: subsystem %d reports a fault\"}\n", i, i, i % 7)
}' > /tmp/pramen-ai-input/tickets.ndjson

step "2/5 target table"
psql "$PRAMEN_POSTGRES_DSN" -q <<'SQL'
CREATE SCHEMA IF NOT EXISTS analytics;
DROP TABLE IF EXISTS analytics.tickets_classified;
CREATE TABLE analytics.tickets_classified (
    id          bigint NOT NULL,
    description text NOT NULL,
    category    text NOT NULL,
    confidence  double precision NOT NULL
);
SQL

step "3/5 validate the pipeline document"
"$PRAMEN_BIN" validate examples/local-tickets-ai-classify.yaml

step "4/5 run (mock provider: offline, deterministic, zero cost)"
ledger_dir=$(mktemp -d)
PRAMEN_LEDGER_PATH="$ledger_dir/ledger.sqlite" \
  "$PRAMEN_BIN" run examples/local-tickets-ai-classify.yaml

step "5/5 verify enriched rows"
count=$(psql "$PRAMEN_POSTGRES_DSN" -tA -c "SELECT count(*) FROM analytics.tickets_classified")
if [ "$count" -ne "$ROWS" ]; then
    echo "FAIL: expected $ROWS rows in analytics.tickets_classified, found $count" >&2
    exit 1
fi
psql "$PRAMEN_POSTGRES_DSN" -c \
  "SELECT id, category, round(confidence::numeric, 2) AS confidence
   FROM analytics.tickets_classified ORDER BY id LIMIT 5"

elapsed=$(( $(date +%s) - started ))
echo "quickstart complete: $count enriched rows in ${elapsed}s"
if [ "$elapsed" -gt 600 ]; then
    echo "FAIL: quickstart exceeded the ten-minute bar (${elapsed}s > 600s)" >&2
    exit 1
fi

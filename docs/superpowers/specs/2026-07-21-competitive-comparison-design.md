# Competitive comparison — design

Date: 2026-07-21  
Status: approved for implementation (orientation + scoreboard)

## Goal

Ship both:

1. **Buyer orientation** — when to pick Pramen vs best-of-class alternatives
   in comparable scenarios, with honest caveats (architecture §2).
2. **Research scoreboard** — dated, reproducible measurements that agents
   and humans regenerate as features land (feeds E2.3 / paper figures).

## Non-goals

- Headline claims without a linked report.
- Forcing live Bedrock / warehouse AI numbers into CI (credentials + budget).
- Claiming Pramen replaces Flink/Spark/warehouse platforms.

## Shape

| Artifact | Owner | Regenerated? |
| --- | --- | --- |
| `docs/compare/orientation.md` | humans/agents (prose) | no (edit deliberately) |
| `docs/benchmarks/compare-scoreboard.json` | harness | yes |
| `docs/benchmarks/compare-scoreboard.md` | harness from JSON | yes |
| `site/.../project/comparison.md` | composes orientation + scoreboard | hand + links |
| `compare/<competitor>/` | harness configs | yes when scenario runs |
| README “Compared to alternatives” | short pointer + 2–3 linked headlines | when scoreboard changes |

## Cadence

- **Offline legs (merge-triggered):** when a PR changes load path, COPY,
  ledger/reuse, or `scripts/bench.sh` / compare harness, regenerate the
  scoreboard (`mise run compare-scoreboard`). CI runs `--check` so drift fails.
- **Cloud / competitor-AI legs:** env-gated (`COMPARE_REDPANDA=1`,
  `COMPARE_DOCETL=1`, AWS credentials); prefer weekly/monthly budget-capped
  runs, not PR CI. Results update the same JSON with `status: measured`
  and a dated report path.

## Scenarios (v1)

| ID | Comparables | Status at ship |
| --- | --- | --- |
| `pg-load-path` | Pramen → PG, DuckDB → PG, DataFusion (no sink), `psql \copy` | measured (cite existing reports) |
| `memoization-reuse` | Pramen ledger vs naive re-dispatch | measured (RQ2 JSON) |
| `redpanda-connect-ai` | Redpanda Connect `aws_bedrock_chat` equiv. | harness_ready |
| `docetl-extract` | DocETL structured extraction | harness_ready |
| `warehouse-ai-sql` | Databricks/Snowflake/BigQuery AI SQL | deferred (qualitative only) |

## Claim rules

1. Every numeric claim on the site/README links to a report under
   `docs/benchmarks/`, `docs/spikes/`, or `docs/research/`.
2. Scoreboard rows carry `status`: `measured` | `harness_ready` |
   `deferred` | `not_applicable`.
3. Qualitative “when they win” lives only in orientation prose.

## Agent protocol

Documented in `AGENTS.md` § Competitive comparison discipline.

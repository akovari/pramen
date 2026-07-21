# When to choose Pramen (and when not to)

Hand-written orientation. Numeric claims belong on the
[competitive scoreboard](../benchmarks/compare-scoreboard.md) and must link
a dated report. This page stays qualitative so it does not fight the
generator.

Architecture §2 remains the authority for product position; this page is
the public, scenario-shaped summary.

## Comparable alternatives

| Family | Best of class (examples) | Comparable Pramen scenario |
| --- | --- | --- |
| Config-driven pipelines + AI processors | Redpanda Connect (`aws_bedrock_chat`, OpenAI processors) | Object store → enrich → operational DB |
| In-warehouse AI SQL | Databricks `ai_query`, Snowflake Cortex, BigQuery `AI.GENERATE_TABLE` | Enrichment when data already lives in that warehouse |
| Columnar engines | DuckDB, DataFusion, Polars | Deterministic transform + load microbenchmarks |
| Structured LLM ETL research systems | DocETL | Schema-bound extraction quality / cost |
| Distributed stream processors | Flink, Spark, RisingWave, Arroyo | **Not** the comparison set — different problem |

## Where Pramen should win

1. **Enrich into an operational database** (S3/Parquet → typed columns in
   PostgreSQL/Aurora) without warehouse ingest + reverse-ETL, with provider
   batch pricing and idempotent bulk load.
2. **Interruptible semantic backfills** — ledger reuse + batch job
   reconciliation so completed work is not re-billed.
3. **Incremental re-enrichment** — content-addressed work keys bill only
   changed records.
4. **Residency-constrained inference** — pinned-region Bedrock or
   self-hosted OpenAI-compatible models with per-row provenance.
5. **Spend control as a runtime property** — budgets and breakers before
   dispatch.

## Where the alternative usually wins

- **Warehouse AI SQL** when the data already lives in that warehouse and
  results should stay there — Pramen adds a hop you do not need.
- **Redpanda Connect** when you need its connector catalog, per-message
  streaming topology, or light online AI without a durable ledger.
- **DuckDB / DataFusion alone** for in-process analytics that never needs
  governed LLM enrichment or a PostgreSQL delivery contract.
- **Flink / Spark / RisingWave** for stateful distributed streaming,
  large joins, and cluster scheduling — out of scope for Pramen’s lean
  profile.
- **DocETL** (today) for research-grade extraction pipelines where a
  Python-centric toolchain is acceptable and database delivery is secondary.

## Honest caveat

This space moves quickly — especially warehouse vendors. The wedge is
durable where residency, destination, model neutrality, or cost economics
keep the workload outside a single vendor platform. See architecture §2.

## How measurements stay current

See [AGENTS.md — Competitive comparison discipline](../../AGENTS.md) and
`mise run compare-scoreboard`. Offline scoreboard legs regenerate on
relevant merges; competitor AI harnesses under `compare/` flip from
`harness_ready` to `measured` only after a dated report lands.

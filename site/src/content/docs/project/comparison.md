---
title: Compared to alternatives
description: When to choose Pramen versus best-of-class tools in comparable scenarios — with a measured scoreboard that stays current.
---

Pramen is not a Flink/Spark replacement and not a warehouse. It competes in
a narrower band: **object storage → governed enrichment → operational
PostgreSQL**, crash-safe and budgeted. This page has two layers:

1. **Orientation** — when each alternative usually wins (qualitative).
2. **Scoreboard** — dated measurements; every number links a report.

Full prose: [`docs/compare/orientation.md`](https://github.com/akovari/pramen/blob/main/docs/compare/orientation.md).  
Generated tables: [`docs/benchmarks/compare-scoreboard.md`](https://github.com/akovari/pramen/blob/main/docs/benchmarks/compare-scoreboard.md).

## When to choose what

| If your job is… | Prefer | Why |
| --- | --- | --- |
| Enrich Parquet/NDJSON into **Aurora/Postgres** with budgets, reuse, batch pricing | **Pramen** | One binary, ledger + COPY delivery contract |
| Data already in Databricks/Snowflake/BigQuery and results stay there | **Warehouse AI SQL** | No extra hop |
| Broad connectors + light online AI in a streaming topology | **Redpanda Connect** | Catalog and ops model |
| In-process analytics, no governed LLM + PG contract | **DuckDB / DataFusion** | Less machinery |
| Stateful distributed streaming / huge joins | **Flink / Spark / …** | Different problem class |

Honest caveat: warehouse vendors are investing heavily. Pramen’s wedge is
strongest where residency, destination, model neutrality, or cost economics
sit outside a single platform — see [architecture §2](https://github.com/akovari/pramen/blob/main/docs/architecture.md).

## Scoreboard (measured + harness-ready)

Offline legs regenerate on relevant merges (`mise run compare-scoreboard`).
Competitor AI harnesses live under [`compare/`](https://github.com/akovari/pramen/tree/main/compare)
and stay `harness_ready` until a dated report lands.

### PostgreSQL load path

From the [v1 bench report](https://github.com/akovari/pramen/blob/main/docs/benchmarks/2026-07-12-v1.md)
(Apple M3 laptop — relative evidence):

| System | Rows out/s | Notes |
| --- | --- | --- |
| **Pramen → PostgreSQL** | 434k–581k | ~7× less CPU than DuckDB→PG on the same server |
| DuckDB → PostgreSQL | 403k–620k | Wall-time tie (server-dominated); ~45 MiB RSS |
| DataFusion direct (no sink) | ~4M | Engine ceiling |
| `psql \copy` CSV | 117k | [S1.3](https://github.com/akovari/pramen/blob/main/docs/spikes/s1-3-postgres-copy.md): Pramen binary COPY **3.1×** faster |

### Semantic reuse (offline mock)

From [RQ2](https://github.com/akovari/pramen/blob/main/docs/research/rq2-memoization.md):

| Scenario | Result |
| --- | --- |
| Crash/replay | **100%** reuse; **0** tokens on replay |
| Batch crash reconcile | **0** rebill |
| Duplicate-heavy (200/20) | **90%** savings vs naive |

### Not measured yet (harnesses ready)

| Scenario | Harness |
| --- | --- |
| Redpanda Connect AI processor | [`compare/redpanda-connect/`](https://github.com/akovari/pramen/tree/main/compare/redpanda-connect) |
| DocETL extraction | [`compare/docetl/`](https://github.com/akovari/pramen/tree/main/compare/docetl) |
| Warehouse AI SQL | deferred (qualitative only) |

## Keeping numbers honest

Documented for contributors and agents in
[`AGENTS.md`](https://github.com/akovari/pramen/blob/main/AGENTS.md):

- No public numeric claim without a report link.
- Offline scoreboard: regenerate when load path / ledger / bench / compare
  harness changes; CI `--check` fails on drift.
- Cloud competitor legs: env-gated, budget-capped — not PR-blocking.

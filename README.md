# Pramen

Pramen is an exploratory open-source project for a fast, portable data movement
and transformation runtime.

The name means **spring** or **source of water** in Czech.

**Documentation: [akovari.github.io/pramen](https://akovari.github.io/pramen/)** —
quickstart, concepts, cookbook, reference, and measured results.

## Direction

Pramen is intended to:

- process bounded and unbounded data with one execution model;
- move Apache Arrow record batches through sources, transforms, and sinks;
- enrich data through schema-bound LLM transformations with durable result
  reuse, validation, budgets, and provenance;
- transform data with built-in SQL/expressions, and later with sandboxed,
  ahead-of-time compilable WebAssembly components;
- connect object storage across AWS, Azure, and Google Cloud to analytical
  databases;
- scale through independent workers before introducing cluster coordination;
- prioritize predictable resources, observable behavior, and explicit delivery
  guarantees.

The v1 promise is deliberately narrow and measurable: **download one static
binary, write one YAML file, and get governed semantic enrichment into
PostgreSQL in under ten minutes** — no services, drivers, or toolchains.

The first proposed end-to-end path is:

```text
S3 / local files
        |
   Parquet reader
        |
  Arrow RecordBatch
        |
   SQL transform
        |
 governed AI extract
 (online or batch)
        |
 native Postgres COPY
        |
Aurora PostgreSQL / RDS PostgreSQL
```

A pipeline is one file. Illustrative shape:

```yaml
apiVersion: pramen.dev/v1alpha1
kind: Pipeline
spec:
  source:
    type: object_store
    url: s3://tickets/raw/
    format: { type: parquet }
  transforms:
    - id: normalize
      type: sql
      query: SELECT ticket_id, trim(description) AS description FROM input
    - id: classify
      type: ai.extract
      model: enrichment
      inputs: [description]
      output:
        fields:
          - { name: category, type: utf8, nullable: false }
          - { name: priority, type: utf8, nullable: false }
      budget: { maxOutputTokensPerRecord: 256 }
  sink:
    type: postgres
    target: support.enriched_tickets
```

`pramen run --smoke` runs the same pipeline with a capped record count, the
fast/cheap model, and a hard cost ceiling — real enriched rows for a few
cents before committing to a full run.

## Why Pramen

Individually, none of Pramen's ingredients are new. The combination is: no
current system pairs a columnar Arrow data plane with governed semantic
operators backed by a durable, content-addressed inference ledger, provider
batch-API scheduling, and database delivery contracts — in one static binary,
with sandboxed WASM components as the extension mechanism.

Where that combination wins today:

- **Object storage to operational databases with AI enrichment.** Warehouse AI
  SQL (Databricks `ai_query`, Snowflake Cortex, BigQuery `AI.GENERATE_TABLE`)
  requires ingesting into the warehouse and reverse-ETL out; pipeline AI
  processors (Redpanda Connect) call models per message at on-demand pricing
  with no result reuse and untyped output.
- **Interruption-proof semantic backfills.** Completed inference is recorded
  durably; a crash or redeploy mid-run does not re-bill processed records, and
  in-flight provider batch jobs are reconciled rather than lost.
- **Incremental re-enrichment.** Content-addressed work keys mean only changed
  records incur model cost on recurring runs.
- **Regulated, residency-constrained processing.** Pinned-region or fully
  self-hosted inference with per-row provenance for audit.
- **AI spend control.** Budgets, circuit breakers, batch pricing, and dedup
  enforced by the runtime, not discovered on the invoice.

The full [architecture document](docs/architecture.md) names the competing
systems and states honestly where each remains the better choice.

## Current status

Early implementation; no stable public API yet. What runs today:

- `pramen validate` and `pramen explain` accept the versioned v1alpha1
  pipeline document, report every validation issue with its path, and ship a
  [generated JSON Schema](docs/schema/pipeline.v1alpha1.schema.json);
- `pramen run` executes pipelines end to end: Parquet or NDJSON — local or
  `s3://` (S3-compatible stores like MinIO included) — → per-batch
  DataFusion SQL → transactional binary `COPY` into PostgreSQL, with
  bounded channels, backpressure, Ctrl-C cancellation, and a run summary
  (see [examples/local-parquet-to-postgres.yaml](examples/local-parquet-to-postgres.yaml));
- checkpointed incremental runs (ADR 0006): file-granular work units on a
  crash-safe append-only store — replaying a finished run loads nothing, a
  grown directory loads only new files;
- `upsert` sink mode: stage + `ON CONFLICT` merge on declared keys, so
  replays are idempotent — the at-least-once contract is pinned by tests
  on both sides (append duplicates, upsert does not);
- run-level cost governance: `maxRunTokens` hard ceilings (ledger reuse
  stays free) and an always-armed circuit breaker against invalid-output
  spikes;
- governed semantic transforms run today: `ai.extract` / `ai.classify` on
  the durable SQLite (WAL) inference ledger — content-addressed work keys,
  result reuse on replay, pre-dispatch token budgets, and strict typed
  output validation — with three providers: deterministic `mock`, any
  OpenAI-compatible endpoint (vLLM, Ollama, llama.cpp), and Amazon Bedrock
  Converse (stub-tested offline per ADR 0005); see
  [examples/local-tickets-ai-classify.yaml](examples/local-tickets-ai-classify.yaml)
  and `pramen ai status`;
- provider-batch execution runs today (`execution: batch`): misses are
  submitted as one asynchronous job whose id is durably recorded per item
  before results are awaited, so a crash after submission reconciles on
  restart instead of resubmitting — submitted work is never billed twice;
  see [examples/local-tickets-ai-classify-batch.yaml](examples/local-tickets-ai-classify-batch.yaml);
- `pramen ai evaluate` measures model quality, cost, and latency on the
  versioned 520-item golden support-ticket corpus
  ([corpora/](corpora/)), through the same provider adapters pipelines
  use, into timestamped diffable reports;
- `pramen run --smoke` rehearses a pipeline cheaply (row cap, clamped
  token ceiling, checkpointing bypassed) before a full run, and the
  documented quickstart is executed and timed in CI on every change
  ([scripts/quickstart.sh](scripts/quickstart.sh));
- the riskiest boundaries are spike-validated with measured results in
  [docs/spikes/](docs/spikes/): durable SQLite inference ledger with 100%
  result reuse and crash recovery (S1.1), bounded-memory Parquet + SQL at
  ~3M rows/s (S1.2), and native binary `COPY` at 3.1x the `psql \copy`
  baseline (S1.3).

The Bedrock/OpenAI batch adapters and the model quality-cost frontier
table are next on the [Phase 1 workstreams](docs/implementation-plan.md).

Read the [product and architecture direction](docs/architecture.md) for the
competitive position, proposed runtime, WASM ABI, connector strategy, delivery
semantics, and phased roadmap. The step-by-step, parallelizable task breakdown
with measurable exit criteria lives in the
[implementation plan](docs/implementation-plan.md); contributor and agent
conventions live in [AGENTS.md](AGENTS.md).

## Initial decisions

- **Core language:** Rust
- **Data plane:** Apache Arrow
- **Lean v1:** one static binary, zero native driver dependencies
- **v1 transforms:** DataFusion SQL/expressions; WebAssembly components are
  the first post-v1 extensibility milestone
- **v1 delivery:** native pure-Rust PostgreSQL `COPY`; ADBC deferred to
  multi-warehouse expansion
- **v1 formats:** Parquet and NDJSON
- **AI transforms:** structured semantic extraction; autonomous agents deferred
- **AI execution:** hosted or self-hosted providers, online or asynchronous
  batch
- **First hosted AI profile:** Amazon Bedrock Converse, online and batch
- **AI residency:** `eu-central-1` only, without cross-region inference
- **Model evaluation:** compare fast/cheap and stronger Bedrock models on the
  same quality-cost benchmark
- **First self-hosted adapter:** vLLM
- **First golden evaluation:** governed support-ticket classification and
  extraction
- **AI governance:** strict schemas, durable result reuse, budgets, provenance,
  and review routing
- **Execution:** unified bounded and unbounded dataflow
- **Scaling:** single worker and shared-nothing first
- **Product shape:** standalone CLI and daemon for platform/data teams
- **Optimization target:** throughput and cost efficiency over per-event latency
- **First vertical:** S3 → SQL transform → semantic extraction → Aurora
  PostgreSQL
- **First production destination:** Amazon Aurora PostgreSQL
- **Research goal:** a peer-reviewed systems paper on cost-optimal,
  restart-safe semantic enrichment; the paper's evaluation and the product
  benchmark are the same artifact
- **Recommended license:** Apache-2.0, to be added when implementation starts

## Immediate next step

The remaining risk spikes and Phase 1 workstreams, in order (each is
developed local-first per ADR 0005; cloud spend only confirms, never
unblocks):

1. the Bedrock/OpenAI batch adapters behind the now-live batch operator
   (P1.8 remainder), then the S2.1 spike numbers on real Bedrock;
2. the model quality-cost frontier table (S2.2 remainder — the corpus
   and `ai evaluate` harness are live; runs against real Bedrock models
   and a local vLLM remain);
3. fault-injection and benchmark suites (P1.19–P1.20);
4. the WASM WIT component round-trip spike, gating the extensibility
   milestone, not v1 (S1.4).

The subsequent AWS acceptance test should compare client-streamed
`COPY FROM STDIN` with Aurora's server-side S3 import where the source
format and transformation permit it.

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
- `pramen run` executes pipelines end to end: Parquet or NDJSON — local,
  `s3://` (MinIO included), `gs://`, or Azure Blob (`az://`, `abfs(s)://`)
  — → per-batch DataFusion SQL → transactional binary `COPY` into
  PostgreSQL, with bounded channels, backpressure, Ctrl-C cancellation, and
  a run summary (see
  [examples/local-parquet-to-postgres.yaml](examples/local-parquet-to-postgres.yaml));
  optional `runtime.residency` validates declared source locations and model
  regions offline (no live cloud lookups);
- checkpointed incremental runs (ADR 0006): file-granular work units on a
  crash-safe store (`file://…` locally, or `postgres://…` /
  `postgresql://…` for the shared fleet backend) — replaying a finished
  run loads nothing, a grown directory (or cloud prefix — unit identity
  from a single `LIST`, MinIO-verified for S3) loads only new files;
- `upsert` sink mode: stage + `ON CONFLICT` merge on declared keys, so
  replays are idempotent — the at-least-once contract is pinned by tests
  on both sides (append duplicates, upsert does not);
- run-level cost governance: `maxRunTokens` hard ceilings (ledger reuse
  stays free) and an always-armed circuit breaker against invalid-output
  spikes;
- governed semantic transforms run today: `ai.extract` / `ai.classify` /
  `ai.generate` on the durable inference ledger (SQLite WAL by default;
  set `PRAMEN_LEDGER_PATH` to a `postgres://` DSN for the shared fleet
  backend) — content-addressed work keys, result reuse on replay,
  pre-dispatch token budgets, and strict typed output validation
  (`ai.generate` requires UTF-8 `maxChars` + `maxOutputTokensPerRecord`)
  — with three providers: deterministic `mock`, any OpenAI-compatible
  endpoint (vLLM, Ollama, llama.cpp), and Amazon Bedrock Converse
  (stub-tested offline per ADR 0005); see
  [examples/local-tickets-ai-classify.yaml](examples/local-tickets-ai-classify.yaml),
  [examples/local-tickets-ai-generate.yaml](examples/local-tickets-ai-generate.yaml),
  and `pramen ai status`;
- provider-batch execution runs today (`execution: batch`): misses are
  submitted as one asynchronous job whose id is durably recorded per item
  before results are awaited, so a crash after submission reconciles on
  restart instead of resubmitting — submitted work is never billed twice;
  the `openai-compat` adapter implements the batch surface via the OpenAI
  Files + Batches APIs (protocol-stub-tested offline), and `bedrock`
  implements model invocation jobs with S3 staging and a `keys.jsonl`
  join fallback (L2-tested against MinIO and a control-plane stub); see
  [examples/local-tickets-ai-classify-batch.yaml](examples/local-tickets-ai-classify-batch.yaml);
- `pramen ai evaluate` measures model quality, cost, and latency on the
  versioned 520-item golden support-ticket corpus
  ([corpora/](corpora/)), through the same provider adapters pipelines
  use, into timestamped diffable reports;
- `onInvalid: review` routes invalid records to a durable review queue —
  withheld from every run, never re-dispatched or re-billed while
  undecided; `pramen ai review` lists/exports the queue, and accepted
  corrections are schema-validated back into the ledger as zero-token
  human-review results (rejections drop permanently);
- `pramen run --smoke` rehearses a pipeline cheaply (row cap, clamped
  token ceiling, checkpointing bypassed) before a full run, and the
  documented quickstart is executed and timed in CI on every change
  ([scripts/quickstart.sh](scripts/quickstart.sh));
- failures are typed and fault-injected: provider timeouts, throttles,
  transport failures, malformed responses, and server errors each carry
  a documented fault class, and a killed database backend mid-`COPY`
  fails typed with the target untouched — all induced offline;
- the riskiest boundaries are spike-validated with measured results in
  [docs/spikes/](docs/spikes/): durable SQLite inference ledger with 100%
  result reuse and crash recovery (S1.1), bounded-memory Parquet + SQL at
  ~3M rows/s (S1.2), and native binary `COPY` at 3.1x the `psql \copy`
  baseline (S1.3);
- a reproducible benchmark suite ([scripts/bench.sh](scripts/bench.sh) +
  Criterion micro-benches): deterministic generated inputs, end-to-end
  throughput / CPU / peak RSS against DataFusion-direct, DuckDB-native,
  and a like-for-like DuckDB→PostgreSQL leg — published runs: 434k–581k
  rows/s into PostgreSQL, wall-time tie with DuckDB's `postgres`
  extension on ~7× less CPU, governance fixed cost under 1 ms per
  semantic record ([docs/benchmarks/](docs/benchmarks/)).

The model quality-cost frontier table and live cloud acceptance legs
(S1.1 Bedrock online, S2.1 batch crash/reconcile, P2.1 1M-record run)
remain blocked on AWS credentials, not code. Phase 2 Group X1
(WASM + OCI, Azure/GCS, `ai.generate`, shared Postgres backends) is
merged in **0.2.0**.

Read the [product and architecture direction](docs/architecture.md) for the
competitive position, proposed runtime, WASM ABI, connector strategy, delivery
semantics, and phased roadmap. The step-by-step, parallelizable task breakdown
with measurable exit criteria lives in the
[implementation plan](docs/implementation-plan.md); contributor and agent
conventions live in [AGENTS.md](AGENTS.md).

## Known limitations (v0.2)

Pramen is lean and measured, not feature-complete. Ship with these stated:

- **Single-node, single-connection sink** — no horizontal scaling, no parallel
  COPY, no fan-out DAGs.
- **PostgreSQL sink only** — ADBC and Flight SQL are Phase 3.
- **Linear pipelines** — one source, ordered transforms, one sink.
- **Bedrock batch minimum job size** — the service enforces a minimum records
  per model invocation job (~1,000); smaller `execution: batch` runs fail at
  provider validation.
- **Cloud acceptance not in CI** — live Bedrock, frontier model runs, and the
  1M-record AWS acceptance test need credentials; PR gates are fully offline.
- **OCI signature verification is a hook** — default allow-all; cosign/sigstore
  and authenticated registry pulls are follow-ups.
- **Review queue is CLI-only** — no web UI yet.
- **Shared Postgres backends** — correct for single-writer / at-least-once;
  exclusive claim leasing for multi-worker fleets is not finished.

The [v0.2 release checklist](docs/release/v0.2-checklist.md) tracks what
must be green before tagging; v0.1 history is in
[docs/release/v0.1-checklist.md](docs/release/v0.1-checklist.md).

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

v0.2 ships Phase 1 plus Phase 2 Group X1 (offline). Remaining:

1. **Live cloud acceptance** (needs AWS credentials): S1.1 Bedrock Converse
   online, S2.1 batch crash/reconcile on real Bedrock, S2.2 model
   quality-cost frontier runs, and P2.1 1M-record S3 → Aurora acceptance.
2. **Phase 2 Group X2**: third-party WASM conformance (X2.1) and a
   reproducible AWS deployment runbook (X2.2).

See the [v0.2 release checklist](docs/release/v0.2-checklist.md) and
[CONTRIBUTING.md](CONTRIBUTING.md) to build from source or install release
binaries.

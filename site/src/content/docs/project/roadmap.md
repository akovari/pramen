---
title: Status and roadmap
description: What works today, what is being built, and in what order.
---

Pramen is developed in the open with a
[task-level plan](https://github.com/akovari/pramen/blob/main/docs/implementation-plan.md)
mirrored to [GitHub issues](https://github.com/akovari/pramen/issues).
This page is the honest summary. Workspace version: **0.2.0**.

## Works today

- **Pipeline document** (`pramen.dev/v1alpha1`): strict parsing, complete
  path-addressed validation, published JSON Schema.
- **CLI**: `validate`, `explain` (text + JSON), `run`, `run --smoke`,
  `ai status`, `ai evaluate`, `ai review`, `transform test`.
- **End-to-end runs**: Parquet or NDJSON — local, `s3://` (MinIO),
  `gs://`, or Azure Blob (`az://` / `abfs(s)://`) — → per-batch DataFusion
  SQL → transactional binary `COPY` into PostgreSQL, with bounded memory,
  backpressure, and Ctrl-C safety. Optional `runtime.residency` validates
  declared source locations and model regions offline.
- **Checkpointed incremental runs**: file-granular work units on a
  crash-safe store — local `file://` JSONL (ADR 0006) or shared
  `postgres://` / `postgresql://` for fleets. Replay loads nothing; a
  grown prefix loads only new objects (LIST identity: key, size,
  last-modified).
- **Upsert sink mode**: stage + `ON CONFLICT` merge on declared keys;
  replays are idempotent.
- **Run-level cost governance**: `maxRunTokens` hard ceiling and an
  always-armed consecutive-invalid circuit breaker.
- **Governed semantic transforms**: `ai.extract` / `ai.classify` /
  `ai.generate` on the durable inference ledger (SQLite WAL by default;
  `PRAMEN_LEDGER_PATH=postgres://…` for the shared backend) —
  content-addressed work keys, durable reuse, pre-dispatch budgets,
  strict typed validation with `fail`/`drop`/`review`. `ai.generate`
  requires UTF-8 `maxChars` and `maxOutputTokensPerRecord` (no silent
  truncation). Providers: `mock`, `openai-compat`, `bedrock`.
- **Provider-batch execution** (`execution: batch`): job ids durably
  recorded before await; crash-after-submit reconciles without re-billing.
  OpenAI Files + Batches and Bedrock model invocation jobs (S3 staging)
  are protocol-stub / L2 tested; live acceptance pending credentials.
- **WASM transforms**: `type: wasm` via Wasmtime (WIT + Arrow IPC),
  `pramen transform test`, Rust guest template, and OCI pull-by-digest
  with fail-closed allow-list + signature verification hook.
- **Review-queue routing** (`onInvalid: review` + `pramen ai review`).
- **Observability**: structured logs + optional OTLP metrics export.
- **Measured quickstart** and **benchmark suite** published in-repo.

## Spike-validated (design proven)

- Durable inference ledger, bounded-memory scanning, binary COPY
  throughput — see [measured results](/pramen/project/benchmarks/).
- WASM–Arrow ABI under limits (~43 ns/row on default batches).

## Still open

- **Cloud acceptance** (credentials): S1.1 live Bedrock online, S2.1
  batch crash numbers, S2.2 quality–cost frontier, P2.1 1M-record AWS run.
- **Phase 2 Group X2** (done offline): third-party WASM conformance
  (`examples/external-wasm-guest/`); deploy artifacts + runbook
  ([Deploying on AWS](/pramen/cookbook/aws-deploy/)). Live AWS apply still
  needs credentials.
- **Phase 3 product (E1)**: **E1.3 fan-out done** (ADR 0007: `from` edges +
  `sinks`, all-sinks-then-checkpoint); **E1.2 Flight SQL append done**
  (ADR 0008: `type: flightSql`, `CommandStatementIngest`); **E1.4 connector
  inspect + support matrix done** (`pramen inspect connector`,
  [support matrix](https://github.com/akovari/pramen/blob/main/docs/connectors/support-matrix.md)).
  Still open: ADBC (E1.1).
- **Phase 3 research**: E2.1 offline dispatch model + mock frontier and
  E2.2 memoization contract/measurements are published under
  [`docs/research/`](https://github.com/akovari/pramen/tree/main/docs/research).
  E2.3 scaffolding shipped: [Compared to alternatives](/pramen/project/comparison/)
  (orientation + generated scoreboard + `compare/` harnesses); measured
  competitor AI / warehouse legs still open. **E2.4 done:**
  `mise run reproduce` / `reproduce-check` +
  [artifact evaluation checklist](https://github.com/akovari/pramen/blob/main/docs/research/artifact-evaluation.md).
  Open: E2.5 paper (needs venue ADR). Live Bedrock frontier remains tied to
  S2.2.
  [Governed AI reuse contract](/pramen/concepts/governed-ai/#reuse-contract-rq2).

## Engineering standards

Every change passes the same gates on Linux (x86_64, aarch64), macOS
(aarch64), and Windows (x86_64): clippy with warnings denied, no
`unsafe`, no `unwrap` outside tests, documented public items,
`cargo-deny`, and offline-only PR tests (zero cloud credentials in CI).
Decisions are recorded as ADRs with goal metrics and reopen triggers.

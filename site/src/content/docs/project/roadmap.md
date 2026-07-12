---
title: Status and roadmap
description: What works today, what is being built, and in what order.
---

Pramen is developed in the open with a
[task-level plan](https://github.com/akovari/pramen/blob/main/docs/implementation-plan.md)
mirrored to [GitHub issues](https://github.com/akovari/pramen/issues).
This page is the honest summary.

## Works today

- **Pipeline document** (`pramen.dev/v1alpha1`): strict parsing, complete
  path-addressed validation, published JSON Schema.
- **CLI**: `validate`, `explain` (text + JSON), `run`, `ai status`,
  `ai evaluate`, `ai review`.
- **End-to-end runs**: Parquet or NDJSON — local or `s3://` (S3 and
  S3-compatible stores like MinIO) — → per-batch DataFusion SQL →
  transactional binary `COPY` into PostgreSQL, with bounded memory,
  backpressure, and Ctrl-C safety.
- **Checkpointed incremental runs**: file-granular work units on a
  crash-safe append-only store; replaying a finished run loads nothing,
  a grown directory loads only new files (ADR 0006). Works on local
  directories and `s3://` prefixes alike — S3 unit identity (key, size,
  last-modified) comes from a single `LIST` request, MinIO-verified end
  to end.
- **Upsert sink mode**: stage + `ON CONFLICT` merge on declared keys;
  replays are idempotent, within-run duplicates collapse
  deterministically (last write wins). The at-least-once contract is
  pinned by tests on both sides: append duplicates on replay, upsert
  does not.
- **Run-level cost governance**: `maxRunTokens` hard ceiling (checked
  before each dispatch, ledger reuse free) and an always-armed
  circuit breaker that aborts after N consecutive invalid outputs.
- **Governed semantic transforms**: `ai.extract` / `ai.classify` on the
  production SQLite (WAL) inference ledger — content-addressed work keys,
  durable result reuse on replay, pre-dispatch input token budgets,
  provider-side output caps, strict typed output validation with
  `fail`/`drop`/`review` policies. Providers: `mock` (deterministic,
  offline), `openai-compat` (vLLM, Ollama, llama.cpp, hosted), and
  `bedrock` (Converse API, default credential chain, region pinning,
  stub-tested offline per ADR 0005 — live acceptance pending credentials).
- **Provider-batch execution** (`execution: batch`): ledger misses are
  submitted as one asynchronous provider job whose id is durably
  recorded per item before results are awaited; a run that crashes after
  submission reconciles on restart by job and item id instead of
  resubmitting — pinned by tests asserting zero re-billing. Exercised
  end to end against the batch-capable `mock` provider. The
  `openai-compat` adapter implements the batch surface via the OpenAI
  Files + Batches APIs (protocol-stub-tested offline), and `bedrock`
  implements model invocation jobs with S3 staging plus a `keys.jsonl`
  join fallback (L2-tested against MinIO and a control-plane stub;
  live acceptance pending credentials).
- **Golden-corpus evaluation** (`pramen ai evaluate`): a versioned,
  520-item labelled support-ticket corpus with weighted rubrics, run
  through the same provider adapters as pipelines; reports schema-valid
  rate, per-field accuracy, macro-F1, a weighted score, tokens, cost, and
  latency percentiles into a timestamped, diffable results directory.
- **Smoke runs** (`run --smoke`): source row cap, clamped semantic token
  ceiling, checkpointing bypassed — a cheap rehearsal that still proves
  sink connectivity under the real transactional contract.
- **Measured quickstart**: the documented binary-to-enriched-rows path is
  executed by `scripts/quickstart.sh` in CI on every change and timed
  against the ten-minute bar, so the docs cannot drift from reality.
- **Runtime guarantees**: commit-safety on failure (no partial loads),
  first-failure error attribution, prompt cooperative shutdown — all
  covered by behavioral tests.
- **Observability**: structured logs (`--log-format pretty|json|silent`,
  the JSON envelope pinned by a snapshot test) and optional OTLP metrics
  export (`run --otlp-endpoint`): the final run counters and duration,
  pushed over HTTP/protobuf to any collector.
- **Typed faults with an injection suite**: provider timeouts (deadline
  enforced), throttles, transport failures, malformed responses, and
  server errors each map to a documented
  [fault class](/pramen/concepts/runtime/#typed-faults); a killed
  database backend mid-`COPY` fails typed with the target table
  untouched. All induced offline.
- **Review-queue routing** (`onInvalid: review` + `pramen ai review`):
  invalid records are durably queued with their inputs, reason, and raw
  model output — withheld from every run and never re-dispatched or
  re-billed while undecided. Accepted corrections are schema-validated
  and re-enter the ledger as zero-token `human-review` results;
  rejections are permanent, auditable drops.
- **Benchmark suite** (`scripts/bench.sh` + Criterion): deterministic
  generated inputs, end-to-end throughput / CPU / peak RSS against
  DataFusion-direct, DuckDB-native, and a like-for-like
  DuckDB→PostgreSQL leg (same query, same server, via its `postgres`
  extension), plus encoder and ledger micro-benches — results published
  with methodology in [measured results](/pramen/project/benchmarks/).

## Spike-validated (design proven)

- **Durable inference ledger** (SQLite WAL): 100% result reuse on replay,
  zero results lost across crashes, microsecond overhead per item — now
  productionized in `pramen-ai`.
- **Bounded-memory scanning** and **binary COPY throughput** — see
  [measured results](/pramen/project/benchmarks/).
- **The WASM transform ABI** (Phase 2's riskiest boundary): Arrow IPC
  through a WIT component at ~43 ns/row on default-size batches — ~2%
  of the measured load path — with memory, fuel, and deadline limits
  all trapping deterministically and exactly reproducible fuel
  accounting. All four Phase 0 risk spikes are now complete.

## In development (Phase 1)

- The model quality-cost frontier table (S2.2 remainder): the corpus and
  `ai evaluate` harness are live; the pinned model choice per tier needs
  runs against real Bedrock models and a local vLLM.
- Live cloud acceptance legs: Bedrock Converse online (S1.1), Bedrock
  batch crash/reconcile numbers (S2.1), and the 1M-record AWS run
  (P2.1) — all blocked on credentials, not code.

## After v1

- **Phase 2 — extensibility and cloud breadth**: sandboxed WebAssembly
  transform components with a conformance suite and OCI distribution;
  Azure Blob and GCS; a web UI over the review queue; Postgres-backed
  shared ledger/checkpoint backends for fleets.
- **Phase 3 — expansion and research**: ADBC warehouse sinks, Flight SQL,
  fan-out DAGs, a connector SDK — and the research program: a
  peer-reviewed systems paper on cost-optimal, restart-safe semantic
  enrichment, whose evaluation doubles as the public benchmark suite.

## Engineering standards

Every PR passes the same gates on Linux (x86_64, aarch64), macOS (aarch64),
and Windows (x86_64): clippy with warnings denied, no `unsafe`, no
`unwrap` outside tests, documented public items, `cargo-deny` supply-chain
checks, and offline-only tests (zero cloud credentials in CI). Decisions
are recorded as ADRs with goal metrics and reopen triggers.

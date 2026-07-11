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
- **CLI**: `validate`, `explain` (text + JSON), `run`, `ai status`.
- **End-to-end runs**: Parquet or NDJSON — local or `s3://` (S3 and
  S3-compatible stores like MinIO) — → per-batch DataFusion SQL →
  transactional binary `COPY` into PostgreSQL, with bounded memory,
  backpressure, and Ctrl-C safety.
- **Checkpointed incremental runs**: file-granular work units on a
  crash-safe append-only store; replaying a finished run loads nothing,
  a grown directory loads only new files (ADR 0006).
- **Governed semantic transforms**: `ai.extract` / `ai.classify` on the
  production SQLite (WAL) inference ledger — content-addressed work keys,
  durable result reuse on replay, pre-dispatch input token budgets,
  provider-side output caps, strict typed output validation with
  `fail`/`drop`/`review` policies. Providers: `mock` (deterministic,
  offline), `openai-compat` (vLLM, Ollama, llama.cpp, hosted), and
  `bedrock` (Converse API, default credential chain, region pinning,
  stub-tested offline per ADR 0005 — live acceptance pending credentials).
- **Runtime guarantees**: commit-safety on failure (no partial loads),
  first-failure error attribution, prompt cooperative shutdown — all
  covered by behavioral tests.

## Spike-validated (design proven)

- **Durable inference ledger** (SQLite WAL): 100% result reuse on replay,
  zero results lost across crashes, microsecond overhead per item — now
  productionized in `pramen-ai`.
- **Bounded-memory scanning** and **binary COPY throughput** — see
  [measured results](/pramen/project/benchmarks/).

## In development (Phase 1)

- Provider-batch execution with restart reconciliation (P1.8), developed
  against a local fake batch service before any cloud spend.
- Upsert sink mode (P1.4); remote work-unit enumeration for checkpointed
  `s3://` sources (P1.1 remainder).
- Per-run cost ceilings and error-spike circuit breakers (P1.11
  remainder); review-queue routing (X1.6).
- The golden evaluation corpus and quality-cost frontier (S2.2), runnable
  against local models first.
- `run --smoke`, `ai evaluate`, and the measured ten-minute quickstart
  (P1.16–P1.18).
- Fault-injection and benchmark suites (P1.19–P1.20).

## After v1

- **Phase 2 — extensibility and cloud breadth**: sandboxed WebAssembly
  transform components with a conformance suite and OCI distribution;
  Azure Blob and GCS; the human review queue; Postgres-backed shared
  ledger/checkpoint backends for fleets.
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

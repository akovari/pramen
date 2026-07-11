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
- **CLI**: `validate`, `explain` (text + JSON), `run` for deterministic
  pipelines.
- **End-to-end deterministic runs**: local Parquet → per-batch DataFusion
  SQL → transactional binary `COPY` into PostgreSQL, with bounded memory,
  backpressure, and Ctrl-C safety.
- **Runtime guarantees**: commit-safety on failure (no partial loads),
  first-failure error attribution, prompt cooperative shutdown — all
  covered by behavioral tests.

## Spike-validated (design proven, productionization underway)

- **Durable inference ledger** (SQLite WAL): 100% result reuse on replay,
  zero results lost across crashes, microsecond overhead per item.
- **Provider adapters**: Bedrock Converse and OpenAI-compatible request/
  response handling proven against local protocol stubs.
- **Bounded-memory scanning** and **binary COPY throughput** — see
  [measured results](/pramen/project/benchmarks/).

## In development (Phase 1)

- `ai.extract` / `ai.classify` operators on the production ledger, with
  budgets, circuit breakers, and schema validation (P1.5–P1.12).
- NDJSON source; remote object stores (S3 first) (P1.1–P1.2).
- Checkpointing and resumable runs (P1.3); upsert sink mode (P1.4).
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
